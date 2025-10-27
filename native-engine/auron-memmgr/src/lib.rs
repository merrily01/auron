// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(get_mut_unchecked)]

pub mod metrics;
pub mod spill;

use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use auron_jni_bridge::{conf, conf::DoubleConf, is_jni_bridge_inited, jni_call_static};
use bytesize::ByteSize;
use datafusion::common::Result;
use once_cell::sync::OnceCell;
use parking_lot::{Condvar, Mutex};

static MEM_MANAGER: OnceCell<Arc<MemManager>> = OnceCell::new();

// never triggers waiting/spilling for consumers which use very little memory
const MIN_TRIGGER_SIZE: usize = 1 << 24; // 16MB

pub struct MemManager {
    total: usize,
    consumers: Mutex<Vec<Arc<MemConsumerInfo>>>,
    status: Mutex<MemManagerStatus>,
    cv: Condvar,
}

impl MemManager {
    pub fn init(total: usize) {
        MEM_MANAGER.get_or_init(|| {
            log::info!(
                "mem manager initialized with total memory: {}",
                ByteSize(total as u64),
            );

            Arc::new(MemManager {
                total,
                consumers: Mutex::default(),
                status: Mutex::default(),
                cv: Condvar::default(),
            })
        });
    }

    pub fn initialized() -> bool {
        MEM_MANAGER.get().is_some()
    }

    pub fn get() -> &'static MemManager {
        MEM_MANAGER.get().expect("mem manager not initialized")
    }

    pub fn num_consumers(&self) -> usize {
        self.consumers.lock().len()
    }

    pub fn total_used(&self) -> usize {
        self.status.lock().total_used
    }

    pub fn mem_used_percent(&self) -> f64 {
        self.total_used() as f64 / self.total as f64
    }

    pub fn register_consumer(mut consumer: Arc<dyn MemConsumer>, spillable: bool) {
        let consumer_info = Arc::new(MemConsumerInfo {
            name: consumer.name().to_owned(),
            status: Mutex::new(MemConsumerStatus {
                mem_used: 0,
                spillable,
            }),
        });
        log::info!("mem manager registering consumer: {}", consumer.name());

        // safety:
        // get_consumer_info() is guaranteed not to be called before this operation
        unsafe {
            let consumer_mut = Arc::get_mut_unchecked(&mut consumer);
            consumer_mut.set_consumer_info(Arc::downgrade(&consumer_info));
        }

        let mm = Self::get();
        let mut mm_consumers = mm.consumers.lock();
        let mut mm_status = mm.status.lock();
        mm_consumers.push(consumer_info);
        mm_status.num_consumers += 1;
        if spillable {
            mm_status.num_spillables += 1;
        }
    }

    pub fn deregister_consumer(consumer: &dyn MemConsumer) {
        let mm = Self::get();
        let mut mm_consumers = mm.consumers.lock();
        let mut mm_status = mm.status.lock();

        let consumer_info = consumer.consumer_info();
        let consumer_status = consumer_info.status.lock();

        // update mm status
        assert!(mm_status.total_used >= consumer_status.mem_used);
        mm_status.num_consumers -= 1;
        mm_status.update_total_used_with_diff(-(consumer_status.mem_used as isize));

        // update mm spillable status
        if consumer_status.spillable {
            assert!(mm_status.mem_spillables >= consumer_status.mem_used);
            mm_status.num_spillables -= 1;
            mm_status.mem_spillables -= consumer_status.mem_used;
        }

        // remove consumer info
        for i in 0..mm_consumers.len() {
            if Arc::ptr_eq(&mm_consumers[i], &consumer_info) {
                log::info!("mem manager deregistered consumer: {}", consumer.name());
                mm_consumers.swap_remove(i);

                drop(mm_status);
                drop(mm_consumers);
                return;
            }
        }
        unreachable!("deregistering non-registered memory consumer")
    }

    pub fn dump_status(&self) {
        let mm_status = self.status.lock();
        log::info!(
            "mem manager status: total: {}, mem_used: {}, jvm_direct: {}, proc resident: {}",
            ByteSize(self.total as u64),
            ByteSize(mm_status.total_used as u64),
            ByteSize(get_mem_jvm_direct_used() as u64),
            ByteSize(get_proc_memory_used() as u64),
        );
        drop(mm_status);

        for consumer in &*self.consumers.lock() {
            let consumer_status = consumer.status.lock();
            log::info!(
                "* consumer: {}, spillable: {}, mem_used: {}",
                consumer.name,
                consumer_status.spillable,
                ByteSize(consumer_status.mem_used as u64),
            );
        }
    }
}

#[derive(Default, Clone, Copy)]
struct MemManagerStatus {
    num_consumers: usize,
    total_used: usize,
    num_spillables: usize,
    mem_spillables: usize,
}

impl MemManagerStatus {
    fn update_total_used_with_diff(&mut self, diff_used: isize) -> usize {
        assert!(self.total_used as isize + diff_used >= 0);

        let new_used = (self.total_used as isize + diff_used) as usize;
        let old_used = std::mem::replace(&mut self.total_used, new_used);

        // freeing some memory, notifies all waiting growers
        if new_used < old_used {
            MemManager::get().cv.notify_all();
        }
        new_used
    }
}

#[derive(Debug)]
pub struct MemConsumerInfo {
    name: String,
    status: Mutex<MemConsumerStatus>,
}

#[derive(Clone, Copy, Debug)]
struct MemConsumerStatus {
    mem_used: usize,
    spillable: bool,
}

#[async_trait]
pub trait MemConsumer: Send + Sync {
    fn name(&self) -> &str;
    fn set_consumer_info(&mut self, consumer_info: Weak<MemConsumerInfo>);
    fn get_consumer_info(&self) -> &Weak<MemConsumerInfo>;

    fn consumer_info(&self) -> Arc<MemConsumerInfo> {
        self.get_consumer_info()
            .upgrade()
            .expect("consumer deregistered")
    }

    fn mem_used_percent(&self) -> f64 {
        let mm = MemManager::get();
        let total = mm.total;
        let mm_status = *mm.status.lock();

        let mem_unspillable = mm_status.total_used - mm_status.mem_spillables;
        let total_managed = total
            .saturating_sub(get_mem_jvm_direct_used())
            .saturating_sub(mem_unspillable);
        let mem_used = self.consumer_info().status.lock().mem_used;
        let consumer_mem_max = total_managed / mm_status.num_spillables.max(1);
        mem_used as f64 / consumer_mem_max as f64
    }

    fn set_spillable(&self, spillable: bool) {
        let consumer_info = self.consumer_info();
        let mut consumer_status = consumer_info.status.lock();

        if consumer_status.spillable != spillable {
            let mut mm_status = MemManager::get().status.lock();
            if spillable {
                mm_status.num_spillables += 1;
                mm_status.mem_spillables += consumer_status.mem_used;
            } else {
                assert!(mm_status.mem_spillables >= consumer_status.mem_used);
                mm_status.num_spillables -= 1;
                mm_status.mem_spillables -= consumer_status.mem_used;
            }
        }
        consumer_status.spillable = spillable;
    }

    async fn update_mem_used(&self, new_used: usize) -> Result<()>
    where
        Self: Sized,
    {
        update_consumer_mem_used_with_custom_updater(
            self,
            |consumer_status| {
                let old_used = std::mem::replace(&mut consumer_status.mem_used, new_used);
                (old_used, new_used)
            },
            false,
        )
        .await
    }

    async fn update_mem_used_with_diff(&self, diff_used: isize) -> Result<()>
    where
        Self: Sized,
    {
        update_consumer_mem_used_with_custom_updater(
            self,
            |consumer_status| {
                let old_used = consumer_status.mem_used;
                let new_used = if diff_used > 0 {
                    old_used.saturating_add(diff_used as usize)
                } else {
                    old_used.saturating_sub(-diff_used as usize)
                };
                consumer_status.mem_used = new_used;
                (old_used, new_used)
            },
            false,
        )
        .await
    }

    async fn force_spill(&self) -> Result<()>
    where
        Self: Sized,
    {
        update_consumer_mem_used_with_custom_updater(
            self,
            |consumer_status| {
                let old_used = consumer_status.mem_used;
                let new_used = old_used + 1;
                (old_used, new_used)
            },
            true,
        )
        .await
    }

    /// spills this consumer and returns used memory after spilling
    async fn spill(&self) -> Result<()> {
        unimplemented!()
    }
}

async fn update_consumer_mem_used_with_custom_updater(
    consumer: &dyn MemConsumer,
    updater: impl Fn(&mut MemConsumerStatus) -> (usize, usize),
    forced: bool,
) -> Result<()> {
    let consumer_name = consumer.name();
    let mm = MemManager::get();
    let consumer_info = consumer.consumer_info();
    let total = mm.total;

    #[derive(Clone, Copy, PartialEq)]
    enum Operation {
        Spill,   // spill this consumer
        Wait,    // wait other consumers to spill
        Nothing, // do nothing
    }

    let (mem_unspillable, mem_jvm_direct_used, mem_proc_total_used);
    let (mem_used, total_used, operation) = {
        let mut mm_status = mm.status.lock();
        let mut consumer_status = consumer_info.status.lock();

        // update consumer info
        let (old_used, new_used) = updater(&mut consumer_status);
        let spillable = consumer_status.spillable;
        let diff_used = new_used as isize - old_used as isize;
        assert!(
            !forced || spillable,
            "forced spilling an unspillable memory consumer"
        );

        // update mm status
        let total_used = mm_status.update_total_used_with_diff(diff_used);

        // update mm spillable status
        if consumer_status.spillable {
            assert!(mm_status.mem_spillables as isize + diff_used >= 0);
            mm_status.mem_spillables = (mm_status.mem_spillables as isize + diff_used) as usize;
        }

        // consumer is empty or unspillable, no need to wait or spill
        if !forced && (old_used == 0 || new_used == 0 || !spillable) {
            return Ok(());
        }

        // unlock
        let num_spillables = mm_status.num_spillables;
        let mem_spillables = mm_status.mem_spillables;
        drop(consumer_status);
        drop(mm_status);

        // get unspillable memory
        mem_unspillable = total_used - mem_spillables;

        // get jvm direct memory used
        mem_jvm_direct_used = get_mem_jvm_direct_used();

        let total_managed = total
            .saturating_sub(mem_jvm_direct_used) // jvm direct memory
            .saturating_sub(mem_unspillable); // unspillable memory
        let consumer_mem_max = total_managed / num_spillables;
        let consumer_mem_min = consumer_mem_max / 8;

        static PROCESS_MEMORY_FRACTION: OnceCell<f64> = OnceCell::new();
        let proc_mem_fraction = PROCESS_MEMORY_FRACTION
            .get_or_init(|| conf::PROCESS_MEMORY_FRACTION.value().unwrap_or(1.0));
        let mem_proc_max = (get_proc_memory_limited() as f64 * proc_mem_fraction) as usize;
        mem_proc_total_used = get_proc_memory_used();

        let total_overflowed = total_used > total_managed;
        let consumer_overflowed = new_used > consumer_mem_max;
        let proc_overflowed = mem_proc_total_used > mem_proc_max;

        let operation = if forced
            || ((total_overflowed || consumer_overflowed || proc_overflowed)
                && new_used > MIN_TRIGGER_SIZE
                && new_used > old_used)
        {
            if forced || (spillable && new_used > consumer_mem_min) {
                Operation::Spill
            } else {
                Operation::Wait
            }
        } else {
            Operation::Nothing
        };
        (new_used, total_used, operation)
    };
    let mut operation = operation;

    // trigger waiting for resources
    if operation == Operation::Wait {
        const WAIT_TIME: Duration = Duration::from_millis(10000);

        let mut mm_status = mm.status.lock();
        let wait = mm
            .cv
            .wait_while_for(&mut mm_status, |s| total < s.total_used, WAIT_TIME);

        if wait.timed_out() {
            log::warn!("mem manager: consumer {consumer_name} timeout waiting for resources");
            operation = Operation::Spill;
        }
    }

    // trigger spilling
    if operation == Operation::Spill {
        log::info!(
            "mem manager spilling {consumer_name} (consumer: {}), total_consumer: {}/{}, unspillable: {}, jvm_direct: {}, proc resident: {}",
            ByteSize(mem_used as u64),
            ByteSize(total_used as u64),
            ByteSize(mm.total as u64),
            ByteSize(mem_unspillable as u64),
            ByteSize(mem_jvm_direct_used as u64),
            ByteSize(mem_proc_total_used as u64),
        );
        consumer.spill().await?;
        return Ok(());
    }
    Ok(())
}

fn get_mem_jvm_direct_used() -> usize {
    if is_jni_bridge_inited() {
        jni_call_static!(JniBridge.getDirectMemoryUsed() -> i64).unwrap_or_default() as usize
    } else {
        0
    }
}

fn get_proc_memory_limited() -> usize {
    if is_jni_bridge_inited() {
        jni_call_static!(JniBridge.getTotalMemoryLimited() -> i64).unwrap_or_default() as usize
    } else {
        0
    }
}

fn get_proc_memory_used() -> usize {
    #[cfg(target_os = "linux")]
    fn get_vmrss_used() -> usize {
        use procfs::{ProcResult, process::Process};

        fn get_vmrss_used_impl() -> ProcResult<usize> {
            let self_proc = Process::myself()?;
            let statm = self_proc.statm()?;
            Ok(statm.resident as usize * procfs::page_size() as usize)
        }
        get_vmrss_used_impl().unwrap_or(0)
    }

    #[cfg(not(target_os = "linux"))]
    fn get_vmrss_used() -> usize {
        0
    }
    get_vmrss_used()
}
