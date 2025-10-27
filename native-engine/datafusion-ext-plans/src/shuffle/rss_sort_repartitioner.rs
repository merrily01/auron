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

use std::sync::Weak;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use auron_memmgr::{MemConsumer, MemConsumerInfo, MemManager};
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::arrow::array_size::BatchSize;
use futures::lock::Mutex;
use jni::objects::GlobalRef;

use crate::shuffle::{Partitioning, ShuffleRepartitioner, buffered_data::BufferedData};

pub struct RssSortShuffleRepartitioner {
    mem_consumer_info: Option<Weak<MemConsumerInfo>>,
    data: Mutex<BufferedData>,
    rss: GlobalRef,
}

impl RssSortShuffleRepartitioner {
    pub fn new(
        partition_id: usize,
        rss_partition_writer: GlobalRef,
        partitioning: Partitioning,
        output_io_time: Time,
    ) -> Self {
        Self {
            mem_consumer_info: None,
            data: Mutex::new(BufferedData::new(
                partitioning,
                partition_id,
                output_io_time,
            )),
            rss: rss_partition_writer,
        }
    }
}

#[async_trait]
impl MemConsumer for RssSortShuffleRepartitioner {
    fn name(&self) -> &str {
        "RssSortShuffleRepartitioner"
    }

    fn set_consumer_info(&mut self, consumer_info: Weak<MemConsumerInfo>) {
        self.mem_consumer_info = Some(consumer_info);
    }

    fn get_consumer_info(&self) -> &Weak<MemConsumerInfo> {
        self.mem_consumer_info
            .as_ref()
            .expect("consumer info not set")
    }

    async fn spill(&self) -> Result<()> {
        let data = self.data.lock().await.drain();
        let rss = self.rss.clone();

        tokio::task::spawn_blocking(move || data.write_rss(rss))
            .await
            .expect("tokio error")?;
        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for RssSortShuffleRepartitioner {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssSortShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        // update memory usage before adding to buffered data
        let mem_used = self.data.lock().await.mem_used() + input.get_batch_mem_size() * 2;
        self.update_mem_used(mem_used).await?;

        // add batch to buffered data
        let mem_used = {
            let mut data = self.data.lock().await;
            data.add_batch(input)?;
            data.mem_used()
        };
        self.update_mem_used(mem_used).await?;

        // we are likely to spill more frequently because the cost of spilling a shuffle
        // repartition is lower than other consumers.
        // rss shuffle spill has even lower cost than normal shuffle
        if self.mem_used_percent() > 0.4 {
            self.force_spill().await?;
        }
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        self.force_spill().await?;
        Ok(())
    }
}
