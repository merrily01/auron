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

use auron_jni_bridge::{
    conf::{DoubleConf, IntConf},
    jni_bridge::JavaClasses,
    *,
};
use auron_memmgr::MemManager;
use datafusion::{
    common::Result,
    error::DataFusionError,
    execution::{
        SessionStateBuilder,
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        runtime_env::RuntimeEnvBuilder,
    },
    prelude::{SessionConfig, SessionContext},
};
use jni::{
    JNIEnv,
    objects::{JClass, JObject, JString},
};
use once_cell::sync::OnceCell;

use crate::{handle_unwinded_scope, logging::init_logging, rt::NativeExecutionRuntime};

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_auron_jni_JniBridge_callNative(
    env: JNIEnv,
    _: JClass,
    executor_memory_overhead: i64,
    log_level: JString,
    native_wrapper: JObject,
) -> i64 {
    handle_unwinded_scope(|| -> Result<i64> {
        static SESSION: OnceCell<SessionContext> = OnceCell::new();
        static INIT: OnceCell<()> = OnceCell::new();

        #[cfg(feature = "http-service")]
        {
            use crate::http::{HTTP_SERVICE, HttpService};
            let _ = HTTP_SERVICE.get_or_try_init(|| {
                eprintln!("initializing http service...");
                Ok::<HttpService, DataFusionError>(HttpService::init())
            });
        }

        INIT.get_or_try_init(|| {
            // logging is not initialized at this moment
            eprintln!("------ initializing auron native environment ------");
            let log_level = env.get_string(log_level).map(|s| String::from(s)).unwrap();
            eprintln!("initializing logging with level: {}", log_level);
            init_logging(log_level.as_str());

            // init jni java classes
            log::info!("initializing JNI bridge");
            JavaClasses::init(&env);

            // init datafusion session context
            log::info!("initializing datafusion session");
            SESSION.get_or_try_init(|| {
                let max_memory = executor_memory_overhead as usize;
                let memory_fraction = conf::MEMORY_FRACTION.value()?;
                let batch_size = conf::BATCH_SIZE.value()? as usize;
                MemManager::init((max_memory as f64 * memory_fraction) as usize);

                let mut session_config = SessionConfig::new().with_batch_size(batch_size);
                session_config
                    .options_mut()
                    .execution
                    .parquet
                    .schema_force_view_types = false;
                let diskManagerBuilder =
                    DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
                let runtime = RuntimeEnvBuilder::new()
                    .with_disk_manager_builder(diskManagerBuilder)
                    .build_arc()?;
                let state = SessionStateBuilder::new()
                    .with_config(session_config)
                    .with_runtime_env(runtime)
                    .build();
                let session = SessionContext::from(state);
                Ok::<_, DataFusionError>(session)
            })?;
            Ok::<_, DataFusionError>(())
        })?;
        let native_wrapper = jni_new_global_ref!(native_wrapper)?;

        // create execution runtime
        let runtime = Box::new(NativeExecutionRuntime::start(
            native_wrapper,
            SESSION.get().unwrap().task_ctx(),
        )?);

        // returns runtime raw pointer
        Ok::<_, DataFusionError>(Box::into_raw(runtime) as usize as i64)
    })
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_auron_jni_JniBridge_nextBatch(
    _: JNIEnv,
    _: JClass,
    raw_ptr: i64,
) -> bool {
    let runtime = unsafe { &*(raw_ptr as usize as *const NativeExecutionRuntime) };
    runtime.next_batch()
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_auron_jni_JniBridge_finalizeNative(
    _: JNIEnv,
    _: JClass,
    raw_ptr: i64,
) {
    let runtime = unsafe { Box::from_raw(raw_ptr as usize as *mut NativeExecutionRuntime) };
    runtime.finalize();
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_apache_auron_jni_JniBridge_onExit(_: JNIEnv, _: JClass) {
    log::info!("exiting native environment");
    if MemManager::initialized() {
        MemManager::get().dump_status();
    }
}
