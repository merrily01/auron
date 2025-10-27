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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{arrow::record_batch::RecordBatch, common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::{df_execution_err, io::ipc_compression::IpcCompressionWriter};
use jni::objects::GlobalRef;
use parking_lot::Mutex;

use crate::shuffle::{ShuffleRepartitioner, rss::RssWriter};

pub struct RssSingleShuffleRepartitioner {
    rss_partition_writer: Arc<Mutex<IpcCompressionWriter<RssWriter>>>,
    output_io_time: Time,
}

impl RssSingleShuffleRepartitioner {
    pub fn new(rss_partition_writer: GlobalRef, output_io_time: Time) -> Self {
        Self {
            rss_partition_writer: Arc::new(Mutex::new(IpcCompressionWriter::new(RssWriter::new(
                rss_partition_writer,
                0,
            )))),
            output_io_time,
        }
    }
}

#[async_trait]
impl ShuffleRepartitioner for RssSingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let rss_partition_writer = self.rss_partition_writer.clone();
        let output_io_time = self.output_io_time.clone();
        tokio::task::spawn_blocking(move || {
            let _output_io_timer = output_io_time.timer();
            rss_partition_writer
                .lock()
                .write_batch(input.num_rows(), input.columns())
        })
        .await
        .or_else(|err| df_execution_err!("{err}"))??;
        Ok(())
    }

    async fn shuffle_write(&self) -> Result<()> {
        let _output_io_timer = self.output_io_time.timer();
        self.rss_partition_writer.lock().finish_current_buf()?;
        Ok(())
    }
}
