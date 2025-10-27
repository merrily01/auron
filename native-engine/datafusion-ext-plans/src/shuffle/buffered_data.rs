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

use std::io::Write;

use arrow::record_batch::RecordBatch;
use auron_jni_bridge::{is_task_running, jni_call};
use bytesize::ByteSize;
use count_write::CountWrite;
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::{
    algorithm::rdx_sort::radix_sort_by_key,
    arrow::{
        array_size::BatchSize,
        selection::{BatchInterleaver, create_batch_interleaver},
    },
    compute_suggested_batch_size_for_output, df_execution_err,
    io::ipc_compression::IpcCompressionWriter,
};
use itertools::Itertools;
use jni::objects::GlobalRef;
#[cfg(test)]
use parking_lot::Mutex;

use crate::{
    common::{
        offsetted::{Offsetted, OffsettedMergeIterator},
        timer_helper::TimerHelper,
    },
    shuffle::{
        Partitioning, evaluate_hashes, evaluate_partition_ids, evaluate_range_partition_ids,
        evaluate_robin_partition_ids, rss::RssWriter,
    },
};

pub struct BufferedData {
    partition_id: usize,
    partitioning: Partitioning,
    staging_batches: Vec<RecordBatch>,
    staging_num_rows: usize,
    staging_mem_used: usize,
    sorted_batches: Vec<RecordBatch>,
    sorted_offsets: Vec<Vec<u32>>,
    num_rows: usize,
    sorted_mem_used: usize,
    output_io_time: Time,
}

impl BufferedData {
    pub fn new(partitioning: Partitioning, partition_id: usize, output_io_time: Time) -> Self {
        Self {
            partition_id,
            partitioning,
            staging_batches: vec![],
            staging_num_rows: 0,
            staging_mem_used: 0,
            sorted_batches: vec![],
            sorted_offsets: vec![],
            num_rows: 0,
            sorted_mem_used: 0,
            output_io_time,
        }
    }

    pub fn drain(&mut self) -> Self {
        std::mem::replace(
            self,
            Self::new(
                self.partitioning.clone(),
                self.partition_id,
                self.output_io_time.clone(),
            ),
        )
    }

    pub fn add_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // first add to staging, mem used is doubled for later sorting
        self.num_rows += batch.num_rows();
        self.staging_num_rows += batch.num_rows();
        self.staging_mem_used += batch.get_batch_mem_size() * 2;
        self.staging_batches.push(batch);

        let suggested_batch_size =
            compute_suggested_batch_size_for_output(self.staging_mem_used, self.staging_num_rows);
        if self.staging_mem_used > suggested_batch_size {
            self.flush_staging()?;
        }
        Ok(())
    }

    fn flush_staging(&mut self) -> Result<()> {
        let sorted_num_rows = self.num_rows - self.staging_num_rows;
        let staging_batches = std::mem::take(&mut self.staging_batches);
        let (offsets, sorted_batch) = sort_batches_by_partition_id(
            staging_batches,
            &self.partitioning,
            sorted_num_rows,
            self.partition_id,
        )?;
        self.staging_num_rows = 0;
        self.staging_mem_used = 0;

        self.sorted_mem_used += sorted_batch.get_batch_mem_size() + offsets.len() * 4;
        self.sorted_batches.push(sorted_batch);
        self.sorted_offsets.push(offsets);
        Ok(())
    }

    // write buffered data to spill/target file, returns uncompressed size and
    // offsets to each partition
    pub fn write<W: Write>(mut self, mut w: W) -> Result<Vec<u64>> {
        if self.num_rows == 0 {
            return Ok(vec![0; self.partitioning.partition_count() + 1]);
        }

        let mem_used = ByteSize(self.mem_used() as u64);
        log::info!("draining all buffered data, total_mem={mem_used}");

        if !self.staging_batches.is_empty() {
            self.flush_staging()?;
        }

        let output_io_time = self.output_io_time.clone();
        let num_partitions = self.partitioning.partition_count();
        let mut writer = IpcCompressionWriter::new(CountWrite::from(&mut w));
        let mut offsets = vec![];
        let mut iter = self.into_sorted_batches()?;

        while let Some((partition_id, batch_iter)) = iter.next_partition_chunk() {
            if !is_task_running() {
                df_execution_err!("task completed/killed")?;
            }

            offsets.resize(partition_id + 1, writer.inner().count());
            for batch in batch_iter {
                output_io_time
                    .with_timer(|| writer.write_batch(batch.num_rows(), batch.columns()))?;
            }
            output_io_time.with_timer(|| writer.finish_current_buf())?;
        }
        offsets.resize(num_partitions + 1, writer.inner().count());

        let compressed_size = ByteSize(offsets.last().cloned().unwrap_or_default());
        log::info!("all buffered data drained, compressed_size={compressed_size}");
        Ok(offsets)
    }

    // write buffered data to rss, returns uncompressed size
    pub fn write_rss(mut self, rss_partition_writer: GlobalRef) -> Result<()> {
        if self.num_rows == 0 {
            return Ok(());
        }

        let mem_used = ByteSize(self.mem_used() as u64);
        log::info!("draining all buffered data to rss, total_mem={mem_used}");

        if !self.staging_batches.is_empty() {
            self.flush_staging()?;
        }

        let output_io_time = self.output_io_time.clone();
        let mut iter = self.into_sorted_batches()?;
        let mut writer = IpcCompressionWriter::new(RssWriter::new(rss_partition_writer.clone(), 0));

        while let Some((partition_id, batch_iter)) = iter.next_partition_chunk() {
            if !is_task_running() {
                df_execution_err!("task completed/killed")?;
            }

            // write all batches with this part id
            writer.set_output(RssWriter::new(rss_partition_writer.clone(), partition_id));
            for batch in batch_iter {
                output_io_time
                    .with_timer(|| writer.write_batch(batch.num_rows(), batch.columns()))?;
            }
            output_io_time.with_timer(|| writer.finish_current_buf())?;
        }

        output_io_time.with_timer(
            || jni_call!(AuronRssPartitionWriterBase(rss_partition_writer.as_obj()).flush() -> ()),
        )?;
        log::info!("all buffered data drained to rss");
        Ok(())
    }

    fn into_sorted_batches(self) -> Result<PartitionedBatchesIterator<'static>> {
        let num_rows = self.num_rows;
        let sub_batch_size = compute_suggested_batch_size_for_output(self.mem_used(), num_rows);
        let num_partitions = self.partitioning.partition_count();
        PartitionedBatchesIterator::try_new(
            self.sorted_batches,
            self.sorted_offsets,
            sub_batch_size,
            num_partitions,
        )
    }

    pub fn mem_used(&self) -> usize {
        self.sorted_mem_used + self.staging_mem_used
    }

    pub fn is_empty(&self) -> bool {
        self.sorted_batches.is_empty() && self.staging_batches.is_empty()
    }
}

struct PartitionedBatchesIterator<'a> {
    batch_interleaver: BatchInterleaver,
    merge_iter: OffsettedMergeIterator<'a, u32, usize>,
    batch_size: usize,
    last_chunk_partition_id: Option<usize>,
}

impl<'a> PartitionedBatchesIterator<'a> {
    pub fn try_new(
        batches: Vec<RecordBatch>,
        batch_offsets: Vec<Vec<u32>>,
        sub_batch_size: usize,
        num_partitions: usize,
    ) -> Result<Self> {
        Ok(Self {
            batch_interleaver: create_batch_interleaver(&batches, true)?,
            merge_iter: OffsettedMergeIterator::new(
                num_partitions,
                batch_offsets
                    .into_iter()
                    .enumerate()
                    .map(|(idx, offsets)| Offsetted::new(offsets, idx))
                    .collect(),
            ),
            batch_size: sub_batch_size,
            last_chunk_partition_id: None,
        })
    }

    /// all iterators returned should have been fully consumed
    pub fn next_partition_chunk(
        &mut self,
    ) -> Option<(usize, impl Iterator<Item = RecordBatch> + 'a)> {
        // safety: bypass lifetime checker
        let batches_iter =
            unsafe { std::mem::transmute::<_, &mut PartitionedBatchesIterator<'a>>(self) };

        let (chunk_partition_id, chunk) = batches_iter.merge_iter.next_partition_chunk()?;

        // last chunk must be fully consumed
        if batches_iter.last_chunk_partition_id == Some(chunk_partition_id) {
            panic!("last chunk not fully consumed");
        }
        batches_iter.last_chunk_partition_id = Some(chunk_partition_id);

        let batch_iter = chunk.batching(|chunk| {
            let mut indices = vec![];
            for (batch_idx, range) in chunk {
                indices.extend(range.map(|offset| (*batch_idx, offset as usize)));
                if indices.len() >= batches_iter.batch_size {
                    break;
                }
            }

            if indices.is_empty() {
                return None;
            }
            let batch_interleaver = &mut batches_iter.batch_interleaver;
            let output_batch = batch_interleaver(&indices).expect("error interleaving batches");
            return Some(output_batch);
        });
        Some((chunk_partition_id, batch_iter))
    }
}

fn sort_batches_by_partition_id(
    batches: Vec<RecordBatch>,
    partitioning: &Partitioning,
    current_num_rows: usize,
    partition_id: usize,
) -> Result<(Vec<u32>, RecordBatch)> {
    let num_partitions = partitioning.partition_count();
    let mut round_robin_start_rows =
        (partition_id * 1000193 + current_num_rows) % partitioning.partition_count();

    // compute partition indices
    let mut partition_indices = batches
        .iter()
        .enumerate()
        .flat_map(|(batch_idx, batch)| {
            let part_ids = match partitioning {
                Partitioning::HashPartitioning(..) => {
                    // compute partition indices
                    let hashes = evaluate_hashes(partitioning, &batch)
                        .expect(&format!("error evaluating hashes with {partitioning}"));
                    evaluate_partition_ids(hashes, partitioning.partition_count())
                }
                Partitioning::RoundRobinPartitioning(..) => {
                    let part_ids =
                        evaluate_robin_partition_ids(partitioning, &batch, round_robin_start_rows);
                    round_robin_start_rows += batch.num_rows();
                    round_robin_start_rows %= partitioning.partition_count();
                    part_ids
                }
                Partitioning::RangePartitioning(sort_expr, _, bounds) => {
                    evaluate_range_partition_ids(&batch, sort_expr, bounds).unwrap()
                }
                _ => unreachable!("unsupported partitioning: {:?}", partitioning),
            };
            part_ids
                .into_iter()
                .enumerate()
                .map(move |(row_idx, part_id)| (part_id, batch_idx as u32, row_idx as u32))
        })
        .collect::<Vec<_>>();

    // sort
    let mut part_counts = vec![0; num_partitions];
    radix_sort_by_key(
        &mut partition_indices,
        &mut part_counts,
        |&(part_id, ..)| part_id as usize,
    );

    // compute partitions
    let mut partition_offsets = Vec::with_capacity(num_partitions + 1);
    let mut offset = 0;
    for part_count in part_counts {
        partition_offsets.push(offset);
        offset += part_count as u32;
    }
    partition_offsets.push(offset);

    // get sorted batch
    let batches_interleaver = create_batch_interleaver(&batches, true)?;
    let sorted_batch = batches_interleaver(
        &partition_indices
            .into_iter()
            .map(|(_, batch_idx, row_idx)| (batch_idx as usize, row_idx as usize))
            .collect::<Vec<_>>(),
    )?;
    return Ok((partition_offsets, sorted_batch));
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
        row::{RowConverter, Rows, SortField},
    };
    use arrow_schema::SortOptions;
    use datafusion::{
        assert_batches_eq,
        common::Result,
        physical_expr::{PhysicalSortExpr, expressions::Column},
    };

    use super::*;

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_round_robin() -> Result<()> {
        let record_batch = build_table_i32(
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        );

        let round_robin_partitioning = Partitioning::RoundRobinPartitioning(4);
        let (_parts, sorted_batch) =
            sort_batches_by_partition_id(vec![record_batch], &round_robin_partitioning, 3, 0)?;

        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 18 | 1 | 6 |",
            "| 14 | 5 | 0 |",
            "| 10 | 9 | 4 |",
            "| 17 | 2 | 7 |",
            "| 13 | 6 | 1 |",
            "| 12 | 7 | 2 |",
            "| 16 | 3 | 8 |",
            "| 19 | 0 | 5 |",
            "| 15 | 4 | 9 |",
            "| 11 | 8 | 3 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }

    #[tokio::test]
    async fn test_range_partition() -> Result<()> {
        let record_batch = build_table_i32(
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        );
        let sort_exprs = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        }];
        let bound1 = Arc::new(Int32Array::from_iter_values([11, 14, 17])) as ArrayRef;
        let bounds = vec![bound1];

        let sort_row_converter = Arc::new(Mutex::new(RowConverter::new(
            sort_exprs
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    Ok(SortField::new_with_options(
                        expr.expr.data_type(&record_batch.schema())?,
                        expr.options,
                    ))
                })
                .collect::<Result<Vec<SortField>>>()?,
        )?));

        let rows: Rows = sort_row_converter.lock().convert_columns(&bounds).unwrap();
        let partition_num = rows.num_rows() + 1;

        let range_repartitioning =
            Partitioning::RangePartitioning(sort_exprs, partition_num, Arc::from(rows));
        let (_parts, sorted_batch) =
            sort_batches_by_partition_id(vec![record_batch], &range_repartitioning, 0, 0)?;

        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 11 | 8 | 3 |",
            "| 10 | 9 | 4 |",
            "| 14 | 5 | 0 |",
            "| 13 | 6 | 1 |",
            "| 12 | 7 | 2 |",
            "| 17 | 2 | 7 |",
            "| 16 | 3 | 8 |",
            "| 15 | 4 | 9 |",
            "| 19 | 0 | 5 |",
            "| 18 | 1 | 6 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }

    #[tokio::test]
    async fn test_range_partition_2() -> Result<()> {
        let record_batch = build_table_i32(
            ("a", &vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]),
            ("b", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ("c", &vec![5, 6, 7, 8, 9, 0, 1, 2, 3, 4]),
        );
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];
        let bound1 = Arc::new(Int32Array::from_iter_values([11, 14, 17])) as ArrayRef;
        let bound2 = Arc::new(Int32Array::from_iter_values([1, 3, 5])) as ArrayRef;

        let bounds = vec![bound1, bound2];

        let sort_row_converter = Arc::new(Mutex::new(RowConverter::new(
            sort_exprs
                .iter()
                .map(|expr: &PhysicalSortExpr| {
                    Ok(SortField::new_with_options(
                        expr.expr.data_type(&record_batch.schema())?,
                        expr.options,
                    ))
                })
                .collect::<Result<Vec<SortField>>>()?,
        )?));

        let rows: Rows = sort_row_converter.lock().convert_columns(&bounds).unwrap();
        let partition_num = rows.num_rows() + 1;

        let range_repartitioning =
            Partitioning::RangePartitioning(sort_exprs, partition_num, Arc::from(rows));
        let (_parts, sorted_batch) =
            sort_batches_by_partition_id(vec![record_batch], &range_repartitioning, 0, 0)?;

        let expected = vec![
            "+----+---+---+",
            "| a  | b | c |",
            "+----+---+---+",
            "| 10 | 9 | 4 |",
            "| 13 | 6 | 1 |",
            "| 12 | 7 | 2 |",
            "| 11 | 8 | 3 |",
            "| 17 | 2 | 7 |",
            "| 16 | 3 | 8 |",
            "| 15 | 4 | 9 |",
            "| 14 | 5 | 0 |",
            "| 19 | 0 | 5 |",
            "| 18 | 1 | 6 |",
            "+----+---+---+",
        ];
        assert_batches_eq!(expected, &vec![sorted_batch]);
        Ok(())
    }
}
