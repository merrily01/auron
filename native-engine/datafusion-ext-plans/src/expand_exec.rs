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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::{arrow::cast::cast, df_execution_err};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::common::execution_context::ExecutionContext;

#[derive(Debug, Clone)]
pub struct ExpandExec {
    schema: SchemaRef,
    projections: Vec<Vec<PhysicalExprRef>>,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl ExpandExec {
    pub fn try_new(
        schema: SchemaRef,
        projections: Vec<Vec<PhysicalExprRef>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        for projections in &projections {
            for i in 0..schema.fields.len() {
                let schema_data_type = schema.fields[i].data_type();
                let projection_data_type = projections
                    .get(i)
                    .map(|expr| expr.data_type(&input_schema))
                    .transpose()?;

                if projection_data_type.as_ref() != Some(schema_data_type) {
                    df_execution_err!(
                        "ExpandExec data type not matches: {projection_data_type:?} vs {schema_data_type:?}"
                    )?;
                }
            }
        }
        Ok(Self {
            schema,
            projections,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ExpandExec")
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            schema: self.schema(),
            projections: self.projections.clone(),
            input: children[0].clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let output = execute_expand(input, self.projections.clone(), exec_ctx)?;
        Ok(output)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

fn execute_expand(
    mut input: SendableRecordBatchStream,
    projections: Vec<Vec<PhysicalExprRef>>,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    Ok(exec_ctx
        .clone()
        .output_with_sender("Expand", move |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());

            while let Some(batch) = input.next().await.transpose()? {
                let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                let num_rows = batch.num_rows();

                for projection in &projections {
                    let arrays = projection
                        .iter()
                        .zip(exec_ctx.output_schema().fields())
                        .map(|(expr, field)| {
                            let array =
                                expr.evaluate(&batch).and_then(|c| c.into_array(num_rows))?;
                            if array.data_type() != field.data_type() {
                                return cast(&array, field.data_type());
                            }
                            Ok(array)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let output_batch = RecordBatch::try_new_with_options(
                        exec_ctx.output_schema(),
                        arrays,
                        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                    )?;
                    exec_ctx
                        .baseline_metrics()
                        .record_output(output_batch.num_rows());
                    sender.send(output_batch).await;
                }
            }
            Ok(())
        }))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{BooleanArray, Float32Array, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use auron_memmgr::MemManager;
    use datafusion::{
        assert_batches_eq,
        common::{Result, ScalarValue},
        logical_expr::Operator,
        physical_expr::expressions::{binary, col, lit},
        physical_plan::{ExecutionPlan, common, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use crate::expand_exec::ExpandExec;

    // build i32 table
    fn build_table_i32(a: (&str, &Vec<i32>)) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(a.0, DataType::Int32, false)]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(a.1.clone()))],
        )
        .unwrap()
    }

    fn build_table_int(a: (&str, &Vec<i32>)) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a);
        let schema = batch.schema();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    // build f32 table
    fn build_table_f32(a: (&str, &Vec<f32>)) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(a.0, DataType::Float32, false)]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Float32Array::from(a.1.clone()))],
        )
        .unwrap()
    }

    fn build_table_float(a: (&str, &Vec<f32>)) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_f32(a);
        let schema = batch.schema();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    // build str table
    fn build_table_str(a: (&str, &Vec<String>)) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(a.0, DataType::Utf8, false)]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(a.1.clone()))],
        )
        .unwrap()
    }

    fn build_table_string(a: (&str, &Vec<String>)) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_str(a);
        let schema = batch.schema();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    // build boolean table
    fn build_table_bool(a: (&str, &Vec<bool>)) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(a.0, DataType::Boolean, false)]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(a.1.clone()))],
        )
        .unwrap()
    }

    fn build_table_boolean(a: (&str, &Vec<bool>)) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_bool(a);
        let schema = batch.schema();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_expand_exec_i32() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_int(("a", &vec![-1, -2, 0, 3]));
        let schema = Schema::new(vec![Field::new("test_i32", DataType::Int32, false)]);

        let projections = vec![
            vec![
                binary(
                    col("test_i32", &schema).unwrap(),
                    Operator::Multiply,
                    lit(ScalarValue::from(2)),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_i32", &schema).unwrap(),
                    Operator::Plus,
                    lit(ScalarValue::from(100)),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_i32", &schema).unwrap(),
                    Operator::Divide,
                    lit(ScalarValue::from(-2)),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_i32", &schema).unwrap(),
                    Operator::Modulo,
                    lit(ScalarValue::from(2)),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_i32", &schema).unwrap(),
                    Operator::BitwiseShiftLeft,
                    lit(ScalarValue::from(1)),
                    &schema,
                )
                .unwrap(),
            ],
        ];

        let expand_exec = ExpandExec::try_new(input.schema(), projections, input)?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+-----+", "| a   |", "+-----+", "| -2  |", "| -4  |", "| 0   |", "| 6   |", "| 99  |",
            "| 98  |", "| 100 |", "| 103 |", "| 0   |", "| 1   |", "| 0   |", "| -1  |", "| -1  |",
            "| 0   |", "| 0   |", "| 1   |", "| -2  |", "| -4  |", "| 0   |", "| 6   |", "+-----+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_expand_exec_f32() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_float(("a", &vec![-1.2, -2.3, 0.0, 3.4]));
        let schema = Schema::new(vec![Field::new("test_f32", DataType::Float32, false)]);

        let projections = vec![
            vec![
                binary(
                    col("test_f32", &schema).unwrap(),
                    Operator::Multiply,
                    lit(ScalarValue::from(2.1_f32)),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_f32", &schema).unwrap(),
                    Operator::Plus,
                    lit(ScalarValue::from(100_f32)),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_f32", &schema).unwrap(),
                    Operator::Divide,
                    lit(ScalarValue::from(-2_f32)),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_f32", &schema).unwrap(),
                    Operator::Modulo,
                    lit(ScalarValue::from(-2_f32)),
                    &schema,
                )
                .unwrap(),
            ],
        ];

        let expand_exec = ExpandExec::try_new(input.schema(), projections, input)?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+-------------+",
            "| a           |",
            "+-------------+",
            "| -2.52       |",
            "| -4.8299994  |",
            "| 0.0         |",
            "| 7.14        |",
            "| 98.8        |",
            "| 97.7        |",
            "| 100.0       |",
            "| 103.4       |",
            "| 0.6         |",
            "| 1.15        |",
            "| -0.0        |",
            "| -1.7        |",
            "| -1.2        |",
            "| -0.29999995 |",
            "| 0.0         |",
            "| 1.4000001   |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_expand_exec_str() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_string((
            "a",
            &vec![
                "hello".to_string(),
                ",".to_string(),
                "rust".to_string(),
                "!".to_string(),
            ],
        ));
        let schema = Schema::new(vec![Field::new("test_str", DataType::Utf8, false)]);

        let projections = vec![vec![
            binary(
                col("test_str", &schema).unwrap(),
                Operator::StringConcat,
                lit(Some("app").unwrap()),
                &schema,
            )
            .unwrap(),
        ]];

        let expand_exec = ExpandExec::try_new(input.schema(), projections, input)?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+----------+",
            "| a        |",
            "+----------+",
            "| helloapp |",
            "| ,app     |",
            "| rustapp  |",
            "| !app     |",
            "+----------+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_expand_exec_bool() -> Result<()> {
        MemManager::init(10000);

        let input = build_table_boolean(("a", &vec![true, false, true, false]));
        let schema = Schema::new(vec![Field::new("test_bool", DataType::Boolean, false)]);

        let projections = vec![
            vec![
                binary(
                    col("test_bool", &schema).unwrap(),
                    Operator::And,
                    lit(ScalarValue::Boolean(Some(true))),
                    &schema,
                )
                .unwrap(),
            ],
            vec![
                binary(
                    col("test_bool", &schema).unwrap(),
                    Operator::Or,
                    lit(ScalarValue::Boolean(Some(true))),
                    &schema,
                )
                .unwrap(),
            ],
        ];

        let expand_exec = ExpandExec::try_new(input.schema(), projections, input)?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let output = expand_exec.execute(0, task_ctx).unwrap();
        let batches = common::collect(output).await?;
        let expected = vec![
            "+-------+",
            "| a     |",
            "+-------+",
            "| true  |",
            "| false |",
            "| true  |",
            "| false |",
            "| true  |",
            "| true  |",
            "| true  |",
            "| true  |",
            "+-------+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }
}
