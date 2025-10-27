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

use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, StructArray, as_struct_array, make_array, new_empty_array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use auron_jni_bridge::{
    is_task_running, jni_call, jni_call_static, jni_new_direct_byte_buffer, jni_new_global_ref,
};
use datafusion::{
    error::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExprRef, physical_exprs_bag_equal},
    physical_expr_common::physical_expr::DynEq,
    physical_plan::PhysicalExpr,
};
use datafusion_ext_commons::{arrow::cast::cast, df_execution_err};
use jni::objects::GlobalRef;
use once_cell::sync::OnceCell;

pub struct SparkUDFWrapperExpr {
    pub serialized: Vec<u8>,
    pub return_type: DataType,
    pub return_nullable: bool,
    pub params: Vec<PhysicalExprRef>,
    pub import_schema: SchemaRef,
    pub params_schema: OnceCell<SchemaRef>,
    jcontext: OnceCell<GlobalRef>,
    expr_string: String,
}

impl PartialEq for SparkUDFWrapperExpr {
    fn eq(&self, other: &Self) -> bool {
        physical_exprs_bag_equal(&self.params, &other.params)
            && self.serialized == other.serialized
            && self.return_type == other.return_type
            && self.return_nullable == other.return_nullable
    }
}

impl DynEq for SparkUDFWrapperExpr {
    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|other| other.eq(self))
            .unwrap_or(false)
    }
}

impl SparkUDFWrapperExpr {
    pub fn try_new(
        serialized: Vec<u8>,
        return_type: DataType,
        return_nullable: bool,
        params: Vec<PhysicalExprRef>,
        expr_string: String,
    ) -> Result<Self> {
        Ok(Self {
            serialized,
            return_type: return_type.clone(),
            return_nullable,
            params,
            import_schema: Arc::new(Schema::new(vec![Field::new("", return_type, true)])),
            params_schema: OnceCell::new(),
            jcontext: OnceCell::new(),
            expr_string,
        })
    }

    fn jcontext(&self) -> Result<GlobalRef> {
        self.jcontext
            .get_or_try_init(|| {
                let serialized_buf = jni_new_direct_byte_buffer!(&self.serialized)?;
                let jcontext_local =
                    jni_call_static!(JniBridge.getAuronUDFWrapperContext(serialized_buf.as_obj()) -> JObject)?;
                jni_new_global_ref!(jcontext_local.as_obj())
            })
            .cloned()
    }
}

impl Display for SparkUDFWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Debug for SparkUDFWrapperExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UDFWrapper({})", self.expr_string)
    }
}

impl Hash for SparkUDFWrapperExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.serialized.hash(state);
    }
}

impl PhysicalExpr for SparkUDFWrapperExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if !is_task_running() {
            df_execution_err!("SparkUDFWrapper: is_task_running=false")?;
        }

        let batch_schema = batch.schema();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(&self.return_type)));
        }

        // init params schema
        let params_schema = self
            .params_schema
            .get_or_try_init(|| -> Result<SchemaRef> {
                let mut param_fields = Vec::with_capacity(self.params.len());
                for param in &self.params {
                    param_fields.push(Field::new(
                        "",
                        param.data_type(batch_schema.as_ref())?,
                        param.nullable(batch_schema.as_ref())?,
                    ));
                }
                Ok(Arc::new(Schema::new(param_fields)))
            })?;

        // evaluate params
        let params: Vec<ArrayRef> = self
            .params
            .iter()
            .zip(params_schema.fields())
            .map(|(param, field)| {
                let param_array = param.evaluate(batch).and_then(|r| r.into_array(num_rows))?;
                cast(&param_array, field.data_type())
            })
            .collect::<Result<_>>()?;
        let params_batch = RecordBatch::try_new_with_options(
            params_schema.clone(),
            params,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;

        // invoke UDF through JNI
        Ok(ColumnarValue::Array(invoke_udf(
            self.jcontext()?,
            params_batch,
            self.import_schema.clone(),
        )?))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        self.params.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::try_new(
            self.serialized.clone(),
            self.return_type.clone(),
            self.return_nullable.clone(),
            children,
            self.expr_string.clone(),
        )?))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

fn invoke_udf(
    jcontext: GlobalRef,
    params_batch: RecordBatch,
    result_schema: SchemaRef,
) -> Result<ArrayRef> {
    // evalute via context
    let struct_array = StructArray::from(params_batch);
    let mut export_ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
    let mut import_ffi_array = FFI_ArrowArray::empty();
    jni_call!(SparkAuronUDFWrapperContext(jcontext.as_obj()).eval(
        &mut export_ffi_array as *mut FFI_ArrowArray as i64,
        &mut import_ffi_array as *mut FFI_ArrowArray as i64,
    ) -> ())?;

    // import output from context
    let import_ffi_schema = FFI_ArrowSchema::try_from(result_schema.as_ref())?;
    let import_struct_array =
        make_array(unsafe { from_ffi(import_ffi_array, &import_ffi_schema)? });
    let import_array = as_struct_array(&import_struct_array).column(0).clone();
    Ok(import_array)
}
