// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`ScalarUDFImpl`] definitions for array_any_value function.

use crate::utils::make_scalar_function;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, GenericListArray, ListArray, OffsetSizeTrait};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, LargeList, List};
use datafusion_common::{exec_err, plan_err, Result};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;
use datafusion_common::cast::as_list_array;

make_udf_expr_and_func!(
    ArrayAnyValue,
    array_any_value,
    array,
    "returns the first non-null value in the list.",
    array_any_value_udf
);

#[derive(Debug)]
pub(super) struct ArrayAnyValue {
    signature: Signature,
    aliases: Vec<String>,
}
impl ArrayAnyValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_any_value".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayAnyValue {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_any_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(field)
            | LargeList(field)
            | FixedSizeList(field, _) => Ok(field.data_type().clone()),
            _ => plan_err!(
                "ArrayElement can only accept List, LargeList or FixedSizeList as the first argument"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_any_value_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_any_value SQL function
pub fn array_any_value_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_any_value expects one argument");
    }

    let array_type = args[0].data_type();
    match array_type {
        List(_) => general_array_any_value::<i32>(&args[0].as_list::<i32>()),
        LargeList(_) => general_array_any_value::<i64>(&args[0].as_list::<i64>()),
        _ => exec_err!("array_any_value does not support type '{array_type:?}'."),
    }
}

fn general_array_any_value<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let mut data: Vec<Option<Vec<Option<i64>>>> = Vec::with_capacity(list_array.len());
    for (row_index, list_array_row) in list_array.iter().enumerate() {
        if let Some(list_array_row) = list_array_row {
            let list_array_row_inner = as_list_array(list_array_row.as_list::<O>())?;
            // if let Some(non_empty) = list_array_row_inner.iter().find(|&s| !s.is_empty()) {
            //     // println!("First non-empty string: {}", non_empty);
            //     data.push(Some(vec![Some(1)]));
            // } else {
            //     println!("All strings are empty.");
            // }
        } else {
            data.push(None);
        }
    }
    Ok(Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
        data,
    )))
}
