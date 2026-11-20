// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Assembling what the server knows about a table's data into the nested shape
//! `get_statistics` returns.
//!
//! Kinetica keeps an exact row count per table and nothing else standing, so
//! that is what an approximate request gets. Asking for exact values is asking
//! for a pass over the data, and only then are the per-column counts computed.

use std::sync::Arc;

use adbc_core::error::Result as AdbcResult;
use adbc_core::options::Statistics;
use adbc_core::schemas;
use arrow_array::{
    ArrayRef, BooleanArray, Int16Array, Int64Array, ListArray, RecordBatch, StringArray,
    StructArray, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, UnionFields};

use crate::error::from_arrow;

/// The union child an integer statistic goes into, which is the order
/// `STATISTIC_VALUE_SCHEMA` declares its fields in.
const INT64_VALUE: i8 = 0;

/// One thing known about a table, or about one of its columns.
pub struct Fact {
    pub schema: String,
    pub table: String,
    /// None when the fact is about the table rather than a column.
    pub column: Option<String>,
    pub key: Statistics,
    pub value: i64,
}

/// Every statistic this driver reports is one ADBC already names, so there are
/// no driver-specific names to map.
/// @return The empty mapping
pub fn names_batch() -> AdbcResult<RecordBatch> {
    RecordBatch::try_new(
        Arc::clone(&schemas::GET_STATISTIC_NAMES_SCHEMA),
        vec![
            Arc::new(StringArray::new_null(0)) as ArrayRef,
            Arc::new(Int16Array::new_null(0)),
        ],
    )
    .map_err(from_arrow)
}

/// @return The struct fields of one of the prescribed nested types
fn fields_of(data_type: &DataType) -> Fields {
    match data_type {
        DataType::Struct(fields) => fields.clone(),
        other => unreachable!("expected a struct, got {other}"),
    }
}

/// @return A list array grouping the values by the given lengths
fn list_of(data_type: &DataType, values: ArrayRef, lengths: &[usize]) -> AdbcResult<ListArray> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    let mut running = 0i32;
    offsets.push(running);

    for length in lengths {
        running += *length as i32;
        offsets.push(running);
    }

    let field = Arc::new(Field::new("item", data_type.clone(), true));

    ListArray::try_new(field, OffsetBuffer::new(offsets.into()), values, None).map_err(from_arrow)
}

/// Assembles the facts into the shape `get_statistics` returns. They are
/// grouped by schema, so they arrive in that order.
/// @return The single batch of statistics
pub fn batch(facts: &[Fact]) -> AdbcResult<RecordBatch> {
    let values = Arc::new(Int64Array::from_iter_values(
        facts.iter().map(|fact| fact.value),
    ));

    let DataType::Union(union_fields, _) = schemas::STATISTIC_VALUE_SCHEMA.clone() else {
        unreachable!("the statistic value is a union");
    };

    let statistic_values = union_value(&union_fields, values, facts.len())?;

    let statistics = StructArray::try_new(
        fields_of(&schemas::STATISTICS_SCHEMA),
        vec![
            Arc::new(StringArray::from_iter_values(
                facts.iter().map(|f| f.table.as_str()),
            )),
            Arc::new(StringArray::from_iter(
                facts.iter().map(|f| f.column.as_deref()),
            )),
            Arc::new(Int16Array::from_iter_values(
                facts.iter().map(|f| i16::from(f.key.clone())),
            )),
            Arc::new(statistic_values),
            // Every value here is counted rather than estimated.
            Arc::new(BooleanArray::from(vec![false; facts.len()])),
        ],
        None,
    )
    .map_err(from_arrow)?;

    // The facts arrive grouped, so a run of them is one schema's worth.
    let mut names: Vec<&str> = Vec::new();
    let mut lengths: Vec<usize> = Vec::new();

    for fact in facts {
        match names.last() {
            Some(last) if *last == fact.schema => *lengths.last_mut().expect("a run") += 1,
            _ => {
                names.push(&fact.schema);
                lengths.push(1);
            }
        }
    }

    let db_schemas = StructArray::try_new(
        fields_of(&schemas::STATISTICS_DB_SCHEMA_SCHEMA),
        vec![
            Arc::new(StringArray::from(names.clone())),
            Arc::new(list_of(
                &schemas::STATISTICS_SCHEMA,
                Arc::new(statistics),
                &lengths,
            )?),
        ],
        None,
    )
    .map_err(from_arrow)?;

    // One unnamed catalog, since Kinetica has schemas but nothing above them.
    let catalogs = list_of(
        &schemas::STATISTICS_DB_SCHEMA_SCHEMA,
        Arc::new(db_schemas),
        &[names.len()],
    )?;

    RecordBatch::try_new(
        Arc::clone(&schemas::GET_STATISTICS_SCHEMA),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(catalogs),
        ],
    )
    .map_err(from_arrow)
}

/// Every value this driver reports is a count, so only one child of the union
/// is ever filled; the rest have to be there all the same.
/// @return The union holding the counts
fn union_value(
    fields: &UnionFields,
    counts: Arc<Int64Array>,
    rows: usize,
) -> AdbcResult<UnionArray> {
    let children: Vec<ArrayRef> = fields
        .iter()
        .map(|(id, field)| match id {
            INT64_VALUE => Arc::clone(&counts) as ArrayRef,
            _ => arrow_array::new_empty_array(field.data_type()),
        })
        .collect();

    UnionArray::try_new(
        fields.clone(),
        ScalarBuffer::from(vec![INT64_VALUE; rows]),
        Some(ScalarBuffer::from((0..rows as i32).collect::<Vec<_>>())),
        children,
    )
    .map_err(from_arrow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    /// @return A row count fact
    fn rows(schema: &str, table: &str, value: i64) -> Fact {
        Fact {
            schema: schema.into(),
            table: table.into(),
            column: None,
            key: Statistics::RowCount,
            value,
        }
    }

    #[test]
    fn nothing_known_is_still_the_right_shape() {
        let batch = batch(&[]).expect("batch");

        assert_eq!(batch.num_rows(), 1, "one catalog, holding no schemas");
        assert_eq!(batch.schema(), Arc::clone(&schemas::GET_STATISTICS_SCHEMA));
    }

    #[test]
    fn facts_are_grouped_by_the_schema_they_belong_to() {
        let facts = [
            rows("a", "one", 1),
            rows("a", "two", 2),
            rows("b", "three", 3),
        ];

        let batch = batch(&facts).expect("batch");

        let catalogs = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("catalogs");
        let db_schemas = catalogs
            .value(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("schemas")
            .clone();

        assert_eq!(db_schemas.len(), 2, "two schemas");

        let names = db_schemas
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("names")
            .clone();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "b");

        let lists = db_schemas
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("lists")
            .clone();
        assert_eq!(lists.value(0).len(), 2, "two tables in the first schema");
        assert_eq!(lists.value(1).len(), 1);
    }

    #[test]
    fn the_names_mapping_is_empty_because_none_are_ours() {
        assert_eq!(names_batch().expect("names").num_rows(), 0);
    }
}
