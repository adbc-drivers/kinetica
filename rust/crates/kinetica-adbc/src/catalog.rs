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

//! Builds the nested catalog structure ADBC prescribes for `get_objects`.
//!
//! The rows come from `information_schema`, one query for the whole depth
//! rather than a call per table: tools ask for this on connect, and a catalog
//! with a few thousand tables would otherwise take minutes.
//!
//! Kinetica has schemas but no catalogs, so a single unnamed catalog holds
//! them all.

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::error::Result as AdbcResult;
use adbc_core::options::{InfoCode, ObjectDepth};
use adbc_core::schemas;
use arrow_array::builder::{Int32Builder, StringBuilder};
use arrow_array::{
    new_null_array, Array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};

use crate::error::from_arrow;

/// One column of one table, as far as ADBC cares.
struct Column {
    name: String,
    position: i32,
    type_name: String,
    nullable: bool,
}

/// Where a foreign key points: one column of the table it references.
struct Usage {
    schema: String,
    table: String,
    column: String,
}

/// One constraint of one table. A primary key names the columns it is made
/// of; a foreign key names those and where each of them points.
struct Constraint {
    name: String,
    kind: String,
    columns: Vec<String>,
    references: Vec<Usage>,
}

/// One table, what it holds and what is promised about it; both stay empty at
/// shallower depths.
struct Table {
    kind: String,
    columns: Vec<Column>,
    constraints: Vec<Constraint>,
}

type Schemas = BTreeMap<String, BTreeMap<String, Table>>;

/// @return The struct fields of one of the prescribed nested types
fn fields_of(data_type: &DataType) -> Fields {
    match data_type {
        DataType::Struct(fields) => fields.clone(),
        other => unreachable!("expected a struct, got {other}"),
    }
}

/// Fills the named arrays and leaves the rest of the prescribed struct null,
/// which is what ADBC expects for the xdbc columns a driver cannot answer.
/// @return The struct array for the given rows
fn struct_of(
    data_type: &DataType,
    rows: usize,
    filled: &[(&str, ArrayRef)],
) -> AdbcResult<StructArray> {
    let fields = fields_of(data_type);

    let arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|field| {
            filled
                .iter()
                .find(|(name, _)| name == field.name())
                .map(|(_, array)| Arc::clone(array))
                .unwrap_or_else(|| new_null_array(field.data_type(), rows))
        })
        .collect();

    StructArray::try_new(fields, arrays, None).map_err(from_arrow)
}

/// @return A list array grouping the values by the given lengths
fn list_of(
    field_name: &str,
    data_type: &DataType,
    values: ArrayRef,
    lengths: &[usize],
) -> AdbcResult<ListArray> {
    list_where(field_name, data_type, values, lengths, None)
}

/// A list column, as `list_of`, null in the rows `present` marks false.
/// @return The list column
fn list_where(
    field_name: &str,
    data_type: &DataType,
    values: ArrayRef,
    lengths: &[usize],
    present: Option<Vec<bool>>,
) -> AdbcResult<ListArray> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    let mut running = 0i32;
    offsets.push(running);

    for length in lengths {
        running += *length as i32;
        offsets.push(running);
    }

    let field = Arc::new(Field::new(field_name, data_type.clone(), true));

    ListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        present.map(NullBuffer::from),
    )
    .map_err(from_arrow)
}

/// Assembles the rows into the shape `get_objects` returns.
/// @return One record batch holding the whole catalog
pub fn build(depth: ObjectDepth, rows: Rows) -> AdbcResult<RecordBatch> {
    let schemas_by_name = rows.schemas;

    let wants_tables = !matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas);
    let wants_columns = matches!(depth, ObjectDepth::All | ObjectDepth::Columns);

    // Columns, flattened, with the length of each table's run.
    let mut column_names = StringBuilder::new();
    let mut positions = Int32Builder::new();
    let mut type_names = StringBuilder::new();
    let mut nullables = StringBuilder::new();
    let mut columns_per_table = Vec::new();

    // Constraints, flattened the same way, with their own two levels of run
    // lengths: the columns each constraint names, and where each of those
    // points for a foreign key.
    let mut constraint_names = StringBuilder::new();
    let mut constraint_kinds = StringBuilder::new();
    let mut constrained_columns = StringBuilder::new();
    let mut columns_per_constraint = Vec::new();
    let mut usage_schemas = StringBuilder::new();
    let mut usage_tables = StringBuilder::new();
    let mut usage_columns = StringBuilder::new();
    let mut usages_per_constraint = Vec::new();
    // Only a foreign key points anywhere; for any other, ADBC wants no list
    let mut usage_present = Vec::new();
    let mut constraints_per_table = Vec::new();

    let mut table_names = StringBuilder::new();
    let mut table_kinds = StringBuilder::new();
    let mut tables_per_schema = Vec::new();

    let mut schema_names = StringBuilder::new();

    for (schema_name, tables) in &schemas_by_name {
        schema_names.append_value(schema_name);
        tables_per_schema.push(if wants_tables { tables.len() } else { 0 });

        if !wants_tables {
            continue;
        }

        for (table_name, table) in tables {
            table_names.append_value(table_name);
            table_kinds.append_value(&table.kind);
            columns_per_table.push(if wants_columns {
                table.columns.len()
            } else {
                0
            });
            constraints_per_table.push(if wants_columns {
                table.constraints.len()
            } else {
                0
            });

            if !wants_columns {
                continue;
            }

            for column in &table.columns {
                column_names.append_value(&column.name);
                positions.append_value(column.position);
                type_names.append_value(&column.type_name);
                nullables.append_value(if column.nullable { "YES" } else { "NO" });
            }

            for constraint in &table.constraints {
                constraint_names.append_value(&constraint.name);
                constraint_kinds.append_value(&constraint.kind);
                columns_per_constraint.push(constraint.columns.len());
                usages_per_constraint.push(constraint.references.len());
                usage_present.push(constraint.kind == "FOREIGN KEY");

                for column in &constraint.columns {
                    constrained_columns.append_value(column);
                }

                for usage in &constraint.references {
                    usage_schemas.append_value(&usage.schema);
                    usage_tables.append_value(&usage.table);
                    usage_columns.append_value(&usage.column);
                }
            }
        }
    }

    let column_names = column_names.finish();
    let column_count = column_names.len();

    let columns = struct_of(
        &schemas::COLUMN_SCHEMA,
        column_count,
        &[
            ("column_name", Arc::new(column_names)),
            ("ordinal_position", Arc::new(positions.finish())),
            ("xdbc_type_name", Arc::new(type_names.finish())),
            ("xdbc_is_nullable", Arc::new(nullables.finish())),
        ],
    )?;

    // Where the foreign keys point. The catalog is left null, as it is
    // everywhere else here: Kinetica has schemas and nothing above them.
    let usage_columns = usage_columns.finish();
    let usage_count = usage_columns.len();

    let usage = struct_of(
        &schemas::USAGE_SCHEMA,
        usage_count,
        &[
            ("fk_db_schema", Arc::new(usage_schemas.finish())),
            ("fk_table", Arc::new(usage_tables.finish())),
            ("fk_column_name", Arc::new(usage_columns)),
        ],
    )?;

    let constraint_names = constraint_names.finish();
    let constraint_count = constraint_names.len();

    let constraints = struct_of(
        &schemas::CONSTRAINT_SCHEMA,
        constraint_count,
        &[
            ("constraint_name", Arc::new(constraint_names)),
            ("constraint_type", Arc::new(constraint_kinds.finish())),
            (
                "constraint_column_names",
                Arc::new(list_of(
                    "item",
                    &DataType::Utf8,
                    Arc::new(constrained_columns.finish()),
                    &columns_per_constraint,
                )?),
            ),
            (
                "constraint_column_usage",
                Arc::new(list_where(
                    "item",
                    &schemas::USAGE_SCHEMA,
                    Arc::new(usage),
                    &usages_per_constraint,
                    Some(usage_present),
                )?),
            ),
        ],
    )?;

    let table_names = table_names.finish();
    let table_count = table_names.len();

    let tables = struct_of(
        &schemas::TABLE_SCHEMA,
        table_count,
        &[
            ("table_name", Arc::new(table_names)),
            ("table_type", Arc::new(table_kinds.finish())),
            (
                "table_columns",
                Arc::new(list_of(
                    "item",
                    &schemas::COLUMN_SCHEMA,
                    Arc::new(columns),
                    &columns_per_table,
                )?),
            ),
            (
                "table_constraints",
                Arc::new(list_of(
                    "item",
                    &schemas::CONSTRAINT_SCHEMA,
                    Arc::new(constraints),
                    &constraints_per_table,
                )?),
            ),
        ],
    )?;

    let schema_names = schema_names.finish();
    let schema_count = schema_names.len();

    let db_schemas = struct_of(
        &schemas::OBJECTS_DB_SCHEMA_SCHEMA,
        schema_count,
        &[
            ("db_schema_name", Arc::new(schema_names)),
            (
                "db_schema_tables",
                Arc::new(list_of(
                    "item",
                    &schemas::TABLE_SCHEMA,
                    Arc::new(tables),
                    &tables_per_schema,
                )?),
            ),
        ],
    )?;

    // One unnamed catalog, since Kinetica has schemas but nothing above them.
    let catalogs = list_of(
        "item",
        &schemas::OBJECTS_DB_SCHEMA_SCHEMA,
        Arc::new(db_schemas),
        &[schema_count],
    )?;

    RecordBatch::try_new(
        Arc::clone(&schemas::GET_OBJECTS_SCHEMA),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(catalogs),
        ],
    )
    .map_err(from_arrow)
}

/// Collects the rows a `get_objects` call needs.
pub struct Rows {
    schemas: Schemas,
}

impl Default for Rows {
    fn default() -> Self {
        Self::new()
    }
}

impl Rows {
    pub fn new() -> Self {
        Self {
            schemas: BTreeMap::new(),
        }
    }

    pub fn add_schema(&mut self, name: &str) {
        self.schemas.entry(name.to_string()).or_default();
    }

    pub fn add_table(&mut self, schema: &str, name: &str, kind: &str) {
        self.schemas.entry(schema.to_string()).or_default().insert(
            name.to_string(),
            Table {
                kind: kind.to_string(),
                columns: Vec::new(),
                constraints: Vec::new(),
            },
        );
    }

    pub fn add_column(
        &mut self,
        schema: &str,
        table: &str,
        name: &str,
        position: i32,
        type_name: &str,
        nullable: bool,
    ) {
        if let Some(entry) = self.schemas.get_mut(schema).and_then(|t| t.get_mut(table)) {
            entry.columns.push(Column {
                name: name.to_string(),
                position,
                type_name: type_name.to_string(),
                nullable,
            });
        }
    }

    /// Records one column of one constraint, which the catalog reads a row at
    /// a time: the constraint is made on the first row that names it and
    /// added to on the ones after.
    pub fn add_constraint_column(
        &mut self,
        schema: &str,
        table: &str,
        name: &str,
        kind: &str,
        column: &str,
    ) {
        let Some(entry) = self.schemas.get_mut(schema).and_then(|t| t.get_mut(table)) else {
            return;
        };

        let constraint = match entry.constraints.iter_mut().find(|held| held.name == name) {
            Some(held) => held,
            None => {
                entry.constraints.push(Constraint {
                    name: name.to_string(),
                    kind: kind.to_string(),
                    columns: Vec::new(),
                    references: Vec::new(),
                });

                entry.constraints.last_mut().expect("just pushed")
            }
        };

        if !constraint.columns.iter().any(|held| held == column) {
            constraint.columns.push(column.to_string());
        }
    }

    /// Records where one column of a foreign key points. A constraint that is
    /// not a foreign key references itself, which ADBC does not ask for, so
    /// the caller passes only the foreign ones.
    pub fn add_constraint_usage(
        &mut self,
        schema: &str,
        table: &str,
        name: &str,
        references_schema: &str,
        references_table: &str,
        references_column: &str,
    ) {
        let Some(entry) = self.schemas.get_mut(schema).and_then(|t| t.get_mut(table)) else {
            return;
        };

        let Some(constraint) = entry.constraints.iter_mut().find(|held| held.name == name) else {
            return;
        };

        constraint.references.push(Usage {
            schema: references_schema.to_string(),
            table: references_table.to_string(),
            column: references_column.to_string(),
        });
    }
}

/// @return The column of a batch as strings, for the catalog queries
pub fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<StringArray>()
}

/// @return The column of a batch as ints, for the catalog queries
pub fn ints<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int32Array> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<Int32Array>()
}

// --------------------------------------------------------------------------
// Driver and server information

/// The union child a value goes into, which is the order `GET_INFO_SCHEMA`
/// declares its fields in.
const STRING_VALUE: i8 = 0;
const BOOL_VALUE: i8 = 1;
const INT64_VALUE: i8 = 2;

/// One answer to `get_info`, which the union carries in one of its children.
enum Info {
    Text(String),
    Flag(bool),
    Number(i64),
}

/// Builds the union ADBC returns from `get_info`, keeping only the codes the
/// caller asked for.
/// @return The single batch of answers
pub fn info_batch(
    codes: Option<&std::collections::HashSet<InfoCode>>,
    vendor_version: Option<String>,
) -> AdbcResult<RecordBatch> {
    let answers: Vec<(InfoCode, Info)> = [
        (InfoCode::VendorName, Some(Info::Text("Kinetica".into()))),
        (InfoCode::VendorVersion, vendor_version.map(Info::Text)),
        (InfoCode::VendorSql, Some(Info::Flag(true))),
        (InfoCode::VendorSubstrait, Some(Info::Flag(false))),
        // The name the driver goes by wherever it is listed
        (
            InfoCode::DriverName,
            Some(Info::Text("Kinetica ADBC Driver".into())),
        ),
        (
            InfoCode::DriverVersion,
            Some(Info::Text(crate::VERSION.into())),
        ),
        (
            InfoCode::DriverArrowVersion,
            Some(Info::Text(crate::ARROW_VERSION.into())),
        ),
        (InfoCode::DriverAdbcVersion, Some(Info::Number(1_001_000))),
    ]
    .into_iter()
    .filter(|(code, _)| codes.is_none_or(|wanted| wanted.contains(code)))
    .filter_map(|(code, value)| value.map(|v| (code, v)))
    .collect();

    let mut names = Vec::with_capacity(answers.len());
    let mut type_ids = Vec::with_capacity(answers.len());
    let mut offsets = Vec::with_capacity(answers.len());

    let mut texts: Vec<String> = Vec::new();
    let mut flags: Vec<bool> = Vec::new();
    let mut numbers: Vec<i64> = Vec::new();

    for (code, value) in answers {
        names.push(u32::from(&code));

        match value {
            Info::Text(v) => {
                type_ids.push(STRING_VALUE);
                offsets.push(texts.len() as i32);
                texts.push(v);
            }
            Info::Flag(v) => {
                type_ids.push(BOOL_VALUE);
                offsets.push(flags.len() as i32);
                flags.push(v);
            }
            Info::Number(v) => {
                type_ids.push(INT64_VALUE);
                offsets.push(numbers.len() as i32);
                numbers.push(v);
            }
        }
    }

    let DataType::Union(fields, _) = schemas::GET_INFO_SCHEMA.field(1).data_type().clone() else {
        unreachable!("the info schema's value column is a union");
    };

    // Every child has to be present even when nothing landed in it, and the
    // ones no answer used stay empty rather than null.
    let children: Vec<ArrayRef> = fields
        .iter()
        .map(|(id, field)| match id {
            STRING_VALUE => Arc::new(StringArray::from(texts.clone())) as ArrayRef,
            BOOL_VALUE => Arc::new(arrow_array::BooleanArray::from(flags.clone())) as ArrayRef,
            INT64_VALUE => Arc::new(arrow_array::Int64Array::from(numbers.clone())) as ArrayRef,
            _ => new_null_array(field.data_type(), 0),
        })
        .collect();

    let values = arrow_array::UnionArray::try_new(
        fields,
        arrow_buffer::ScalarBuffer::from(type_ids),
        Some(arrow_buffer::ScalarBuffer::from(offsets)),
        children,
    )
    .map_err(from_arrow)?;

    RecordBatch::try_new(
        Arc::clone(&schemas::GET_INFO_SCHEMA),
        vec![
            Arc::new(arrow_array::UInt32Array::from(names)),
            Arc::new(values),
        ],
    )
    .map_err(from_arrow)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// @return Each text answer in an info batch, by its code
    fn texts(batch: &RecordBatch) -> std::collections::HashMap<u32, String> {
        let codes = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .expect("codes");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UnionArray>()
            .expect("values");
        let strings = values
            .child(STRING_VALUE)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("strings");

        (0..batch.num_rows())
            .filter(|&row| values.type_id(row) == STRING_VALUE)
            .map(|row| {
                (
                    codes.value(row),
                    strings.value(values.value_offset(row)).to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn the_driver_reports_its_name_and_version() {
        let batch = info_batch(None, Some("7.2.3.23.20261014183705".into())).expect("batch");
        let texts = texts(&batch);

        let name = &texts[&u32::from(&InfoCode::DriverName)];
        let version = &texts[&u32::from(&InfoCode::DriverVersion)];

        assert_eq!(name, "Kinetica ADBC Driver");

        // MAJOR.MINOR.PATCH and nothing more: the build number is in the log
        assert_eq!(version, crate::VERSION);
        let parts: Vec<&str> = version.split('.').collect();
        assert_eq!(parts.len(), 3, "{version}");
        assert!(
            parts.iter().all(|part| part.parse::<u64>().is_ok()),
            "{version}"
        );

        // The Arrow the driver was built with, as ADBC writes it
        let arrow = &texts[&u32::from(&InfoCode::DriverArrowVersion)];
        assert!(arrow.starts_with('v'), "{arrow}");

        // The server's version is passed on whole, build number and all.
        assert_eq!(
            texts[&u32::from(&InfoCode::VendorVersion)],
            "7.2.3.23.20261014183705"
        );
    }
}
