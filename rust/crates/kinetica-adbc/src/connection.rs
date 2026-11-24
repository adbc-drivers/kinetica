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

//! The connection: catalog metadata, statistics, and the statements it opens.
//!
//! Every trait method here is synchronous, so each one that talks to the
//! server crosses the boundary in the `runtime` module.

use std::collections::HashSet;
use std::sync::Arc;

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue, Statistics};
use adbc_core::schemas;
use adbc_core::sync::Connection;
use adbc_core::Optionable;
use arrow_array::{Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::Schema;

use gpudb_api::{Kinetica, ShowTableRequestOptions};

use crate::catalog;
use crate::context;
use crate::error::{could_not, error, from_arrow, missing, not_found, unsupported};
use crate::partition;
use crate::reader::{ArrowStreamReader, SingleBatchReader};
use crate::runtime;
use crate::session::Session;
use crate::statement::{self, KineticaStatement};
use crate::statistics;

/// Wraps an identifier so a name needing it survives being pasted into SQL.
/// @return The quoted identifier
pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// @return Whether a name matches a SQL pattern, or any name if there is none
pub(crate) fn matches(name: &str, pattern: Option<&str>) -> bool {
    let Some(pattern) = pattern else {
        return true;
    };

    // Only the two wildcards SQL gives a LIKE pattern are honored, which is
    // all the catalog queries alongside this one accept either.
    let escaped: String = pattern
        .chars()
        .map(|c| match c {
            '%' => ".*".to_string(),
            '_' => ".".to_string(),
            other => regex::escape(&other.to_string()),
        })
        .collect();

    regex::Regex::new(&format!("(?is)^{escaped}$"))
        .map(|expression| expression.is_match(name))
        .unwrap_or(false)
}

/// @return The count at one column of a single-row result
pub(crate) fn count_at(batch: &RecordBatch, index: usize) -> Option<i64> {
    let column = batch.columns().get(index)?;

    if (batch.num_rows() == 0) || column.is_null(0) {
        return None;
    }

    column
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .map(|values| values.value(0))
        .or_else(|| {
            column
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .map(|values| i64::from(values.value(0)))
        })
}

/// The system property holding the version a caller means by the server's.
pub(crate) const VERSION_PROPERTY: &str = "version.gpudb_core_version";

// --------------------------------------------------------------------------
// Connection

pub struct KineticaConnection {
    /// The client, and the server session it holds. Shared with every
    /// statement opened here, and with the readers they hand out, so that the
    /// session ends only once the last of them has let go.
    session: Arc<Session>,
    /// What this connection has been told -- schema, catalog, and the server
    /// options to put on a statement -- shared with every statement opened
    /// here, so a change reaches one that is already open.
    context: context::Shared,
    /// Whether the transaction calls are accepted and do nothing, rather than
    /// refused. Kinetica has none either way.
    fake_transactions: bool,
    /// What a caller said about meaning to write. Reported back and nothing
    /// more: it is a hint, and what a connection may do is the server's to
    /// decide from the permissions it was given.
    read_only: bool,
}

impl Optionable for KineticaConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        match key {
            // Kinetica has no multi-statement transactions, so autocommit is
            // always on and turning it off cannot do anything. Asking for
            // autocommit is accepted, so that a tool which asks as a matter of
            // course still works. Asking to turn it off is refused unless the
            // caller has set `kinetica.fake_transactions` to have the
            // transaction calls accepted and ignored; even then, a later
            // rollback cannot undo anything. The refusal says "not
            // implemented" because that is what a caller asking on principle
            // degrades on; anything else stops it dead.
            OptionConnection::AutoCommit => match value {
                OptionValue::String(ref v) if v.eq_ignore_ascii_case("true") => Ok(()),
                _ if self.fake_transactions => Ok(()),
                _ => Err(self.no_transactions("disabling autocommit", Status::NotImplemented)),
            },
            OptionConnection::Other(ref name) if name == crate::options::FAKE_TRANSACTIONS => {
                match value {
                    OptionValue::String(ref v) => {
                        self.fake_transactions = v.eq_ignore_ascii_case("true");
                        Ok(())
                    }
                    _ => Err(unsupported(
                        "a fake-transactions setting that is not true or false",
                    )),
                }
            }
            // A hint: it is recorded and reported back, and the server decides
            // what a connection may actually do.
            OptionConnection::ReadOnly => match value {
                OptionValue::String(ref said) => {
                    self.read_only = said.eq_ignore_ascii_case("true");
                    Ok(())
                }
                _ => Err(unsupported("a read-only setting that is not true or false")),
            },
            // Every statement opened from here looks for unqualified names in
            // this schema, which is what the server calls the current one.
            // A schema the server does not have is refused here, rather than by
            // the first statement that looks a name up in it.
            OptionConnection::CurrentSchema => match value {
                OptionValue::String(schema) => {
                    // The lookup takes a pattern, so the match is made exact here
                    let known = self
                        .schemas_matching(Some(&schema))?
                        .iter()
                        .any(|found| found.eq_ignore_ascii_case(&schema));
                    if !known {
                        return Err(missing(&format!("schema {schema} does not exist")));
                    }

                    self.context.lock().unwrap().schema = Some(schema);
                    Ok(())
                }
                _ => Err(unsupported("a schema that is not a name")),
            },
            // Which catalogs there are is the server's business; it says so
            // when the statement runs rather than being asked here.
            OptionConnection::CurrentCatalog => match value {
                OptionValue::String(catalog) => {
                    self.context.lock().unwrap().catalog = Some(catalog);
                    Ok(())
                }
                _ => Err(unsupported("a catalog that is not a name")),
            },
            // The server options the statements opened here carry, and the
            // cap on what a read hands back.
            OptionConnection::Other(ref name) => {
                let OptionValue::String(ref said) = value else {
                    return Err(unsupported(&format!("{name}, which takes a string")));
                };

                if name == crate::options::MAX_ROWS {
                    self.context.lock().unwrap().max_rows = Some(context::rows(name, said)?);
                    return Ok(());
                }

                if name == crate::options::USE_KEY_LOOKUP {
                    self.context.lock().unwrap().use_key_lookup = context::flag(name, said)?;
                    return Ok(());
                }

                let mut context = self.context.lock().unwrap();

                if context.loading.set(name, said)?
                    || context.file_read.set(name, said)?
                    || context.query.set(name, said)?
                {
                    return Ok(());
                }

                Err(unsupported(&format!("connection option {name}")))
            }
            other => Err(unsupported(&format!(
                "connection option {}",
                other.as_ref()
            ))),
        }
    }

    /// What a caller reads back is what this connection actually does, which
    /// for the ones Kinetica settles for itself is not what was asked for.
    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        match key {
            // Always on: there are no transactions to hold anything back.
            OptionConnection::AutoCommit => Ok("true".to_string()),
            OptionConnection::ReadOnly => Ok(self.read_only.to_string()),
            OptionConnection::Other(ref name) if name == crate::options::FAKE_TRANSACTIONS => {
                Ok(self.fake_transactions.to_string())
            }
            // What was set, or what the server falls back to.
            OptionConnection::CurrentSchema => match self.context.lock().unwrap().schema.clone() {
                Some(schema) => Ok(schema),
                None => self.current_schema(),
            },
            OptionConnection::CurrentCatalog => {
                match self.context.lock().unwrap().catalog.clone() {
                    Some(catalog) => Ok(catalog),
                    None => Err(not_found("the current catalog")),
                }
            }
            OptionConnection::Other(ref name) if name == crate::options::MAX_ROWS => self
                .context
                .lock()
                .unwrap()
                .max_rows
                .map(|rows| rows.to_string())
                .ok_or_else(|| not_found(crate::options::MAX_ROWS)),
            OptionConnection::Other(ref name) if name == crate::options::USE_KEY_LOOKUP => {
                Ok(self.context.lock().unwrap().use_key_lookup.to_string())
            }
            OptionConnection::Other(ref name) => {
                let context = self.context.lock().unwrap();

                context
                    .loading
                    .get(name)
                    .or_else(|| context.file_read.get(name))
                    .or_else(|| context.query.get(name))
                    .ok_or_else(|| not_found(&format!("connection option {name}")))
            }
            other => Err(not_found(&format!("connection option {}", other.as_ref()))),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        Err(not_found(&format!("bytes option {}", key.as_ref())))
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        Err(not_found(&format!("integer option {}", key.as_ref())))
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        Err(not_found(&format!("double option {}", key.as_ref())))
    }
}

impl KineticaConnection {
    /// @return The schema an unqualified name is looked for in
    fn current_schema(&self) -> AdbcResult<String> {
        let batches = self.query("SELECT CURRENT_SCHEMA() AS s")?;

        let found = batches
            .first()
            .and_then(|batch| catalog::strings(batch, "s"))
            .filter(|values| values.len() > 0)
            .map(|values| values.value(0).to_string());

        found.ok_or_else(|| unsupported("a current schema, which the server did not name"))
    }

    /// @return A connection over the given client, with no inserters yet built
    pub(crate) fn new(
        client: Kinetica,
        fake_transactions: bool,
        context: context::Context,
        user: &str,
    ) -> AdbcResult<Self> {
        let session = Session::new(client, user);

        // A caller that named a user to act as means it from the first
        // statement, not from the first SET USER.
        session.impersonation().apply(&session)?;

        Ok(Self {
            session: Arc::new(session),
            context: Arc::new(std::sync::Mutex::new(context)),
            fake_transactions,
            read_only: false,
        })
    }

    /// @return An error saying the call means nothing here, for a caller that
    ///         has not asked for the transaction calls to be accepted and
    ///         ignored
    fn no_transactions(&self, what: &str, status: Status) -> AdbcError {
        error(
            &format!(
                "{what}: Kinetica has no transactions, so this would do nothing. Set {} to \
                 accept it and carry on regardless.",
                crate::options::FAKE_TRANSACTIONS
            ),
            status,
        )
    }

    /// Runs a catalog query and collects its batches.
    /// @return Every batch of the result
    fn query(&self, sql: &str) -> AdbcResult<Vec<RecordBatch>> {
        let stream = KineticaStatement::run(&self.session, sql, i64::from(u16::MAX), 0)?.stream;

        ArrowStreamReader::new(stream)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(from_arrow)
    }
}

/// Builds a WHERE clause from the filters a caller supplied, ignoring those
/// left unset. ADBC's filters are LIKE patterns, `%` and `_` included.
/// @return The clause, or an empty string when nothing was given
fn filter(conditions: &[(&str, Option<&str>)]) -> String {
    let clauses: Vec<String> = conditions
        .iter()
        .filter_map(|(column, value)| {
            value.map(|v| format!("{} LIKE '{}'", column, v.replace('\'', "''")))
        })
        .collect();

    if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    }
}

/// The parts of `get_statistics` that talk to the server, which the trait has
/// no place for.
impl KineticaConnection {
    /// Reads what is promised about the tables: the columns each constraint
    /// is made of, and, for a foreign key, where each of them points.
    ///
    /// Two queries rather than one. key_column_usage names the columns the
    /// constraint is on; for a foreign key, the second pairs each of them with
    /// the column it references. ADBC asks for the second only of foreign
    /// keys.
    fn constraints_of(
        &self,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        rows: &mut catalog::Rows,
    ) -> AdbcResult<()> {
        for batch in self.query(&format!(
            "SELECT k.table_schema, k.table_name, k.constraint_name, t.constraint_type, \
             k.column_name FROM information_schema.key_column_usage k \
             JOIN information_schema.table_constraints t \
             ON t.constraint_schema = k.constraint_schema \
             AND t.constraint_name = k.constraint_name \
             AND t.table_name = k.table_name{} ORDER BY k.ordinal_position",
            filter(&[("k.table_schema", db_schema), ("k.table_name", table_name),])
        ))? {
            let (Some(schemas), Some(tables), Some(names), Some(kinds), Some(columns)) = (
                catalog::strings(&batch, "table_schema"),
                catalog::strings(&batch, "table_name"),
                catalog::strings(&batch, "constraint_name"),
                catalog::strings(&batch, "constraint_type"),
                catalog::strings(&batch, "column_name"),
            ) else {
                continue;
            };

            for i in 0..names.len() {
                rows.add_constraint_column(
                    schemas.value(i),
                    tables.value(i),
                    names.value(i),
                    kinds.value(i),
                    columns.value(i),
                );
            }
        }

        // Where the foreign keys point: referential_constraints names the key
        // each one references, and the server pairs a foreign key's columns
        // with that key's by position, so the two are joined on it.
        for batch in self.query(&format!(
            "SELECT k.table_schema, k.table_name, k.constraint_name, p.table_schema \
             AS referenced_schema, p.table_name AS referenced_table, p.column_name \
             AS referenced_column FROM information_schema.key_column_usage k \
             JOIN information_schema.referential_constraints r \
             ON r.constraint_schema = k.constraint_schema \
             AND r.constraint_name = k.constraint_name \
             JOIN information_schema.key_column_usage p \
             ON p.constraint_schema = r.unique_constraint_schema \
             AND p.constraint_name = r.unique_constraint_name \
             AND p.ordinal_position = k.ordinal_position{} ORDER BY k.ordinal_position",
            filter(&[("k.table_schema", db_schema), ("k.table_name", table_name)])
        ))? {
            let (
                Some(schemas),
                Some(tables),
                Some(names),
                Some(referenced_schemas),
                Some(referenced_tables),
                Some(referenced_columns),
            ) = (
                catalog::strings(&batch, "table_schema"),
                catalog::strings(&batch, "table_name"),
                catalog::strings(&batch, "constraint_name"),
                catalog::strings(&batch, "referenced_schema"),
                catalog::strings(&batch, "referenced_table"),
                catalog::strings(&batch, "referenced_column"),
            )
            else {
                continue;
            };

            for i in 0..names.len() {
                rows.add_constraint_usage(
                    schemas.value(i),
                    tables.value(i),
                    names.value(i),
                    referenced_schemas.value(i),
                    referenced_tables.value(i),
                    referenced_columns.value(i),
                );
            }
        }

        Ok(())
    }

    /// @return The schemas whose name matches, or all of them
    fn schemas_matching(&self, pattern: Option<&str>) -> AdbcResult<Vec<String>> {
        let mut names = Vec::new();

        for batch in self.query(&format!(
            "SELECT schema_name FROM information_schema.schemata{}",
            filter(&[("schema_name", pattern)])
        ))? {
            if let Some(found) = catalog::strings(&batch, "schema_name") {
                names.extend((0..found.len()).map(|i| found.value(i).to_string()));
            }
        }

        Ok(names)
    }

    /// Reads one schema's tables and their sizes in a single call, since the
    /// row count is the one statistic the server already has for all of them.
    /// @return Each matching table and the rows it holds
    fn tables_matching(
        &self,
        schema: &str,
        pattern: Option<&str>,
    ) -> AdbcResult<Vec<(String, i64)>> {
        // Sizes make the server count the rows as well.
        let options = ShowTableRequestOptions {
            get_sizes: "true".to_string(),
            ..Default::default()
        };

        let response = runtime::block_on(
            self.session
                .show_table_with_opts(schema.to_string(), options),
        )
        .map_err(could_not("list the tables"))?;

        Ok(response
            .table_names
            .iter()
            .zip(response.sizes.iter())
            .filter(|(name, _)| matches(name, pattern))
            .map(|(name, rows)| (name.clone(), *rows))
            .collect())
    }

    /// Counts the nulls and the distinct values of every column in one pass,
    /// rather than one query per column.
    /// @return What the pass found
    fn column_facts(
        &self,
        schema: &str,
        table: &str,
        rows: i64,
    ) -> AdbcResult<Vec<statistics::Fact>> {
        let qualified = format!("{}.{}", quote(schema), quote(table));
        let columns = self.table_columns(&qualified)?;

        if columns.is_empty() || (rows == 0) {
            return Ok(Vec::new());
        }

        let selected: Vec<String> = columns
            .iter()
            .flat_map(|column| {
                let name = quote(column);
                [
                    format!("COUNT(*) - COUNT({name})"),
                    format!("COUNT(DISTINCT {name})"),
                ]
            })
            .collect();

        let query = format!("SELECT {} FROM {qualified}", selected.join(", "));
        let mut facts = Vec::new();

        for batch in self.query(&query)? {
            for (index, column) in columns.iter().enumerate() {
                for (offset, key) in [(0, Statistics::NullCount), (1, Statistics::DistinctCount)] {
                    let Some(value) = count_at(&batch, (index * 2) + offset) else {
                        continue;
                    };

                    facts.push(statistics::Fact {
                        schema: schema.to_string(),
                        table: table.to_string(),
                        column: Some(column.clone()),
                        key,
                        value,
                    });
                }
            }
        }

        Ok(facts)
    }

    /// @return The table's column names, in order
    fn table_columns(&self, qualified: &str) -> AdbcResult<Vec<String>> {
        let stream = KineticaStatement::run(
            &self.session,
            &format!("SELECT * FROM {qualified} LIMIT 0"),
            1,
            0,
        )?
        .stream;

        let reader = ArrowStreamReader::new(stream)?;

        Ok(reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect())
    }
}

impl Connection for KineticaConnection {
    type StatementType = KineticaStatement;

    fn new_statement(&mut self) -> AdbcResult<Self::StatementType> {
        Ok(KineticaStatement::new(
            Arc::clone(&self.session),
            Arc::clone(&self.context),
        ))
    }

    /// A connection does not know which query is running on it, so there is
    /// nothing here to name; a statement cancels its own.
    fn cancel(&mut self) -> AdbcResult<()> {
        Err(unsupported(
            "cancelling a connection; cancel the statement instead",
        ))
    }

    // ----------------------------------------------------------------------
    // Catalog metadata
    // Not yet implemented. Building these on one information_schema query per
    // depth is the next piece of work; a call per table would make GetObjects
    // cost minutes on a large catalog.

    /// Tools ask for this on connect to decide what the driver can do, so a
    /// server that will not answer costs the version rather than the call.
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        let version = runtime::block_on(self.session.show_system_properties())
            .ok()
            .and_then(|response| response.property_map.get(VERSION_PROPERTY).cloned());

        let batch = catalog::info_batch(codes.as_ref(), version)?;

        Ok(Box::new(SingleBatchReader::new(batch)))
    }

    /// Reads the catalog from information_schema, one query per depth, and
    /// assembles the nested shape ADBC prescribes.
    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        // Kinetica's one catalog is unnamed, so only a pattern that matches an
        // empty name, nothing but `%`, selects it.
        if catalog.is_some_and(|pattern| !pattern.chars().all(|c| c == '%')) {
            return Ok(Box::new(SingleBatchReader::new(RecordBatch::new_empty(
                Arc::clone(&schemas::GET_OBJECTS_SCHEMA),
            ))));
        }

        let mut rows = catalog::Rows::new();

        for batch in self.query(&format!(
            "SELECT schema_name FROM information_schema.schemata{}",
            filter(&[("schema_name", db_schema)])
        ))? {
            if let Some(names) = catalog::strings(&batch, "schema_name") {
                for i in 0..names.len() {
                    rows.add_schema(names.value(i));
                }
            }
        }

        if !matches!(depth, ObjectDepth::Catalogs | ObjectDepth::Schemas) {
            let kinds = table_type
                .map(|types| {
                    let quoted: Vec<String> = types
                        .iter()
                        .map(|t| format!("'{}'", t.replace('\'', "''")))
                        .collect();
                    format!(" AND table_type IN ({})", quoted.join(", "))
                })
                .unwrap_or_default();

            for batch in self.query(&format!(
                "SELECT table_schema, table_name, table_type FROM information_schema.tables{}{}",
                filter(&[("table_schema", db_schema), ("table_name", table_name)]),
                kinds
            ))? {
                let (Some(schemas), Some(names), Some(types)) = (
                    catalog::strings(&batch, "table_schema"),
                    catalog::strings(&batch, "table_name"),
                    catalog::strings(&batch, "table_type"),
                ) else {
                    continue;
                };

                for i in 0..names.len() {
                    rows.add_schema(schemas.value(i));
                    rows.add_table(schemas.value(i), names.value(i), types.value(i));
                }
            }
        }

        if matches!(depth, ObjectDepth::All | ObjectDepth::Columns) {
            for batch in self.query(&format!(
                "SELECT table_schema, table_name, column_name, data_type, is_nullable, \
                 ordinal_position FROM information_schema.columns{}",
                filter(&[
                    ("table_schema", db_schema),
                    ("table_name", table_name),
                    ("column_name", column_name),
                ])
            ))? {
                let (Some(schemas), Some(tables), Some(names), Some(types)) = (
                    catalog::strings(&batch, "table_schema"),
                    catalog::strings(&batch, "table_name"),
                    catalog::strings(&batch, "column_name"),
                    catalog::strings(&batch, "data_type"),
                ) else {
                    continue;
                };

                let positions = catalog::ints(&batch, "ordinal_position");
                let nullable = catalog::ints(&batch, "is_nullable");

                for i in 0..names.len() {
                    rows.add_column(
                        schemas.value(i),
                        tables.value(i),
                        names.value(i),
                        // information_schema counts from 0, ADBC from 1
                        positions.map(|p| p.value(i) + 1).unwrap_or(0),
                        types.value(i),
                        nullable.map(|n| n.value(i) != 0).unwrap_or(true),
                    );
                }
            }
        }

        if matches!(depth, ObjectDepth::All) {
            self.constraints_of(db_schema, table_name, &mut rows)?;
        }

        let batch = catalog::build(depth, rows)?;

        Ok(Box::new(SingleBatchReader::new(batch)))
    }

    /// Asks the server for the table with no rows, so the schema comes back
    /// through the same mapping a query would use rather than being rebuilt
    /// from catalog text.
    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        let qualified = match db_schema {
            Some(schema) => format!("{}.{}", quote(schema), quote(table_name)),
            None => quote(table_name),
        };

        let query = format!("SELECT * FROM {qualified} LIMIT 0");
        let stream = KineticaStatement::run(&self.session, &query, 1, 0)?.stream;

        Ok(ArrowStreamReader::new(stream)?.schema().as_ref().clone())
    }

    fn get_table_types(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        let types = StringArray::from(vec![
            "BASE TABLE",
            "VIEW",
            "MATERIALIZED VIEW",
            "EXTERNAL TABLE",
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schemas::GET_TABLE_TYPES_SCHEMA),
            vec![Arc::new(types)],
        )
        .map_err(from_arrow)?;

        Ok(Box::new(SingleBatchReader::new(batch)))
    }

    fn get_statistic_names(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(Box::new(SingleBatchReader::new(statistics::names_batch()?)))
    }

    /// Reports the row count Kinetica keeps standing for every matching table,
    /// and, when exact values are asked for, the null and distinct counts of
    /// each of their columns. Those are not kept, so asking for them is asking
    /// for a pass over the table, which is why they wait to be asked for.
    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut facts = Vec::new();

        for schema in self.schemas_matching(db_schema)? {
            for (table, rows) in self.tables_matching(&schema, table_name)? {
                facts.push(statistics::Fact {
                    schema: schema.clone(),
                    table: table.clone(),
                    column: None,
                    key: Statistics::RowCount,
                    value: rows,
                });

                if !approximate {
                    facts.extend(self.column_facts(&schema, &table, rows)?);
                }
            }
        }

        Ok(Box::new(SingleBatchReader::new(statistics::batch(&facts)?)))
    }

    // ----------------------------------------------------------------------
    // Transactions
    // Kinetica has none. These accept and do nothing, so that a tool driving
    // the connection the way it drives a transactional one is not stopped by
    // it.

    /// Every statement has already taken effect, so there is nothing to make
    /// permanent. Saying so is the default; a caller that would rather have
    /// the call accepted and ignored sets `kinetica.fake_transactions`
    /// on the connection.
    fn commit(&mut self) -> AdbcResult<()> {
        if self.fake_transactions {
            return Ok(());
        }

        Err(self.no_transactions("commit", Status::InvalidState))
    }

    /// There is nothing to undo either, and a caller that rolls back after an
    /// error it had handled should learn that here rather than believe it.
    fn rollback(&mut self) -> AdbcResult<()> {
        if self.fake_transactions {
            return Ok(());
        }

        Err(self.no_transactions("rollback", Status::InvalidState))
    }

    /// A partition is a range of the query it came from, so reading one runs
    /// that query again rather than picking up a handle the server holds.
    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        let piece = partition::Partition::decode(partition.as_ref())?;

        let page = KineticaStatement::run_page(
            &self.session,
            &piece.query,
            piece.limit,
            piece.offset,
            piece.parameters,
            statement::Run {
                paging_table: piece.paging_table.clone(),
                ..statement::Run::from(&context::Context::read(&self.context))
            },
        )?;

        // Asked for a result that is gone, the server runs the query again
        // under the same name rather than failing. A read of the result as it
        // was names no other tables and keeps its total; a fresh run names the
        // tables it made, and its total moves if the data has. Its rows are a
        // different snapshot from the other pieces', so they are refused, and
        // what it made is cleared, since nothing else knows it is there.
        if piece.paging_table.is_some() {
            let ran_again = !page.result_tables.is_empty() || piece.total != page.total;

            if ran_again {
                let mut made = page.result_tables;
                made.push(page.paging_table);
                KineticaStatement::discard(&self.session, &made);

                return Err(error(
                    "could not read the partition: the result it was cut from is gone. \
                     The statement that executed the partitions was released, or the \
                     result outlived its time to live; execute the partitions again",
                    Status::InvalidState,
                ));
            }
        }

        Ok(Box::new(ArrowStreamReader::new(page.stream)?))
    }
}
