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

//! The statement: what a query or an ingest is set up on and run from.

use std::sync::Arc;

use adbc_core::error::Result as AdbcResult;
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::sync::Statement;
use adbc_core::Optionable;
use arrow_array::{Int64Array, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema};

use gpudb_api::{ClearTablesRequestOptions, ExecuteSqlRequest, ExecuteSqlRequestOptions, Kinetica};

use crate::context::{self, Context};
use crate::control::{self, Control};
use crate::ddl;
use crate::error::{could_not, from_arrow, invalid, not_found, not_ready, unsupported};
use crate::file;
use crate::ingest;
use crate::insert;
use crate::kifs;
use crate::lookup;
use crate::params;
use crate::partition;
use crate::query::QueryOptions;
use crate::reader::{ArrowStreamReader, ChainedReader, PagedReader, SingleBatchReader};
use crate::runtime;
use crate::session::Session;

/// Rows per request, matching the server's own `max_get_records_size`.
pub(crate) const DEFAULT_BATCH_SIZE: i64 = 20_000;

/// The encoding that makes the server answer with an Arrow IPC stream.
pub(crate) const ARROW_ENCODING: &str = "arrow";

/// The /admin/alter/jobs action that stops a running job.
pub(crate) const CANCEL_ACTION: &str = "cancel";

/// The option the server reads the catalog from, which the generated request
/// options have no field for.
const CURRENT_CATALOG: &str = "current_catalog";

/// @return A one-row result holding the count, for a statement the driver
///         carried out itself and so has no server response to hand back
fn counted(files: i64) -> AdbcResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "files_transferred",
        DataType::Int64,
        false,
    )]));

    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![files]))]).map_err(from_arrow)
}

/// Minutes, which is what the TTL is counted in. Long enough for a reader that
/// pauses between pages, short enough that an abandoned one is not left about.
const PAGING_TABLE_MINUTES: i64 = 20;

/// What every result this process asks the server to keep is named with. The
/// process id is in it, so a table left in sys_sql_temp says which process
/// made it, and the live tests count only their own among other sessions'.
/// @return The prefix of this process's paging table names
pub fn paging_table_prefix() -> String {
    format!("__ADBC_Paging_{:x}_", std::process::id())
}

/// @return A name for a result this driver is about to ask the server to keep
pub(crate) fn paging_table_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    format!(
        "{}{:x}_{:x}",
        paging_table_prefix(),
        since.as_nanos() as u64,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The response info key saying what kind of statement was run.
pub(crate) const GROUP_INFO_KEY: &str = "X-Kinetica-Group";

/// The value it holds for a statement that produced a result.
pub(crate) const QUERY_GROUP: &str = "QUERY";

/// The value it holds for a statement that defines or alters an object.
const DDL_GROUP: &str = "DDL";

#[derive(Default)]
pub(crate) struct Counts {
    affected: std::sync::atomic::AtomicI64,
    total: std::sync::atomic::AtomicI64,
    /// Whether the last read went to the rank holding its row. Kept beside
    /// the counts because it is read the same way, after a call that had
    /// nowhere to put it.
    key_lookup: std::sync::atomic::AtomicI64,
}

impl Counts {
    /// Keeps what the server said, so it can be asked for afterwards.
    pub(crate) fn record(&self, page: &Page) {
        use std::sync::atomic::Ordering;

        self.affected.store(page.affected, Ordering::Relaxed);
        self.total.store(page.total, Ordering::Relaxed);
    }

    /// Keeps what the server said about one of several runs of a statement,
    /// added to what the runs before it said, so a caller is told about the
    /// read as a whole. A count of -1, meaning none, adds nothing.
    pub(crate) fn record_after(&self, page: &Page, before: (i64, i64)) {
        use std::sync::atomic::Ordering;

        let plus = |earlier: i64, this: i64| match (earlier < 0, this < 0) {
            (_, true) => earlier,
            (true, false) => this,
            (false, false) => earlier + this,
        };

        self.affected
            .store(plus(before.0, page.affected), Ordering::Relaxed);
        self.total
            .store(plus(before.1, page.total), Ordering::Relaxed);
    }

    /// @return The rows affected and the total, as they stand
    pub(crate) fn snapshot(&self) -> (i64, i64) {
        use std::sync::atomic::Ordering;

        (
            self.affected.load(Ordering::Relaxed),
            self.total.load(Ordering::Relaxed),
        )
    }

    /// Keeps a count the driver worked out itself, for a statement that
    /// never reached the server and so has no page to read it from.
    pub(crate) fn set_affected(&self, affected: i64) {
        use std::sync::atomic::Ordering;

        self.affected.store(affected, Ordering::Relaxed);
        self.total.store(-1, Ordering::Relaxed);
    }

    /// Says which way the read that is about to run was routed.
    pub(crate) fn set_key_lookup(&self, used: bool) {
        use std::sync::atomic::Ordering;

        self.key_lookup.store(i64::from(used), Ordering::Relaxed);
    }

    /// @return The count under the given driver-specific option name
    pub(crate) fn get(&self, name: &str) -> Option<i64> {
        use std::sync::atomic::Ordering;

        match name {
            crate::options::COUNT_AFFECTED => Some(self.affected.load(Ordering::Relaxed)),
            crate::options::TOTAL_RECORDS => Some(self.total.load(Ordering::Relaxed)),
            crate::options::KEY_LOOKUP => Some(self.key_lookup.load(Ordering::Relaxed)),
            _ => None,
        }
    }
}

/// The response info key listing the tables a statement left behind besides
/// its paging table, which are cleared with it.
const RESULT_TABLE_LIST_KEY: &str = "result_table_list";

/// What one request to /execute/sql came back with.
pub(crate) struct Page {
    /// The result the server materialized, empty when it made none.
    pub(crate) paging_table: String,
    /// Other tables the statement left behind, which go when it does.
    pub(crate) result_tables: Vec<String>,
    pub(crate) stream: Vec<u8>,
    /// Rows the statement changed, or -1 when it was a query.
    pub(crate) affected: i64,
    /// Rows the whole result holds, however many this page carried, or -1
    /// when the statement produced no result.
    pub(crate) total: i64,
    pub(crate) has_more: bool,
}

/// What a run carries beyond the statement itself: where unqualified names
/// are looked for, which result it reads, and the server options the
/// connection was configured with.
///
/// Owned rather than borrowed, since a paging reader carries one into a
/// closure that outlives the statement that built it.
#[derive(Debug, Clone, Default)]
pub(crate) struct Run {
    /// The result to read, named so that every page agrees with the first.
    pub paging_table: Option<String>,
    pub schema: Option<String>,
    pub catalog: Option<String>,
    /// What the caller asked the server to do with its statements.
    pub options: QueryOptions,
}

impl From<&Context> for Run {
    /// @return A run against what the connection has been told
    fn from(context: &Context) -> Self {
        Self {
            paging_table: None,
            schema: context.schema.clone(),
            catalog: context.catalog.clone(),
            options: context.query.clone(),
        }
    }
}

/// What reading one result needs, owned, since a reader running the statement
/// once per bound row opens each result after the statement call has returned.
#[derive(Clone)]
struct Source {
    session: Arc<Session>,
    query: String,
    batch_size: i64,
    counts: Arc<Counts>,
    run: Run,
}

impl Source {
    /// Runs the query and pages through what it made, letting go of it once
    /// the reader is done.
    /// @param parameters The row of parameters to run it with, if any
    /// @param before What earlier runs of the same read counted, which this
    ///     run's counts add to; none for a read's first run
    /// @return A reader over the result, uncapped
    fn open(
        self,
        parameters: Option<String>,
        before: Option<(i64, i64)>,
    ) -> AdbcResult<PagedReader> {
        let Source {
            session,
            query,
            batch_size,
            counts,
            run,
        } = self;

        // Named before the first page, so every page after it reads the result
        // the first one left behind rather than running the query again.
        let wanted = paging_table_name();
        let made: Arc<std::sync::Mutex<Option<String>>> = Arc::default();
        let made_for_cleanup = Arc::clone(&made);
        let tables: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
        let kept_tables = Arc::clone(&tables);
        let fetching = Arc::clone(&session);

        let reader = PagedReader::new(Box::new(move |offset| {
            let asked_for = made
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| wanted.clone());

            let page = KineticaStatement::run_page(
                &fetching,
                &query,
                batch_size,
                offset,
                parameters.clone(),
                Run {
                    paging_table: Some(asked_for),
                    ..run.clone()
                },
            )?;

            // A single-table query is answered without one being made, and
            // then there is nothing to hold on to.
            if !page.paging_table.is_empty() {
                *made.lock().unwrap() = Some(page.paging_table.clone());
            }

            // Whatever else the statement left behind goes when it does. A
            // read of the kept result names nothing, but one that had to run
            // the query again names what it made, so each page is looked at,
            // and a name already held is not held twice.
            let mut kept = kept_tables.lock().unwrap();

            for table in &page.result_tables {
                if !kept.contains(table) {
                    kept.push(table.clone());
                }
            }

            drop(kept);

            // Every page carries the same totals, so the first one to arrive
            // is what a caller asking afterwards is told.
            match before {
                None => counts.record(&page),
                Some(before) => counts.record_after(&page, before),
            }

            Ok((page.stream, page.has_more))
        }))?;

        // The server keeps the result until it is told otherwise or the time
        // to live runs out; a reader that is done with it says so.
        Ok(reader.on_finished(Box::new(move || {
            let mut going = std::mem::take(&mut *tables.lock().unwrap());

            if let Some(table) = made_for_cleanup.lock().unwrap().take() {
                going.push(table);
            }

            KineticaStatement::discard(&session, &going);
        })))
    }
}

// --------------------------------------------------------------------------
// Statement

pub struct KineticaStatement {
    /// The client, and the server session it holds, shared with the
    /// connection this was opened from and with the readers handed out here.
    session: Arc<Session>,
    query: String,
    batch_size: i64,
    /// Parameters bound for the next execution, if any.
    bound: Option<RecordBatch>,
    /// Rows bound for bulk ingest, which are read once and consumed.
    stream: Option<Box<dyn RecordBatchReader + Send>>,
    /// The table a bound stream is written to.
    target: Option<String>,
    /// Whether that table is expected to exist, or is made first.
    mode: IngestMode,
    /// What the last run reported, shared with the reader it handed out.
    counts: Arc<Counts>,
    /// What the connection has been told: the schema unqualified names are
    /// looked for in, the catalog, and the server options to send.
    context: context::Shared,
    /// The schema of the table an ingest writes to, when it is not the one
    /// the connection is set to.
    target_schema: Option<String>,
    /// A cap on this statement's reads, which beats the connection's.
    max_rows: Option<i64>,
    /// Whether this statement may be answered by the rank holding its row,
    /// which beats the connection's answer.
    use_key_lookup: Option<bool>,
    /// What execute_partitions left on the server. A partitioned read has no
    /// one reader to finish, and the pieces are read on other connections
    /// that cannot know which of them is last, so the statement that made the
    /// result is what lets go of it.
    partitioned: Vec<String>,
}

impl Drop for KineticaStatement {
    /// Lets go of a partitioned result. A consumer still holding partitions
    /// loses what they point at, which is the same bargain a result set makes
    /// when it is closed: reading one afterwards is refused rather than run
    /// again (see `read_partition`).
    fn drop(&mut self) {
        if self.partitioned.is_empty() {
            return;
        }

        Self::discard(&self.session, &std::mem::take(&mut self.partitioned));
    }
}

impl KineticaStatement {
    /// @return A statement not yet given anything to run
    pub(crate) fn new(session: Arc<Session>, context: context::Shared) -> Self {
        Self {
            session,
            query: String::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            bound: None,
            stream: None,
            target: None,
            mode: IngestMode::Append,
            counts: Arc::default(),
            context,
            target_schema: None,
            max_rows: None,
            use_key_lookup: None,
            partitioned: Vec::new(),
        }
    }

    /// Runs the query for one page.
    /// @return What came back
    pub(crate) fn run(
        client: &Kinetica,
        query: &str,
        batch_size: i64,
        offset: i64,
    ) -> AdbcResult<Page> {
        Self::run_with(client, query, batch_size, offset, None)
    }

    /// Runs one page, substituting the given parameters if there are any.
    /// @return What came back
    pub(crate) fn run_with(
        client: &Kinetica,
        query: &str,
        batch_size: i64,
        offset: i64,
        parameters: Option<String>,
    ) -> AdbcResult<Page> {
        Self::run_page(
            client,
            query,
            batch_size,
            offset,
            parameters,
            Run::default(),
        )
    }

    /// Runs one page against a named result, which is what keeps the pages of
    /// one read agreeing with each other.
    /// @return What came back
    pub(crate) fn run_page(
        client: &Kinetica,
        query: &str,
        batch_size: i64,
        offset: i64,
        parameters: Option<String>,
        run: Run,
    ) -> AdbcResult<Page> {
        let mut options = ExecuteSqlRequestOptions::default();

        // Unqualified names in the statement are looked for here, which is
        // what a caller means by setting a schema on the connection.
        if let Some(schema) = &run.schema {
            options.current_schema = schema.clone();
        }

        // The JSON array substituted for $1, $2 and so on.
        if let Some(json) = parameters {
            options.query_parameters = json;
        }

        // Without a name the server runs the query again for every page, and
        // a table changing underneath them moves the rows between offsets. The
        // first page names one; the rest ask for the one it made.
        if let Some(table) = &run.paging_table {
            options.paging_table = table.clone();
            options.paging_table_ttl = PAGING_TABLE_MINUTES.to_string();
        }

        let mut options = options.to_map();

        // The catalog the server resolves names against, where it federates
        // more than its own. Not a field of the generated options.
        if let Some(catalog) = &run.catalog {
            options.insert(CURRENT_CATALOG.to_string(), catalog.clone());
        }

        // Last, so that a caller asking for a paging table TTL of its own
        // gets that rather than the default set above.
        run.options.apply(&mut options);

        let request = ExecuteSqlRequest {
            statement: query.to_string(),
            offset,
            limit: batch_size,
            encoding: ARROW_ENCODING.to_string(),
            request_schema_str: String::new(),
            data: Vec::new(),
            options,
        };

        // The raw form sends the request as built and hands back the payload
        // undecoded; the decoded one insists on Avro and transposes it.
        let response = runtime::block_on(client.execute_sql_raw(&request))
            .map_err(could_not("run the statement"))?;

        // The server says which of the two counts it filled: a query reports
        // how many rows the whole result holds and nothing about changes, and
        // everything else reports the changes and nothing about a result.
        let group = response.info.get(GROUP_INFO_KEY).map(String::as_str);

        // What the server says it made for this statement, beside the paging
        // table. It is documented as empty when no paging table was made, and
        // is read only then, as GPUdbSqlIterator reads it: these are cleared
        // later, so a name is not taken from a statement that kept nothing.
        let result_tables = if response.paging_table.is_empty() {
            Vec::new()
        } else {
            response
                .info
                .get(RESULT_TABLE_LIST_KEY)
                .map(|list| {
                    list.split(',')
                        .map(str::trim)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default()
        };

        Ok(Page {
            paging_table: response.paging_table,
            result_tables,
            stream: response.binary_encoded_response.into_vec(),
            // A definition changes no rows, whatever count the server gives
            affected: match group {
                Some(QUERY_GROUP) => -1,
                Some(DDL_GROUP) => 0,
                _ => response.count_affected,
            },
            total: response.total_number_of_records,
            has_more: response.has_more_records,
        })
    }

    /// Runs the query for no rows at all, which still answers with the schema
    /// and the size of the whole result.
    /// @return What came back, carrying no rows
    fn probe(
        &self,
        client: &Kinetica,
        query: &str,
        parameters: Option<String>,
    ) -> AdbcResult<Page> {
        // Asking for no rows still materializes the result and reports its
        // size, so the pieces that follow read what this call left behind.
        Self::run_page(
            client,
            query,
            0,
            0,
            parameters,
            Run {
                paging_table: Some(paging_table_name()),
                ..self.run_context()
            },
        )
    }

    /// @return Each bound row as the JSON array Kinetica substitutes, or
    ///     nothing when no parameters are bound
    fn parameter_rows(&self) -> AdbcResult<Option<Vec<String>>> {
        let Some(batch) = &self.bound else {
            return Ok(None);
        };

        // Run once per row, a query bound to no rows would run not at all,
        // and leave no result to say what its columns would have been.
        if batch.num_rows() == 0 {
            return Err(invalid(
                "a query bound to no rows of parameters has nothing to run; bind \
                 at least one row",
            ));
        }

        (0..batch.num_rows())
            .map(|row| params::row_to_json(batch, row))
            .collect::<AdbcResult<Vec<_>>>()
            .map(Some)
    }

    /// @return The bound parameters as the JSON array Kinetica substitutes,
    ///     for a read that runs the query once, which is only one row's
    fn parameters(&self) -> AdbcResult<Option<String>> {
        match self.parameter_rows()? {
            None => Ok(None),
            Some(rows) if rows.len() == 1 => Ok(rows.into_iter().next()),
            Some(_) => Err(unsupported(
                "binding more than one row to a partitioned query; bind a single \
                 row, or use execute to read every row's result in turn",
            )),
        }
    }

    /// Sends a bound stream wherever it can go fastest.
    ///
    /// A named target table is ADBC's own bulk ingest, and a plain
    /// `INSERT INTO t VALUES (?, ?)` says the same thing in SQL, so both reach
    /// the workers directly. Anything else the statement does to its values —
    /// an expression, a function, a SELECT, an ON CONFLICT clause — only the
    /// server can do, so it runs the statement once per row.
    /// @return The number of rows the stream accounted for
    fn consume(&self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<i64> {
        if let Some(table) = self.qualified_target() {
            // Only a named target says what a table made here would be called,
            // so a mode that creates one belongs to this branch alone.
            if let Some(sql) = ddl::create(&table, reader.schema().as_ref(), self.mode)? {
                // A table made here is a table whose shape this connection
                // may already have an answer for.
                self.session.retrievers().forget();

                Self::run_page(
                    &self.session,
                    &sql,
                    self.batch_size,
                    0,
                    None,
                    self.run_context(),
                )?;
            }

            return ingest::ingest(
                Arc::clone(&self.session),
                &table,
                ingest::Mapping::ByName,
                &ingest::Built::from(&Context::read(&self.context)),
                reader,
            );
        }

        if self.query.is_empty() {
            return Err(unsupported(
                "a bound stream with neither a target table nor a query",
            ));
        }

        if let Some(plain) = insert::recognize(&self.query) {
            return ingest::ingest(
                Arc::clone(&self.session),
                &plain.table,
                match plain.columns.as_deref() {
                    Some(named) => ingest::Mapping::Named(named),
                    None => ingest::Mapping::InOrder,
                },
                &ingest::Built::from(&Context::read(&self.context)),
                reader,
            );
        }

        self.apply_each(reader)
    }

    /// Runs the statement once for every row of the stream, which is what a
    /// statement the workers cannot carry needs.
    /// @return The number of rows affected in total
    fn apply_each(&self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<i64> {
        let mut total = 0;

        for batch in reader {
            let batch = batch.map_err(from_arrow)?;

            for row in 0..batch.num_rows() {
                let parameters = params::row_to_json(&batch, row)?;
                total += Self::run_page(
                    &self.session,
                    &self.query,
                    self.batch_size,
                    0,
                    Some(parameters),
                    self.run_context(),
                )?
                .affected
                .max(0);
            }
        }

        Ok(total)
    }

    /// Builds the reader that pages through the whole result. Bound to several
    /// rows of parameters, the query runs once for each, and the reader hands
    /// back each run's rows in turn, as ADBC defines it.
    fn reader(&self) -> AdbcResult<Box<dyn RecordBatchReader + Send>> {
        // A SELECT that pins every key column can be answered by the rank
        // holding the row, without the head rank or the SQL engine. A bound
        // statement is left to the server: its values are not in the text, so
        // the key cannot be read from it.
        // A statement that could have changed a table makes what this
        // connection remembers about its tables stale.
        self.forget_changed_tables();

        let context = Context::read(&self.context);

        // A catalog is the server's to resolve, and only /execute/sql is told
        // which one; the calls a lookup makes would read the table of
        // whichever catalog is the default. So a connection federating more
        // than its own leaves its reads to the server.
        let planned = match (&self.bound, &context.catalog) {
            (None, None) if self.use_key_lookup(&context) => {
                lookup::plan(&self.session, &self.query, context.schema.as_deref())
            }
            _ => None,
        };

        self.counts.set_key_lookup(planned.is_some());

        if let Some(planned) = planned {
            return Ok(Box::new(self.key_reader(planned)?));
        }

        let source = Source {
            session: Arc::clone(&self.session),
            query: self.query.clone(),
            batch_size: self.batch_size,
            counts: Arc::clone(&self.counts),
            run: self.run_context(),
        };
        let max_rows = self.max_rows();

        let Some(rows) = self.parameter_rows()? else {
            return Ok(Box::new(source.open(None, None)?.with_limit(max_rows)));
        };

        // The cap is the read's, not each run's, so the chain keeps it.
        Ok(Box::new(ChainedReader::new(
            rows.len(),
            Box::new(move |run| {
                // A run's counts add to what the runs before it said, so the
                // total grows as each starts, and is the whole read's once the
                // last has.
                let before = (run > 0).then(|| source.counts.snapshot());
                source.clone().open(Some(rows[run].clone()), before)
            }),
            max_rows,
        )?))
    }

    /// Lets go of what the server was keeping for a result: the paging table
    /// and whatever else the statement left behind, in one request. Nothing
    /// is done about a failure: it all has a time to live, and a caller
    /// clearing up has nothing better to report.
    pub(crate) fn discard(client: &Kinetica, tables: &[String]) {
        let tables: Vec<String> = tables
            .iter()
            .filter(|table| !table.is_empty())
            .cloned()
            .collect();

        // The server refuses an empty list outright.
        if tables.is_empty() {
            return;
        }

        // A table the server already let go is a success, not a failure, and
        // does not stop the rest being cleared.
        let options = ClearTablesRequestOptions {
            no_error_if_not_exists: "true".to_string(),
        };

        let _ = runtime::block_on(client.clear_tables_with_opts(tables, options));
    }

    /// @return The schema unqualified names in this statement are looked for in
    fn schema(&self) -> Option<String> {
        Context::read(&self.context).schema
    }

    /// @return A run against what the connection this was opened from has
    ///         been told
    fn run_context(&self) -> Run {
        Run::from(&Context::read(&self.context))
    }

    /// The statement's own cap beats the connection's.
    /// @return The most rows a read here hands back
    fn max_rows(&self) -> Option<i64> {
        self.max_rows.or(Context::read(&self.context).max_rows)
    }

    /// @return Whether a read here may go to the rank holding its row
    fn use_key_lookup(&self, context: &Context) -> bool {
        self.use_key_lookup.unwrap_or(context.use_key_lookup)
    }

    /// Loads a local file into a table, for a statement whose rows are here
    /// rather than on the server.
    /// @return The number of rows loaded
    fn load_file(&self, found: file::FromFile) -> AdbcResult<i64> {
        let context = Context::read(&self.context);
        let settings = context.file_read.and(&found.said)?;
        let files = file::matching(&found.path)?;

        let table = match &context.schema {
            Some(schema) if !found.table.contains('.') => format!("{schema}.{}", found.table),
            _ => found.table.clone(),
        };

        // Emptying the table first is part of the load, so it happens after
        // the file is known to be readable and before any row goes.
        let rows = file::Rows::new(files, settings.clone(), found.selected.as_deref())?;

        if settings.initial_clear {
            Self::run_page(
                &self.session,
                &format!("DELETE FROM {table}"),
                self.batch_size,
                0,
                None,
                self.run_context(),
            )?;
        }

        ingest::ingest(
            Arc::clone(&self.session),
            &table,
            match found.columns.as_deref() {
                Some(named) => ingest::Mapping::Named(named),
                // Without a column list the file's own header says where each
                // column goes; with no header there is nothing to go on but
                // the order they come in.
                None if settings.has_header => ingest::Mapping::ByName,
                None => ingest::Mapping::InOrder,
            },
            &ingest::Built::from(&context),
            Box::new(rows),
        )
    }

    /// Carries out a statement that changes what the connection is, rather
    /// than asking the server anything: who it acts as, and where its
    /// unqualified names are looked for.
    /// @return Whether this was one of those, and nothing was sent
    fn control(&self) -> AdbcResult<bool> {
        let Some(control) = control::recognize(&self.query) else {
            return Ok(false);
        };

        match control {
            Control::Act { user, revertible } => {
                self.session
                    .impersonation()
                    .carry_out(&self.session, user, revertible)?;
            }
            Control::Revert => {
                self.session
                    .impersonation()
                    .carry_out_revert(&self.session)?;
            }
            Control::Schema(schema) => {
                self.context.lock().unwrap().schema = (!schema.is_empty()).then_some(schema);
            }
            Control::Catalog(catalog) => {
                self.context.lock().unwrap().catalog = (!catalog.is_empty()).then_some(catalog);
            }
        }

        // Every one of these changes what a bare table name means -- a
        // different user sees different tables, and a different schema or
        // catalog resolves to different ones -- so what this connection has
        // worked out about its tables no longer holds.
        self.session.retrievers().forget();

        Ok(true)
    }

    /// Lets go of what this connection remembers about its tables, unless
    /// the statement it is about to run leaves every table as it was.
    fn forget_changed_tables(&self) {
        if !lookup::reads_only(&self.query) {
            self.session.retrievers().forget();
        }
    }

    /// Builds the reader for a key lookup, which pages the same way a read
    /// through the head rank does: the worker caps a response at the server's
    /// own limit and says whether more is waiting.
    fn key_reader(&self, planned: lookup::Planned) -> AdbcResult<PagedReader> {
        let counts = Arc::clone(&self.counts);
        let max_rows = self.max_rows();

        Ok(PagedReader::new(Box::new(move |offset| {
            let page = planned.read(offset)?;

            // Every page carries the same total, so the first one to arrive is
            // what a caller asking afterwards is told.
            counts.record(&page);

            Ok((page.stream, page.has_more))
        }))?
        .with_limit(max_rows))
    }

    /// @return The ingest target, under whichever schema was named for it
    fn qualified_target(&self) -> Option<String> {
        let table = self.target.as_ref()?;

        // A target that names its own schema keeps it; otherwise the
        // connection's stands in, so an unqualified target means what an
        // unqualified name in a query means.
        let schema = self.target_schema.clone().or_else(|| self.schema());

        Some(match schema {
            Some(schema) if !table.contains('.') => format!("{schema}.{table}"),
            _ => table.clone(),
        })
    }

    /// Remembers what the server said about a statement that produced no
    /// reader to carry it.
    fn record(&self, page: &Page) {
        self.counts.record(page);
    }
}

impl Optionable for KineticaStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        match (key, value) {
            (OptionStatement::Other(name), OptionValue::Int(rows))
                if name == crate::options::BATCH_SIZE =>
            {
                self.batch_size = rows.clamp(1, DEFAULT_BATCH_SIZE);
                Ok(())
            }
            (OptionStatement::Other(ref name), OptionValue::String(ref rows))
                if name == crate::options::MAX_ROWS =>
            {
                self.max_rows = Some(context::rows(name, rows)?);
                Ok(())
            }
            (OptionStatement::Other(ref name), OptionValue::Int(rows))
                if name == crate::options::MAX_ROWS =>
            {
                self.max_rows = Some(rows);
                Ok(())
            }
            (OptionStatement::Other(ref name), OptionValue::String(ref said))
                if name == crate::options::USE_KEY_LOOKUP =>
            {
                self.use_key_lookup = Some(context::flag(name, said)?);
                Ok(())
            }
            (OptionStatement::TargetTable, OptionValue::String(table)) => {
                self.target = Some(table);
                Ok(())
            }
            (OptionStatement::TargetDbSchema, OptionValue::String(schema)) => {
                self.target_schema = Some(schema);
                Ok(())
            }
            (OptionStatement::IngestMode, OptionValue::String(mode)) => {
                self.mode = match mode.as_str() {
                    "adbc.ingest.mode.append" => IngestMode::Append,
                    "adbc.ingest.mode.create" => IngestMode::Create,
                    "adbc.ingest.mode.replace" => IngestMode::Replace,
                    "adbc.ingest.mode.create_append" => IngestMode::CreateAppend,
                    other => return Err(unsupported(&format!("ingest mode {other}"))),
                };

                Ok(())
            }
            (other, _) => Err(unsupported(&format!("statement option {}", other.as_ref()))),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        self.get_option_int(key.clone())
            .map(|value| value.to_string())
            .map_err(|_| not_found(&format!("statement option {}", key.as_ref())))
    }

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        Err(not_found(&format!("bytes option {}", key.as_ref())))
    }

    /// The counts the server reported for the last run. A call that returns a
    /// reader has nowhere to put them, so they are read from here instead.
    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        self.counts
            .get(key.as_ref())
            .ok_or_else(|| not_found(&format!("integer option {}", key.as_ref())))
    }

    /// The total row count doubles as ADBC's own maximum progress, which is
    /// what a caller watching a query's size reads.
    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        if matches!(key, OptionStatement::MaxProgress) {
            return Ok(self.counts.get(crate::options::TOTAL_RECORDS).unwrap_or(-1) as f64);
        }

        self.get_option_int(key.clone())
            .map(|value| value as f64)
            .map_err(|_| not_found(&format!("double option {}", key.as_ref())))
    }
}

impl Statement for KineticaStatement {
    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        self.bound = Some(batch);
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        self.stream = Some(reader);
        Ok(())
    }

    fn execute(&mut self) -> AdbcResult<Box<dyn RecordBatchReader + Send + 'static>> {
        // Nothing is sent for one of these, and there is nothing to read
        // back: the result is that the connection is now different.
        if self.control()? {
            self.counts.set_affected(0);

            return Ok(Box::new(SingleBatchReader::new(RecordBatch::new_empty(
                Arc::new(Schema::empty()),
            ))));
        }

        // The rows of these are here rather than on the server, so the
        // driver reads them and the statement is never sent.
        if let Some(found) = file::recognize(&self.query) {
            let loaded = self.load_file(found)?;
            self.counts.set_affected(loaded);

            return Ok(Box::new(SingleBatchReader::new(counted(loaded)?)));
        }

        // A transfer answers with how many files moved, since a consumer
        // that ran it through execute still expects a result to read.
        if let Some(transfer) = kifs::recognize(&self.query) {
            let moved = transfer.checked()?.run(&self.session)?;
            self.counts.set_affected(moved);

            return Ok(Box::new(SingleBatchReader::new(counted(moved)?)));
        }

        self.reader()
    }

    /// Applies each bound row in turn, which is what binding several rows to
    /// an update means.
    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        if self.control()? {
            self.counts.set_affected(0);

            return Ok(Some(0));
        }

        self.forget_changed_tables();

        if let Some(reader) = self.stream.take() {
            return self.consume(reader).map(Some);
        }

        // Binding a batch rather than a stream is how a caller hands over one
        // batch at a time; for an ingest it means the same thing.
        if self.target.is_some() || insert::recognize(&self.query).is_some() {
            if let Some(batch) = self.bound.take() {
                return self
                    .consume(Box::new(SingleBatchReader::new(batch)))
                    .map(Some);
            }
        }

        if let Some(table) = &self.target {
            return Err(not_ready(&format!(
                "an ingest into {table} has nothing bound to load; bind data first"
            )));
        }

        if let Some(found) = file::recognize(&self.query) {
            let loaded = self.load_file(found)?;
            self.counts.set_affected(loaded);

            return Ok(Some(loaded));
        }

        // Files are here, not on the server, so this statement is carried
        // out rather than sent.
        if let Some(transfer) = kifs::recognize(&self.query) {
            let moved = transfer.checked()?.run(&self.session)?;
            self.counts.set_affected(moved);

            return Ok(Some(moved));
        }

        let Some(batch) = self.bound.clone() else {
            let page = Self::run_page(
                &self.session,
                &self.query,
                self.batch_size,
                0,
                None,
                self.run_context(),
            )?;
            self.record(&page);

            // A query run this way changed nothing and says so with -1, which
            // is what ADBC reads as a count it does not have.
            return Ok(Some(page.affected));
        };

        let mut total = 0;

        for row in 0..batch.num_rows() {
            let parameters = params::row_to_json(&batch, row)?;
            let page = Self::run_page(
                &self.session,
                &self.query,
                self.batch_size,
                0,
                Some(parameters),
                self.run_context(),
            )?;
            self.record(&page);
            total += page.affected.max(0);
        }

        Ok(Some(total))
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        Ok(self.reader()?.schema().as_ref().clone())
    }

    /// Asks for the result's size and shape without its rows, and slices it
    /// into pieces a consumer can read at once.
    fn execute_partitions(&mut self) -> AdbcResult<adbc_core::PartitionedResult> {
        let parameters = self.parameters()?;

        let page = self.probe(&self.session, &self.query, parameters.clone())?;

        self.record(&page);

        // Held until this statement goes, which is what frees it: see the
        // field's own note on why no reader can do it.
        Self::discard(&self.session, &std::mem::take(&mut self.partitioned));

        self.partitioned = page.result_tables.clone();

        if !page.paging_table.is_empty() {
            self.partitioned.push(page.paging_table.clone());
        }

        Ok(adbc_core::PartitionedResult {
            partitions: partition::slice(
                &self.query,
                parameters,
                (!page.paging_table.is_empty()).then(|| page.paging_table.clone()),
                page.total,
                self.batch_size,
            ),
            schema: ArrowStreamReader::new(page.stream)?
                .schema()
                .as_ref()
                .clone(),
            rows_affected: page.affected,
        })
    }

    /// Nothing here knows what a parameter is meant to hold: the server is not
    /// asked to prepare the statement, and it does not say. The count is read
    /// from the statement itself and every parameter is left untyped, which is
    /// how Arrow says it does not know.
    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        let fields: Vec<Field> = (1..=params::count(&self.query))
            .map(|position| Field::new(format!("${position}"), DataType::Null, true))
            .collect();

        Ok(Schema::new(fields))
    }

    /// Nothing is prepared server-side: parameters are substituted per call,
    /// so there is no handle to hold. Succeeding lets a caller that prepares
    /// before executing work, which is what ADBC expects of it.
    fn prepare(&mut self) -> AdbcResult<()> {
        Ok(())
    }

    /// Anything bound was bound for the statement being replaced, so it is
    /// dropped rather than carried into a query it was never meant for.
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> AdbcResult<()> {
        self.query = query.as_ref().to_string();
        self.bound = None;
        self.stream = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> AdbcResult<()> {
        Err(unsupported("Substrait plans"))
    }

    /// The server does not hand back a handle for a running query, so the one
    /// to stop is found among the jobs by the text it was sent as. Two
    /// identical queries in flight at once cannot be told apart, and both are
    /// cancelled.
    fn cancel(&mut self) -> AdbcResult<()> {
        let response = runtime::block_on(self.session.admin_show_jobs())
            .map_err(could_not("find the running statement"))?;

        let mine: Vec<i64> = response
            .job_id
            .iter()
            .zip(response.query_text.iter())
            .filter(|(_, text)| text.as_str() == self.query.as_str())
            .map(|(id, _)| *id)
            .collect();

        if mine.is_empty() {
            return Ok(());
        }

        runtime::block_on(
            self.session
                .admin_alter_jobs(mine, CANCEL_ACTION.to_string()),
        )
        .map_err(could_not("cancel the statement"))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_paging_table_is_named_for_the_process_that_asked_for_it() {
        let name = paging_table_name();

        assert!(name.starts_with(&paging_table_prefix()), "{name}");
        assert!(
            name.contains(&format!("_{:x}_", std::process::id())),
            "{name}"
        );
        assert_ne!(name, paging_table_name(), "two names alike");
    }
}
