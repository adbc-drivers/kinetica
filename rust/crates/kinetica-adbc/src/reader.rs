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

//! Readers over the Arrow IPC streams the server returns.
//!
//! ADBC has no offset or limit, so a result larger than one request is paged
//! here and the consumer sees one continuous stream. Two behaviors of the
//! server shape this:
//!
//! * a paging table is not created for single-table queries even when asked
//!   for, so the statement is re-issued at successive offsets rather than read
//!   from a materialized result;
//! * without one the read is not a snapshot, so concurrent deletes shift the
//!   offsets and rows can be skipped. That is the server's existing behavior
//!   for paged reads, not something introduced here.

use std::sync::Arc;

use adbc_core::error::Result as AdbcResult;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{ArrowError, Schema};

use crate::error::from_arrow;

/// Reads the batches out of one response.
pub struct ArrowStreamReader {
    inner: StreamReader<std::io::Cursor<Vec<u8>>>,
}

impl ArrowStreamReader {
    /// @return A reader over the stream, or an error if it is not readable
    pub fn new(stream: Vec<u8>) -> AdbcResult<Self> {
        let inner =
            StreamReader::try_new(std::io::Cursor::new(stream), None).map_err(from_arrow)?;
        Ok(Self { inner })
    }
}

impl Iterator for ArrowStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl RecordBatchReader for ArrowStreamReader {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// Fetches one page of a result.
///
/// @return The stream for that page and whether the server has more
pub type PageFetcher = Box<dyn FnMut(i64) -> AdbcResult<(Vec<u8>, bool)> + Send>;

/// Presents a paged result as one stream.
///
/// The first page is fetched on construction, because a consumer may ask for
/// the schema before reading anything.
pub struct PagedReader {
    fetch: PageFetcher,
    /// Run when the reader is dropped, to let go of whatever the server kept
    /// on its behalf. A consumer that stops reading half way still gets here.
    finished: Option<Box<dyn FnOnce() + Send>>,
    page: ArrowStreamReader,
    schema: Arc<Schema>,
    rows_delivered: i64,
    has_more: bool,
    /// The offset the current page was fetched at, so a page that yields
    /// nothing cannot be requested again.
    page_offset: i64,
    /// The most rows to hand back, whatever the result holds. The last batch
    /// is cut short rather than overshooting.
    limit: Option<i64>,
}

impl PagedReader {
    pub fn new(mut fetch: PageFetcher) -> AdbcResult<Self> {
        let (stream, has_more) = fetch(0)?;
        let page = ArrowStreamReader::new(stream)?;
        let schema = page.schema();

        Ok(Self {
            fetch,
            finished: None,
            page,
            schema,
            rows_delivered: 0,
            has_more,
            page_offset: 0,
            limit: None,
        })
    }

    /// Caps what the reader hands back. A negative or absent cap means all of
    /// it, which is what the option means when nobody has set it.
    /// @return The reader, capped
    pub fn with_limit(mut self, limit: Option<i64>) -> Self {
        self.limit = limit.filter(|rows| *rows >= 0);
        self
    }

    /// Gives the reader something to do once nothing more will be read from it.
    pub fn on_finished(mut self, finished: Box<dyn FnOnce() + Send>) -> Self {
        self.finished = Some(finished);
        self
    }

    /// Replaces the exhausted page with the next one.
    /// @return False once the server reports nothing further
    fn advance(&mut self) -> Result<bool, ArrowError> {
        // A page that delivered no rows would otherwise be asked for again at
        // the same offset, so the stream ends instead of spinning.
        if !self.has_more || (self.rows_delivered <= self.page_offset) {
            return Ok(false);
        }

        let (stream, has_more) = (self.fetch)(self.rows_delivered)
            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        self.page =
            ArrowStreamReader::new(stream).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
        self.page_offset = self.rows_delivered;
        self.has_more = has_more;

        Ok(true)
    }
}

impl Drop for PagedReader {
    fn drop(&mut self) {
        if let Some(finished) = self.finished.take() {
            finished();
        }
    }
}

impl Iterator for PagedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Asked for a number of rows, the reader stops at it rather than
            // reading a page it would have to throw most of away.
            if self.limit.is_some_and(|limit| self.rows_delivered >= limit) {
                return None;
            }

            if let Some(batch) = self.page.next() {
                let batch = batch.map(|batch| match self.limit {
                    // The batch that reaches the cap is cut at it; the rows
                    // after it are the server's, not the caller's.
                    Some(limit) if self.rows_delivered + batch.num_rows() as i64 > limit => {
                        batch.slice(0, (limit - self.rows_delivered) as usize)
                    }
                    _ => batch,
                });

                if let Ok(ref b) = batch {
                    self.rows_delivered += b.num_rows() as i64;
                }

                return Some(batch);
            }

            match self.advance() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl RecordBatchReader for PagedReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

/// Opens the result of one run of a statement, given which run it is.
pub type RunOpener = Box<dyn FnMut(usize) -> AdbcResult<PagedReader> + Send>;

/// Presents the results of several runs of one statement as one stream, which
/// is how ADBC reads a query bound to several rows of parameters: each run's
/// rows follow the last's, in the order the rows were bound.
///
/// The first run is opened on construction, for its schema. Each of the rest
/// is opened only once the one before it has been read to the end and let go,
/// so the server holds one run's result at a time, and a consumer that stops
/// early runs none of the rest.
pub struct ChainedReader {
    open: RunOpener,
    runs: usize,
    /// How many runs have been opened, which is also the next one to open.
    opened: usize,
    current: Option<PagedReader>,
    schema: Arc<Schema>,
    rows_delivered: i64,
    /// The most rows to hand back across every run together.
    limit: Option<i64>,
}

impl ChainedReader {
    /// @return A reader over the given number of runs, the first of them
    ///     already open, capped at the given number of rows in all
    pub fn new(runs: usize, mut open: RunOpener, limit: Option<i64>) -> AdbcResult<Self> {
        let limit = limit.filter(|rows| *rows >= 0);
        let first = open(0)?.with_limit(limit);
        let schema = first.schema();

        Ok(Self {
            open,
            runs,
            opened: 1,
            current: Some(first),
            schema,
            rows_delivered: 0,
            limit,
        })
    }

    /// Opens the next run, which must answer with the columns the first did.
    /// @return The run's reader, capped at what is left of the limit
    fn open_next(&mut self) -> Result<PagedReader, ArrowError> {
        let run = self.opened;
        self.opened += 1;

        let reader = (self.open)(run).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        if !same_columns(&reader.schema(), &self.schema) {
            return Err(ArrowError::SchemaError(format!(
                "the run for row {} answered with columns {:?}, where the first \
                 row's answered with {:?}",
                run + 1,
                reader.schema().fields(),
                self.schema.fields()
            )));
        }

        Ok(reader.with_limit(self.limit.map(|limit| limit - self.rows_delivered)))
    }
}

/// @return Whether two results have the same columns, by name and type. What
///     else a field carries, such as whether it is nullable, can differ from
///     one run to the next without changing what the stream holds.
fn same_columns(a: &Schema, b: &Schema) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields()
            .iter()
            .zip(b.fields().iter())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}

impl Iterator for ChainedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.limit.is_some_and(|limit| self.rows_delivered >= limit) {
                return None;
            }

            if let Some(current) = &mut self.current {
                match current.next() {
                    Some(Ok(batch)) => {
                        self.rows_delivered += batch.num_rows() as i64;

                        // Every batch is handed out under the stream's own
                        // schema, whatever a later run said beyond the columns.
                        return Some(if batch.schema() == self.schema {
                            Ok(batch)
                        } else {
                            RecordBatch::try_new(Arc::clone(&self.schema), batch.columns().to_vec())
                        });
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    // Let go of before the next run is made, so the server
                    // never holds two of them for this reader.
                    None => self.current = None,
                }
            }

            if self.opened >= self.runs {
                return None;
            }

            match self.open_next() {
                Ok(reader) => self.current = Some(reader),
                Err(e) => {
                    // A failed run ends the stream rather than being tried again.
                    self.opened = self.runs;
                    return Some(Err(e));
                }
            }
        }
    }
}

impl RecordBatchReader for ChainedReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

/// Presents one already-built batch as a reader, for the fixed answers ADBC
/// asks for such as the list of table types.
pub struct SingleBatchReader {
    schema: Arc<Schema>,
    batch: Option<RecordBatch>,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        Self {
            schema: batch.schema(),
            batch: Some(batch),
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch.take().map(Ok)
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field};
    use std::sync::Mutex;

    /// @return One page as the server sends it: an IPC stream of one batch
    fn page(field: Field, values: Arc<dyn Array>) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values]).unwrap();
        let mut stream = Vec::new();
        let mut writer = StreamWriter::try_new(&mut stream, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        stream
    }

    /// @return A one-page result of the given integers
    fn ints(values: &[i32]) -> PagedReader {
        let stream = page(
            Field::new("n", DataType::Int32, true),
            Arc::new(Int32Array::from(values.to_vec())),
        );

        PagedReader::new(Box::new(move |_| Ok((stream.clone(), false)))).unwrap()
    }

    /// @return Every integer the reader hands back, in order
    fn read_all(reader: ChainedReader) -> Vec<i32> {
        reader
            .flat_map(|batch| {
                let batch = batch.unwrap();
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                column.values().to_vec()
            })
            .collect()
    }

    /// @return An opener for one run per list, noting which runs it opened
    fn runs(results: Vec<Vec<i32>>, opened: Arc<Mutex<Vec<usize>>>) -> RunOpener {
        Box::new(move |run| {
            opened.lock().unwrap().push(run);
            Ok(ints(&results[run]))
        })
    }

    #[test]
    fn each_runs_rows_follow_the_last_runs_in_order() {
        let opened = Arc::default();
        let reader = ChainedReader::new(
            3,
            runs(vec![vec![2], vec![3, 4], vec![5]], Arc::clone(&opened)),
            None,
        )
        .unwrap();

        assert_eq!(read_all(reader), vec![2, 3, 4, 5]);
        assert_eq!(*opened.lock().unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn a_run_with_no_rows_adds_nothing_and_the_next_still_follows() {
        let reader = ChainedReader::new(
            3,
            runs(vec![vec![1], vec![], vec![2]], Arc::default()),
            None,
        )
        .unwrap();

        assert_eq!(read_all(reader), vec![1, 2]);
    }

    #[test]
    fn a_cap_counts_every_run_and_the_runs_past_it_are_never_made() {
        let opened = Arc::default();
        let reader = ChainedReader::new(
            4,
            runs(
                vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]],
                Arc::clone(&opened),
            ),
            Some(3),
        )
        .unwrap();

        assert_eq!(read_all(reader), vec![1, 2, 3]);
        assert_eq!(*opened.lock().unwrap(), vec![0, 1]);
    }

    #[test]
    fn only_the_first_run_is_made_before_anything_is_read() {
        let opened = Arc::default();
        let reader = ChainedReader::new(
            3,
            runs(vec![vec![1], vec![2], vec![3]], Arc::clone(&opened)),
            None,
        )
        .unwrap();

        assert_eq!(reader.schema().field(0).name(), "n");
        assert_eq!(*opened.lock().unwrap(), vec![0]);
    }

    #[test]
    fn a_run_answering_with_other_columns_ends_the_stream_with_an_error() {
        let opener: RunOpener = Box::new(|run| {
            if run == 0 {
                return Ok(ints(&[1]));
            }

            let stream = page(
                Field::new("n", DataType::Utf8, true),
                Arc::new(StringArray::from(vec!["two"])),
            );
            PagedReader::new(Box::new(move |_| Ok((stream.clone(), false))))
        });
        let mut reader = ChainedReader::new(3, opener, None).unwrap();

        assert!(reader.next().unwrap().is_ok());
        assert!(matches!(
            reader.next(),
            Some(Err(ArrowError::SchemaError(_)))
        ));
        assert!(reader.next().is_none());
    }

    #[test]
    fn a_later_run_differing_only_in_nullability_takes_the_streams_schema() {
        let opener: RunOpener = Box::new(|run| {
            if run == 0 {
                return Ok(ints(&[1]));
            }

            let stream = page(
                Field::new("n", DataType::Int32, false),
                Arc::new(Int32Array::from(vec![2])),
            );
            PagedReader::new(Box::new(move |_| Ok((stream.clone(), false))))
        });
        let reader = ChainedReader::new(2, opener, None).unwrap();
        let schema = reader.schema();

        for batch in reader {
            assert_eq!(batch.unwrap().schema(), schema);
        }
    }
}
