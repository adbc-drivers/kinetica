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

//! Bulk ingest, which reaches the workers directly rather than going through
//! SQL on the head rank.
//!
//! The target table's shape is only known at runtime, so records carry their
//! own schema instead of being a compiled struct. Values are converted to the
//! Avro base types Kinetica's wire format uses, which for the date-like and
//! the wide numeric types means text.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, TimeUnit};
use serde::ser::{Serialize, SerializeMap, Serializer}; // codespell:ignore ser

use gpudb_api::{
    BulkInserter, Kinetica, KineticaError, KineticaRecord, ShardKeyExtractor, ShardKeyValue,
    ShardKeyValues,
};

use crate::context::Context;
use crate::error::{from_arrow, incompatible, to_adbc, unsupported};
use crate::loading::Loading;
use crate::options;
use crate::runtime;
use crate::session::Session;

/// Flush workers, one per expected rank plus headroom.
const NUM_WORKERS: usize = 16;

/// The Avro base type a column is written as, refined by the Kinetica property
/// that says what the text or the integer actually means.
#[derive(Clone, Copy, PartialEq)]
enum Target {
    Int,
    Long,
    Float,
    Double,
    Bytes,
    Text,
    Date,
    DateTime,
    /// A TIMESTAMP column, which the server holds as milliseconds since the
    /// epoch, UTC: an Arrow timestamp of any unit is brought to that first.
    Timestamp,
    Time,
    Uuid,
    /// A geometry column declared as text. The server always sends WKB on the
    /// Arrow path whatever the column is, so a batch read back from it has to
    /// be converted before a text column will take it.
    Geometry,
    /// A geometry column declared as bytes, which holds WKB. It takes WKB as it
    /// comes, and WKT once converted.
    GeometryWkb,
}

/// Routing carries the column name as a `&'static str`, so names are interned
/// once and shared by every ingest into the same table.
/// @return The interned name
fn intern(name: &str) -> &'static str {
    static NAMES: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();

    let mut names = NAMES
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap();

    if let Some(found) = names.get(name) {
        return found;
    }

    let interned: &'static str = Box::leak(name.to_string().into_boxed_str());
    names.insert(interned);

    interned
}

/// One column of the target table.
struct Column {
    name: &'static str,
    target: Target,
    is_shard_key: bool,
}

/// The target table's shape, shared by every record of one ingest.
struct IngestSchema {
    columns: Vec<Column>,
}

/// A value in the form Avro will write it.
enum Value {
    Null,
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // A nullable column is a union of null and the type, and the variant is
        // picked from the value itself, so nothing here has to know which.
        match self {
            Value::Null => serializer.serialize_none(),
            Value::Int(v) => serializer.serialize_i32(*v),
            Value::Long(v) => serializer.serialize_i64(*v),
            Value::Float(v) => serializer.serialize_f32(*v),
            Value::Double(v) => serializer.serialize_f64(*v),
            Value::Text(v) => serializer.serialize_str(v),
            Value::Bytes(v) => serializer.serialize_bytes(v),
        }
    }
}

/// One row, carrying the schema because its shape is not known at compile time.
struct Record {
    schema: Arc<IngestSchema>,
    values: Vec<Value>,
}

impl Serialize for Record {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // A record is serialized as a map of column name to value, which the
        // Avro writer matches against the schema's fields.
        let mut map = serializer.serialize_map(Some(self.values.len()))?;

        for (column, value) in self.schema.columns.iter().zip(self.values.iter()) {
            map.serialize_entry(column.name, value)?;
        }

        map.end()
    }
}

impl KineticaRecord for Record {
    /// The real schema is read from the table and handed to the inserter when
    /// it is built, so nothing asks the type for it.
    fn avro_schema(&self) -> &'static str {
        ""
    }

    fn type_properties() -> HashMap<String, Vec<String>> {
        HashMap::new()
    }
}

impl ShardKeyExtractor for Record {
    fn shard_key_values(&self) -> ShardKeyValues<'_> {
        self.schema
            .columns
            .iter()
            .zip(self.values.iter())
            .filter(|(column, _)| column.is_shard_key)
            .map(|(column, value)| (column.name, shard_key_value(value)))
            .collect()
    }
}

/// Routing needs the value in the same form the server hashes it in, which for
/// the text-backed types is the text itself.
/// @return The routing value; a null hashes as zero bytes in the column's
/// width, which sends it to the rank the server would pick for it
fn shard_key_value(value: &Value) -> ShardKeyValue<'_> {
    match value {
        Value::Null => ShardKeyValue::Null,
        Value::Int(v) => ShardKeyValue::Int(*v),
        Value::Long(v) => ShardKeyValue::Long(*v),
        Value::Float(v) => ShardKeyValue::Float(*v),
        Value::Double(v) => ShardKeyValue::Double(*v),
        Value::Text(v) => ShardKeyValue::String(v),
        // No column holding bytes can be a shard key.
        Value::Bytes(_) => ShardKeyValue::Null,
    }
}

/// Reads the target table's columns, in the order the Avro schema declares.
/// None is marked as a shard key yet: which ones route a record is the
/// inserter's to say, once it is built from the schema text read here.
/// @return The schema to build records against, and the Avro schema text
fn describe(client: &Kinetica, table: &str) -> AdbcResult<(IngestSchema, String)> {
    let response = runtime::block_on(client.show_table(table.to_string())).map_err(to_adbc)?;

    let schema_text = response
        .type_schemas
        .first()
        .cloned()
        .ok_or_else(|| unsupported(&format!("ingest into '{table}', which has no type")))?;

    let properties = response.properties.first().cloned().unwrap_or_default();

    let parsed: serde_json::Value = serde_json::from_str(&schema_text)
        .map_err(|e| unsupported(&format!("the type of '{table}' ({e})")))?;

    let fields = parsed
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| unsupported(&format!("the type of '{table}', which has no fields")))?;

    let mut columns = Vec::with_capacity(fields.len());

    for field in fields {
        let name = field
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| unsupported("a column without a name"))?;

        let empty = Vec::new();
        let column_properties = properties.get(name).unwrap_or(&empty);

        columns.push(Column {
            target: target_of(field.get("type"), column_properties),
            is_shard_key: false,
            name: intern(name),
        });
    }

    Ok((IngestSchema { columns }, schema_text))
}

/// @return The Avro base type of a column, looking through a nullable union
fn base_type(node: Option<&serde_json::Value>) -> &str {
    match node {
        Some(serde_json::Value::String(name)) => name.as_str(),
        Some(serde_json::Value::Array(variants)) => variants
            .iter()
            .filter_map(|v| v.as_str())
            .find(|v| *v != "null")
            .unwrap_or("string"),
        _ => "string",
    }
}

/// @return How a column's values have to be written
fn target_of(node: Option<&serde_json::Value>, properties: &[String]) -> Target {
    let base = base_type(node);

    for property in properties {
        match property.as_str() {
            "date" => return Target::Date,
            "datetime" => return Target::DateTime,
            "timestamp" if base == "long" => return Target::Timestamp,
            "time" => return Target::Time,
            "uuid" => return Target::Uuid,
            // Geometry is text or binary by its base type, and either may be
            // given WKB or WKT, so both are told apart from plain text and
            // bytes.
            "wkt" if base == "string" => return Target::Geometry,
            "wkt" if base == "bytes" => return Target::GeometryWkb,
            _ => {}
        }
    }

    match base {
        "int" => Target::Int,
        "long" => Target::Long,
        "float" => Target::Float,
        "double" => Target::Double,
        "bytes" => Target::Bytes,
        _ => Target::Text,
    }
}

/// Casts a column to the Arrow type the target is read from, which leaves the
/// conversions arrow-rs already knows how to do to arrow-rs.
/// @return The cast column
fn coerce(column: &ArrayRef, target: Target) -> AdbcResult<ArrayRef> {
    let wanted = match target {
        Target::Int => DataType::Int32,
        Target::Long => DataType::Int64,
        Target::Float => DataType::Float32,
        Target::Double => DataType::Float64,
        Target::Bytes | Target::GeometryWkb => DataType::Binary,
        Target::Date => DataType::Date32,
        Target::DateTime => DataType::Timestamp(TimeUnit::Millisecond, None),
        // Milliseconds, keeping the zone: a zoned timestamp holds UTC
        // instants, so changing the unit is all it needs. Any other column is
        // taken as milliseconds already, as a plain long column takes it.
        Target::Timestamp => match column.data_type() {
            DataType::Timestamp(_, zone) => {
                DataType::Timestamp(TimeUnit::Millisecond, zone.clone())
            }
            _ => DataType::Int64,
        },
        Target::Time => DataType::Time32(TimeUnit::Millisecond),
        // A UUID arrives either as its text or as the sixteen bytes behind the
        // extension type, and only the first can be cast.
        Target::Uuid | Target::Text | Target::Geometry => DataType::Utf8,
    };

    if column.data_type() == &wanted {
        return Ok(Arc::clone(column));
    }

    if target == Target::Uuid && matches!(column.data_type(), DataType::FixedSizeBinary(16)) {
        return Ok(Arc::clone(column));
    }

    // A vector is written from bytes, and its values buffer already holds the
    // little-endian floats the server stores, so the rows are handed over
    // whole rather than cast; arrow-rs has no cast for this pair anyway.
    if target == Target::Bytes
        && matches!(column.data_type(), DataType::FixedSizeList(field, _)
            if field.data_type() == &DataType::Float32)
    {
        return Ok(Arc::clone(column));
    }

    // Strict, so that a pair arrow-rs will not convert is reported rather than
    // quietly turned into nulls, which is what the default (safe) options do.
    let options = arrow_cast::CastOptions {
        safe: false,
        format_options: Default::default(),
    };

    // A geometry arrives as WKB (what the server sends, and geoarrow.wkb) or as
    // WKT, in any of Arrow's binary or string layouts. Each is brought to its
    // plain layout and converted a value at a time below; a cast from one kind
    // to the other would read WKB as text, or send text as WKB.
    if matches!(target, Target::Geometry | Target::GeometryWkb) {
        let plain = match column.data_type() {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Some(DataType::Binary)
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(DataType::Utf8),
            _ => None,
        };

        if let Some(plain) = plain {
            return arrow_cast::cast_with_options(column.as_ref(), &plain, &options)
                .map_err(from_arrow);
        }
    }

    arrow_cast::cast_with_options(column.as_ref(), &wanted, &options).map_err(from_arrow)
}

/// @return The value at one row of an already-coerced column, or an error
/// for a geometry that is neither WKB nor WKT this driver can read
fn value_at(column: &ArrayRef, target: Target, row: usize) -> AdbcResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }

    Ok(match target {
        Target::Int => Value::Int(
            column
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(row),
        ),
        Target::Long => Value::Long(
            column
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        ),
        Target::Timestamp => Value::Long(match column.data_type() {
            DataType::Timestamp(..) => column
                .as_primitive::<arrow_array::types::TimestampMillisecondType>()
                .value(row),
            _ => column
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        }),
        Target::Float => Value::Float(
            column
                .as_primitive::<arrow_array::types::Float32Type>()
                .value(row),
        ),
        Target::Double => Value::Double(
            column
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(row),
        ),
        Target::Bytes => match column.data_type() {
            // A vector: its values buffer is already the little-endian floats
            // the server stores, so the row is handed over as those bytes.
            DataType::FixedSizeList(_, width) => {
                let lists = column.as_fixed_size_list();
                let floats = lists.value(row);
                let floats = floats.as_primitive::<arrow_array::types::Float32Type>();

                let mut raw = Vec::with_capacity(*width as usize * 4);
                for i in 0..floats.len() {
                    raw.extend_from_slice(&floats.value(i).to_le_bytes());
                }

                Value::Bytes(raw)
            }
            _ => Value::Bytes(column.as_binary::<i32>().value(row).to_vec()),
        },
        Target::Text => Value::Text(column.as_string::<i32>().value(row).to_string()),
        Target::Date => Value::Text(
            arrow_array::temporal_conversions::date32_to_datetime(
                column
                    .as_primitive::<arrow_array::types::Date32Type>()
                    .value(row),
            )
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default(),
        ),
        Target::DateTime => Value::Text(
            arrow_array::temporal_conversions::timestamp_ms_to_datetime(
                column
                    .as_primitive::<arrow_array::types::TimestampMillisecondType>()
                    .value(row),
            )
            .map(|d| d.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
            .unwrap_or_default(),
        ),
        Target::Time => Value::Text(
            arrow_array::temporal_conversions::time32ms_to_time(
                column
                    .as_primitive::<arrow_array::types::Time32MillisecondType>()
                    .value(row),
            )
            .map(|t| t.format("%H:%M:%S%.3f").to_string())
            .unwrap_or_default(),
        ),
        Target::Uuid => match column.data_type() {
            DataType::FixedSizeBinary(16) => {
                Value::Text(format_uuid(column.as_fixed_size_binary().value(row)))
            }
            _ => Value::Text(column.as_string::<i32>().value(row).to_string()),
        },
        Target::Geometry => match column.data_type() {
            // What the server sent on the way out, turned back into the text
            // the column is declared to hold. Bytes that do not read as WKB
            // are refused: written as an empty string they would fail the
            // batch they went in, or be stored as a geometry nobody gave.
            DataType::Binary => match wkb_to_wkt(column.as_binary::<i32>().value(row)) {
                Some(wkt) => Value::Text(wkt),
                None => {
                    return Err(AdbcError::with_message_and_status(
                        format!("row {row} holds a geometry that is not WKB this driver can read"),
                        Status::InvalidData,
                    ))
                }
            },
            _ => Value::Text(column.as_string::<i32>().value(row).to_string()),
        },
        Target::GeometryWkb => match column.data_type() {
            DataType::Binary => Value::Bytes(column.as_binary::<i32>().value(row).to_vec()),
            _ => match wkt_to_wkb(column.as_string::<i32>().value(row)) {
                Some(wkb) => Value::Bytes(wkb),
                None => {
                    return Err(AdbcError::with_message_and_status(
                        format!("row {row} holds a geometry that is not WKT this driver can read"),
                        Status::InvalidData,
                    ))
                }
            },
        },
    })
}

/// Converts the WKB the Arrow path sends into the WKT a text geometry column
/// stores, keeping a Z or M the WKB has.
/// @return The WKT, or None if the bytes are not a geometry
fn wkb_to_wkt(bytes: &[u8]) -> Option<String> {
    use geozero::ToWkt;

    let wkb = geozero::wkb::Wkb(bytes);

    wkb.to_wkt_ndim(dimensions_of(&wkb)?).ok()
}

/// Which dimensions a geometry's coordinates carry. The writers emit the ones
/// they are asked for, leaving out the rest and filling a missing Z or M with
/// zero, so they are asked for exactly these.
#[derive(Default)]
struct Dimensions {
    z: bool,
    m: bool,
}

impl geozero::GeomProcessor for Dimensions {
    /// Asks the reader for Z and M as well, which it otherwise leaves out.
    fn multi_dim(&self) -> bool {
        true
    }

    fn coordinate(
        &mut self,
        _x: f64,
        _y: f64,
        z: Option<f64>,
        m: Option<f64>,
        _t: Option<f64>,
        _tm: Option<u64>,
        _idx: usize,
    ) -> geozero::error::Result<()> {
        self.z |= z.is_some();
        self.m |= m.is_some();
        Ok(())
    }
}

/// @return The dimensions a geometry's coordinates carry, or None if it
/// cannot be read
fn dimensions_of(geometry: &impl geozero::GeozeroGeometry) -> Option<geozero::CoordDimensions> {
    let mut found = Dimensions::default();
    geometry.process_geom(&mut found).ok()?;

    Some(geozero::CoordDimensions {
        z: found.z,
        m: found.m,
        ..geozero::CoordDimensions::xy()
    })
}

/// Converts WKT into the WKB a binary geometry column stores, keeping a Z or M
/// the text has and adding none it lacks.
/// @return The WKB, or None if the text is not a geometry
fn wkt_to_wkb(text: &str) -> Option<Vec<u8>> {
    use geozero::ToWkb;

    let wkt = geozero::wkt::Wkt(text);

    wkt.to_wkb(dimensions_of(&wkt)?).ok()
}

/// @return The hyphenated form of sixteen UUID bytes
fn format_uuid(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// An inserter and the shape of the table it fills, kept together because
/// building either one costs the same look at the table.
struct Cached {
    inserter: Arc<BulkInserter<Record>>,
    schema: Arc<IngestSchema>,
    /// What it was built with. A connection that has since changed how its
    /// rows should be loaded gets one built again rather than the old one
    /// quietly going on as it was.
    built_for: Built,
    /// When this was last handed out, so the one idle longest goes first.
    used: u64,
}

/// What an inserter was built with, and what a later ingest is compared
/// against to decide whether the one held still does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Built {
    loading: Loading,
    update_on_existing_pk: bool,
    ignore_existing_pk: bool,
}

impl From<&Context> for Built {
    /// @return What a connection in this state would have an inserter built
    ///         with
    fn from(context: &Context) -> Self {
        Self {
            loading: context.loading.clone(),
            update_on_existing_pk: said(context, options::UPDATE_ON_EXISTING_PK),
            ignore_existing_pk: said(context, options::IGNORE_EXISTING_PK),
        }
    }
}

/// The two primary-key options are statement options as well, so they are
/// read from there rather than being said twice.
/// @return Whether the option was set to true
fn said(context: &Context, name: &str) -> bool {
    context.query.get(name).is_some_and(|value| value == "true")
}

/// How many tables a connection keeps an inserter for. Each holds a queue per
/// worker and a flush of its own, so they are not kept without limit; a
/// connection loading more tables than this is paying the setup again for the
/// one it comes back to least often.
const KEPT_INSERTERS: usize = 16;

/// Inserters kept on the connection between ingests. Building one reads the table, the shard map and the caller's
/// permissions, none of which change between ingests into the same table.
#[derive(Default)]
pub struct Inserters {
    built: Mutex<HashMap<String, Cached>>,
    /// Counts uses, so the least recent can be told from the rest.
    clock: std::sync::atomic::AtomicU64,
}

impl Inserters {
    /// The cache, taken back even after a panic while it was held. It is only
    /// ever changed a whole entry at a time, so whatever such a panic left is
    /// a cache that is at worst missing an entry, which the next ingest
    /// builds again. `close` runs from destructors, where unwrapping would
    /// turn one panic into two and abort the process.
    /// @return The cache, locked
    fn built(&self) -> MutexGuard<'_, HashMap<String, Cached>> {
        self.built.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// @return The inserter for a table, built on first use
    fn get(
        &self,
        session: &Session,
        table: &str,
        built_for: &Built,
    ) -> AdbcResult<(Arc<BulkInserter<Record>>, Arc<IngestSchema>)> {
        let now = self
            .clock
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut built = self.built();

        if let Some(cached) = built.get_mut(table) {
            if &cached.built_for == built_for {
                cached.used = now;

                return Ok((Arc::clone(&cached.inserter), Arc::clone(&cached.schema)));
            }

            // Built for something else. What it is holding still has to go,
            // and go before the replacement starts sending, or the rows would
            // arrive out of the order they were given in.
            if let Some(stale) = built.remove(table) {
                let _ = runtime::block_on(stale.inserter.close());
            }
        }

        // A compiled record type would carry its own schema; this one's shape
        // is the table's, so the table's schema is what the inserter encodes
        // with.
        let (mut schema, schema_text) = describe(session, table)?;

        let inserter: Arc<BulkInserter<Record>> = runtime::block_on(BulkInserter::with_schema(
            Arc::clone(session.client()),
            table.to_string(),
            built_for.loading.rows_per_insertion,
            NUM_WORKERS,
            Some(built_for.loading.inserter_options(
                built_for.update_on_existing_pk,
                built_for.ignore_existing_pk,
            )),
            &schema_text,
        ))
        .map_err(to_adbc)?;

        // Which columns route a record is only known once the inserter has read
        // the table's shard key, or its primary key when it has none, so they
        // are marked after it is built.
        let shard_columns = inserter.shard_column_names();

        for column in &mut schema.columns {
            column.is_shard_key = shard_columns.iter().any(|name| name == column.name);
        }

        let schema = Arc::new(schema);

        built.insert(
            table.to_string(),
            Cached {
                inserter: Arc::clone(&inserter),
                schema: Arc::clone(&schema),
                built_for: built_for.clone(),
                used: now,
            },
        );

        // An inserter handed out earlier goes on working: what is dropped here
        // is only this cache's hold on it.
        if built.len() > KEPT_INSERTERS {
            let coldest = built
                .iter()
                .min_by_key(|(_, cached)| cached.used)
                .map(|(name, _)| name.clone());

            if let Some(name) = coldest {
                if let Some(evicted) = built.remove(&name) {
                    let _ = runtime::block_on(evicted.inserter.close());
                }
            }
        }

        Ok((inserter, schema))
    }
}

impl Inserters {
    /// Sends what every inserter built here is holding and closes it, which
    /// also stops its timed flush before that outlives the connection that
    /// asked for it. The connection calls this before ending its session,
    /// since rows still queued may be bound for a table the session owns and
    /// a closed client cannot send them. Calling it again does nothing.
    pub(crate) fn close(&self) {
        for (_, cached) in self.built().drain() {
            let _ = runtime::block_on(cached.inserter.close());
        }
    }
}

impl Drop for Inserters {
    fn drop(&mut self) {
        self.close();
    }
}

/// How a stream's columns line up with the table's.
pub enum Mapping<'a> {
    /// By name, which is what bulk ingest into a named table means.
    ByName,
    /// By the names a statement gave them, in the order it gave them.
    Named(&'a [String]),
    /// In order, which is what an insert that names no columns means. The
    /// stream's own names say nothing then: bound parameters are called after
    /// their position.
    InOrder,
}

/// Sends every row of the stream to the target table through the workers.
/// @return The number of rows inserted
pub(crate) fn ingest(
    session: Arc<Session>,
    table: &str,
    columns: Mapping<'_>,
    built_for: &Built,
    mut reader: Box<dyn RecordBatchReader + Send>,
) -> AdbcResult<i64> {
    let (inserter, schema) = session.inserters().get(&session, table, built_for)?;

    let mut queued = 0;
    let streamed = stream(&inserter, &schema, &columns, &mut reader, &mut queued);

    // What was queued is sent and accounted for here even when the stream
    // failed part-way. Left on the inserter, which the connection keeps, it
    // would go out with the next ingest into this table, and its failures
    // would be reported there.
    let settled = settle(&inserter);

    // Counted from what is drained: the inserter's own count reads the queue
    // this empties.
    let errors = inserter.drain_errors();
    let failed: usize = errors.iter().map(|error| error.record_count).sum();

    if let Err(mut error) = streamed {
        // Kinetica has no transaction to undo them in, so the rows ahead of
        // the failure are inserted, and the caller is told how many.
        if queued > 0 && settled.is_ok() {
            error.message = match failed {
                0 => format!("{} ({queued} rows before it were inserted)", error.message),
                _ => format!(
                    "{} ({queued} rows before it were sent, of which {failed} failed)",
                    error.message
                ),
            };
        }

        return Err(error);
    }

    settled?;

    if let Some(first) = errors.first() {
        return Err(to_adbc(KineticaError::Config(format!(
            "{failed} of {queued} rows failed to insert: {first:?}"
        ))));
    }

    Ok(queued)
}

/// Converts the stream a batch at a time and hands its rows to the inserter,
/// stopping at the first failure.
/// @return An error if a batch could not be read, converted or queued
fn stream(
    inserter: &Arc<BulkInserter<Record>>,
    schema: &Arc<IngestSchema>,
    columns: &Mapping<'_>,
    reader: &mut Box<dyn RecordBatchReader + Send>,
    queued: &mut i64,
) -> AdbcResult<()> {
    for batch in reader {
        let batch = batch.map_err(from_arrow)?;
        append(inserter, schema, columns, &batch, queued)?;
    }

    Ok(())
}

/// Sends what is buffered and waits for it to land, which is what makes the
/// rows visible to the query a caller runs next. Closing would wait as well,
/// but also retire an inserter worth keeping.
/// @return An error if the wait could not be made
fn settle(inserter: &Arc<BulkInserter<Record>>) -> AdbcResult<()> {
    runtime::block_on(inserter.flush()).map_err(to_adbc)
}

/// Converts one batch and hands its rows to the inserter, counting each one
/// queued, so a failure part-way through still says how far it got.
/// @return An error if a row could not be converted or queued
fn append(
    inserter: &Arc<BulkInserter<Record>>,
    schema: &Arc<IngestSchema>,
    names: &Mapping<'_>,
    batch: &RecordBatch,
    queued: &mut i64,
) -> AdbcResult<()> {
    let data = align(schema, names, batch)?;

    for row in 0..batch.num_rows() {
        let values = schema
            .columns
            .iter()
            .zip(data.iter())
            .map(|(column, found)| match found {
                Some(array) => value_at(array, column.target, row).map_err(|mut error| {
                    error.message = format!("column '{}': {}", column.name, error.message);
                    error
                }),
                None => Ok(Value::Null),
            })
            .collect::<AdbcResult<Vec<_>>>()?;

        inserter
            .insert(Arc::new(Record {
                schema: Arc::clone(schema),
                values,
            }))
            .map_err(|refused| to_adbc(refused.into()))?;

        *queued += 1;
    }

    Ok(())
}

/// Lines the batch's columns up with the table's, by the names a statement
/// gave them or, failing that, by the names the stream carries. A table column
/// the stream says nothing about is left null.
/// @return One array per table column, in the table's order
fn align(
    schema: &IngestSchema,
    names: &Mapping<'_>,
    batch: &RecordBatch,
) -> AdbcResult<Vec<Option<ArrayRef>>> {
    // An insert that names no columns fills the table's own, in its order, and
    // has nothing to match by name.
    if let Mapping::InOrder = names {
        if batch.num_columns() > schema.columns.len() {
            return Err(unsupported(&format!(
                "a stream of {} columns for a table of {}",
                batch.num_columns(),
                schema.columns.len()
            )));
        }

        return schema
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| match batch.columns().get(index) {
                Some(found) => coerce(found, column.target).map(Some),
                None => Ok(None),
            })
            .collect();
    }

    if let Mapping::Named(names) = names {
        if names.len() != batch.num_columns() {
            return Err(unsupported(&format!(
                "a stream of {} columns for a statement naming {}",
                batch.num_columns(),
                names.len()
            )));
        }
    }

    let stream: Vec<&str> = match names {
        Mapping::Named(names) => names.iter().map(String::as_str).collect(),
        _ => batch
            .schema_ref()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect(),
    };

    // A stream column the table has no place for would otherwise be dropped
    // without a word, so it is reported instead.
    for name in &stream {
        if !schema.columns.iter().any(|column| column.name == *name) {
            return Err(incompatible(&format!(
                "the table has no column '{name}' for the data to go into"
            )));
        }
    }

    schema
        .columns
        .iter()
        .map(
            |column| match stream.iter().position(|name| *name == column.name) {
                Some(index) => coerce(batch.column(index), column.target).map(Some),
                None => Ok(None),
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// POINT(1 2), little-endian WKB.
    const POINT: [u8; 21] = [
        1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
    ];

    #[test]
    fn wkb_for_a_text_geometry_column_is_written_as_wkt() {
        let column: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![&POINT[..]]));

        match value_at(&column, Target::Geometry, 0) {
            Ok(Value::Text(wkt)) => assert_eq!(wkt, "POINT(1 2)"),
            _ => panic!("a point should read back as its WKT"),
        }
    }

    #[test]
    fn bytes_that_are_not_wkb_are_refused_rather_than_written_empty() {
        let column: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![&b"not wkb"[..]]));

        match value_at(&column, Target::Geometry, 0) {
            Err(refused) => assert_eq!(refused.status, Status::InvalidData),
            Ok(_) => panic!("unreadable bytes should be refused"),
        }
    }

    /// Converts row 0 of a column the way an ingest does.
    fn through(column: ArrayRef, target: Target) -> AdbcResult<Value> {
        value_at(&coerce(&column, target)?, target, 0)
    }

    #[test]
    fn wkb_in_any_binary_layout_reaches_a_text_geometry_column_as_wkt() {
        let layouts: [ArrayRef; 3] = [
            Arc::new(arrow_array::BinaryArray::from(vec![&POINT[..]])),
            Arc::new(arrow_array::LargeBinaryArray::from(vec![&POINT[..]])),
            Arc::new(arrow_array::BinaryViewArray::from_iter_values([&POINT[..]])),
        ];

        for column in layouts {
            let layout = column.data_type().clone();

            match through(column, Target::Geometry) {
                Ok(Value::Text(wkt)) => assert_eq!(wkt, "POINT(1 2)", "{layout}"),
                _ => panic!("{layout} should read back as WKT"),
            }
        }
    }

    #[test]
    fn wkt_in_any_string_layout_reaches_a_binary_geometry_column_as_wkb() {
        let layouts: [ArrayRef; 3] = [
            Arc::new(arrow_array::StringArray::from(vec!["POINT(1 2)"])),
            Arc::new(arrow_array::LargeStringArray::from(vec!["POINT(1 2)"])),
            Arc::new(arrow_array::StringViewArray::from_iter_values([
                "POINT(1 2)",
            ])),
        ];

        for column in layouts {
            let layout = column.data_type().clone();

            match through(column, Target::GeometryWkb) {
                Ok(Value::Bytes(wkb)) => assert_eq!(wkb, POINT, "{layout}"),
                _ => panic!("{layout} should be written as WKB"),
            }
        }
    }

    #[test]
    fn wkb_reaches_a_binary_geometry_column_unchanged() {
        let column: ArrayRef = Arc::new(arrow_array::LargeBinaryArray::from(vec![&POINT[..]]));

        match through(column, Target::GeometryWkb) {
            Ok(Value::Bytes(wkb)) => assert_eq!(wkb, POINT),
            _ => panic!("WKB should be written as it came"),
        }
    }

    #[test]
    fn a_third_dimension_survives_the_conversion_to_wkb() {
        let wkb = wkt_to_wkb("POINT Z (1 2 3)").expect("a 3-D point");

        // Byte order, type, then three doubles: Z is the last eight bytes.
        assert_eq!(wkb.len(), 1 + 4 + 3 * 8);
        assert_eq!(f64::from_le_bytes(wkb[21..29].try_into().unwrap()), 3.0);
    }

    #[test]
    fn a_third_dimension_survives_the_conversion_to_wkt() {
        let wkb = wkt_to_wkb("POINT Z (1 2 3)").expect("a 3-D point");

        // The implicit form, which Kinetica reads as 3-D as it does
        // "POINT Z (1 2 3)", and prints back as the latter.
        assert_eq!(wkb_to_wkt(&wkb).as_deref(), Some("POINT(1 2 3)"));
        assert_eq!(wkb_to_wkt(&POINT).as_deref(), Some("POINT(1 2)"));
    }

    #[test]
    fn text_that_is_not_wkt_is_refused() {
        let column: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["not a shape"]));

        match through(column, Target::GeometryWkb) {
            Err(refused) => assert_eq!(refused.status, Status::InvalidData),
            Ok(_) => panic!("unreadable text should be refused"),
        }
    }

    #[test]
    fn a_geometry_column_is_told_apart_from_text_and_bytes_by_its_base_type() {
        let wkt = vec!["wkt".to_string()];

        let target = |base: &str, properties: &[String]| {
            target_of(Some(&serde_json::Value::String(base.into())), properties)
        };

        assert!(target("string", &wkt) == Target::Geometry);
        assert!(target("bytes", &wkt) == Target::GeometryWkb);
        assert!(target("bytes", &[]) == Target::Bytes);
        assert!(target("string", &[]) == Target::Text);
    }

    #[test]
    fn a_timestamp_reaches_a_timestamp_column_in_milliseconds_whatever_its_unit() {
        use arrow_array::{
            TimestampMicrosecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };

        let target = target_of(
            Some(&serde_json::Value::String("long".into())),
            &["timestamp".to_string()],
        );
        assert!(target == Target::Timestamp);

        // 2023-05-15 13:45:30.123 UTC, in each unit Arrow has
        let ms = 1_684_158_330_123;
        let units: [ArrayRef; 3] = [
            Arc::new(TimestampSecondArray::from(vec![ms / 1_000]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![ms * 1_000 + 456]).with_timezone("UTC")),
            Arc::new(TimestampNanosecondArray::from(vec![ms * 1_000_000]).with_timezone("UTC")),
        ];
        let expected = [ms / 1_000 * 1_000, ms, ms];

        for (column, expected) in units.into_iter().zip(expected) {
            match through(column, target) {
                Ok(Value::Long(stored)) => assert_eq!(stored, expected),
                Ok(_) => panic!("a timestamp should be written as a long"),
                Err(refused) => panic!("{}", refused.message),
            }
        }
    }

    #[test]
    fn a_zoned_timestamp_keeps_its_instant_and_a_long_is_taken_as_milliseconds() {
        use arrow_array::{Int64Array, TimestampSecondArray};

        // The same instant, given in Tokyo's zone: Arrow holds it as UTC
        let tokyo: ArrayRef =
            Arc::new(TimestampSecondArray::from(vec![1_684_158_330]).with_timezone("Asia/Tokyo"));
        match through(tokyo, Target::Timestamp) {
            Ok(Value::Long(stored)) => assert_eq!(stored, 1_684_158_330_000),
            Ok(_) => panic!("a timestamp should be written as a long"),
            Err(refused) => panic!("{}", refused.message),
        }

        let plain: ArrayRef = Arc::new(Int64Array::from(vec![1_684_158_330_123]));
        match through(plain, Target::Timestamp) {
            Ok(Value::Long(stored)) => assert_eq!(stored, 1_684_158_330_123),
            Ok(_) => panic!("a timestamp should be written as a long"),
            Err(refused) => panic!("{}", refused.message),
        }
    }

    #[test]
    fn closing_after_a_panic_with_the_cache_held_does_not_panic_again() {
        let inserters = Inserters::default();

        // A panic while the cache is held leaves its lock poisoned.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = inserters.built.lock().unwrap();
            panic!("while the cache was held");
        }));
        assert!(inserters.built.is_poisoned());

        // What a connection's destructor calls, which must not panic.
        let closed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inserters.close()));
        assert!(closed.is_ok(), "close panicked on a poisoned cache");
    }
}
