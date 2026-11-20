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

//! Partitioned execution, which hands a consumer the pieces of one result so
//! it can read them at once and from wherever it likes.
//!
//! Kinetica has no partition handles, so a piece is a range of the same query:
//! the descriptor carries everything needed to run it again, which is what
//! lets another connection, or another process, read it.
//!
//! The ranges are taken against the result as it stood when the query was
//! first run. A table changing underneath them shifts the offsets, so rows can
//! be read twice or missed, exactly as they can when paging through the result
//! in one reader.

use adbc_core::error::Result as AdbcResult;

use crate::error::unsupported;

/// One piece of a result, as the bytes ADBC passes back to `read_partition`.
pub struct Partition {
    pub query: String,
    pub offset: i64,
    pub limit: i64,
    pub parameters: Option<String>,
    /// The result every piece reads from, so they agree with each other and
    /// the query is not run again for each. Empty when the server made none.
    pub paging_table: Option<String>,
    /// How many rows that result held, which is how a reader tells the result
    /// it was cut from from one the server made again in its place.
    pub total: i64,
}

/// What this build writes into a descriptor and will accept back.
///
/// A descriptor is made to be carried to another process, which may be running
/// a different build of this driver, so the shape it was written in is stated
/// rather than assumed. A reader that does not know a version says so instead
/// of reading the fields it happens to recognize.
const DESCRIPTOR_VERSION: i64 = 1;

impl Partition {
    /// @return The descriptor to hand the consumer
    pub fn encode(&self) -> Vec<u8> {
        let mut value = serde_json::json!({
            "version": DESCRIPTOR_VERSION,
            "query": self.query,
            "offset": self.offset,
            "limit": self.limit,
            "total": self.total,
        });

        if let Some(parameters) = &self.parameters {
            value["parameters"] = serde_json::Value::String(parameters.clone());
        }

        if let Some(table) = &self.paging_table {
            value["paging_table"] = serde_json::Value::String(table.clone());
        }

        value.to_string().into_bytes()
    }

    /// @return The partition a descriptor describes
    pub fn decode(bytes: &[u8]) -> AdbcResult<Self> {
        let malformed = || unsupported("a partition descriptor this driver did not write");

        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| malformed())?;

        match value.get("version").and_then(|v| v.as_i64()) {
            Some(DESCRIPTOR_VERSION) => {}
            Some(other) => {
                return Err(unsupported(&format!(
                    "a partition descriptor of version {other}, where this driver writes {DESCRIPTOR_VERSION}"
                )))
            }
            None => return Err(malformed()),
        }

        Ok(Self {
            query: value
                .get("query")
                .and_then(|v| v.as_str())
                .ok_or_else(malformed)?
                .to_string(),
            offset: value
                .get("offset")
                .and_then(|v| v.as_i64())
                .ok_or_else(malformed)?,
            limit: value
                .get("limit")
                .and_then(|v| v.as_i64())
                .ok_or_else(malformed)?,
            parameters: value
                .get("parameters")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            paging_table: value
                .get("paging_table")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            total: value
                .get("total")
                .and_then(|v| v.as_i64())
                .ok_or_else(malformed)?,
        })
    }
}

/// Slices a result of the given size into pieces one request can each answer.
/// @return The descriptors, one per piece
pub fn slice(
    query: &str,
    parameters: Option<String>,
    paging_table: Option<String>,
    total: i64,
    rows: i64,
) -> Vec<Vec<u8>> {
    let rows = rows.max(1);

    // An empty result still gets a partition, so that reading them all back
    // gives the same rows as executing the query would.
    let pieces = if total <= 0 {
        1
    } else {
        (total + rows - 1) / rows
    };

    (0..pieces)
        .map(|piece| {
            Partition {
                query: query.to_string(),
                offset: piece * rows,
                limit: rows,
                parameters: parameters.clone(),
                paging_table: paging_table.clone(),
                total,
            }
            .encode()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_descriptor_survives_the_round_trip() {
        let partition = Partition {
            query: "SELECT 1".into(),
            offset: 40,
            limit: 20,
            parameters: Some("[1]".into()),
            paging_table: Some("sys_sql_temp.t".into()),
            total: 55,
        };

        let decoded = Partition::decode(&partition.encode()).expect("decode");

        assert_eq!(decoded.query, "SELECT 1");
        assert_eq!(decoded.offset, 40);
        assert_eq!(decoded.limit, 20);
        assert_eq!(decoded.parameters.as_deref(), Some("[1]"));
        assert_eq!(decoded.paging_table.as_deref(), Some("sys_sql_temp.t"));
        assert_eq!(decoded.total, 55);
    }

    #[test]
    fn slicing_covers_every_row_exactly_once() {
        let pieces: Vec<_> = slice("SELECT 1", None, None, 50, 20)
            .iter()
            .map(|p| Partition::decode(p).expect("decode"))
            .collect();

        assert_eq!(pieces.len(), 3);
        assert_eq!(
            pieces.iter().map(|p| p.offset).collect::<Vec<_>>(),
            vec![0, 20, 40]
        );
    }

    #[test]
    fn an_empty_result_still_has_one_piece() {
        assert_eq!(slice("SELECT 1", None, None, 0, 20).len(), 1);
    }

    #[test]
    fn rubbish_is_not_a_descriptor() {
        assert!(Partition::decode(b"not json").is_err());
        assert!(Partition::decode(br#"{"query":"SELECT 1"}"#).is_err());
        assert!(
            Partition::decode(br#"{"version":1,"query":"SELECT 1","offset":0,"limit":20}"#)
                .is_err(),
            "a descriptor without the total its reader checks against"
        );
    }

    /// A descriptor written by another build is refused rather than read for
    /// the fields this one happens to know.
    #[test]
    fn a_descriptor_of_another_version_is_refused() {
        let ahead = br#"{"version":2,"query":"SELECT 1","offset":0,"limit":10,"total":10}"#;
        let behind = br#"{"query":"SELECT 1","offset":0,"limit":10}"#;

        let refused = match Partition::decode(ahead) {
            Err(refused) => refused,
            Ok(_) => panic!("a version this build does not write should be refused"),
        };
        assert!(refused.to_string().contains('2'), "{refused}");

        assert!(
            Partition::decode(behind).is_err(),
            "a descriptor with no version at all"
        );
    }
}
