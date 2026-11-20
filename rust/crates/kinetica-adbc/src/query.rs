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

//! The options the server takes on a statement: which optimizations to run,
//! what to do about a primary key that is already there, how long what the
//! statement leaves behind should live.
//!
//! They go on every `/execute/sql` a connection sends, and are settable on the
//! database or on a connection already open. Only what a caller has actually
//! asked for is sent, so a statement nobody has configured looks exactly as
//! it did before.

use std::collections::{BTreeMap, HashMap};

use adbc_core::error::Result as AdbcResult;

use crate::error::invalid;
use crate::options;

/// The flags, as the ADBC option a caller sets and the option the server
/// reads, which is the ADBC option's name without its prefix.
const FLAGS: [(&str, &str); 10] = [
    (
        "adbc.kinetica.cost_based_optimization",
        "cost_based_optimization",
    ),
    (
        "adbc.kinetica.distributed_operations",
        "distributed_operations",
    ),
    ("adbc.kinetica.ignore_existing_pk", "ignore_existing_pk"),
    (
        "adbc.kinetica.update_on_existing_pk",
        "update_on_existing_pk",
    ),
    ("adbc.kinetica.parallel_execution", "parallel_execution"),
    ("adbc.kinetica.plan_cache", "plan_cache"),
    ("adbc.kinetica.results_caching", "results_caching"),
    (
        "adbc.kinetica.rule_based_optimization",
        "rule_based_optimization",
    ),
    ("adbc.kinetica.ssq_optimization", "ssq_optimization"),
    (
        "adbc.kinetica.validate_change_column",
        "validate_change_column",
    ),
];

/// The counts, named the same way.
const NUMBERS: [(&str, &str); 2] = [
    (options::TTL, "ttl"),
    (options::PAGING_TABLE_TTL, "paging_table_ttl"),
];

/// Whether to count distinct values approximately. Apart from the others
/// because the server marks it internal, so it is spelled out here rather
/// than taken from a generated constant.
const APPROX_COUNT_DISTINCT: (&str, &str) = (
    options::USE_APPROX_COUNT_DISTINCT,
    "use_approx_count_distinct",
);

/// What a caller has asked the server to do with its statements, held by the
/// server's own option names so that sending them is a copy.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    said: BTreeMap<&'static str, String>,
}

impl QueryOptions {
    /// Takes the option if it is one of these.
    /// @return Whether the name was one of these options
    pub fn set(&mut self, name: &str, value: &str) -> AdbcResult<bool> {
        if let Some(server_name) = flag_named(name) {
            self.said.insert(server_name, flag(name, value)?);
            return Ok(true);
        }

        if let Some(server_name) = number_named(name) {
            self.said.insert(server_name, number(name, value)?);
            return Ok(true);
        }

        Ok(false)
    }

    /// @return What was set, or None if the name is not one of these options
    pub fn get(&self, name: &str) -> Option<String> {
        let server_name = flag_named(name).or_else(|| number_named(name))?;

        self.said.get(server_name).cloned()
    }

    /// Puts what was asked for onto a statement about to be sent. Applied
    /// after the driver's own options, so that a caller naming a paging table
    /// TTL gets the one it named rather than the default.
    pub fn apply(&self, options: &mut HashMap<String, String>) {
        for (name, value) in &self.said {
            options.insert(name.to_string(), value.clone());
        }
    }
}

/// @return The server's name for the flag, if that is what this is
fn flag_named(name: &str) -> Option<&'static str> {
    if name == APPROX_COUNT_DISTINCT.0 {
        return Some(APPROX_COUNT_DISTINCT.1);
    }

    FLAGS
        .iter()
        .find(|(option, _)| *option == name)
        .map(|(_, server)| *server)
}

/// @return The server's name for the count, if that is what this is
fn number_named(name: &str) -> Option<&'static str> {
    NUMBERS
        .iter()
        .find(|(option, _)| *option == name)
        .map(|(_, server)| *server)
}

/// @return The flag as the server spells one
fn flag(name: &str, value: &str) -> AdbcResult<String> {
    match value {
        _ if value.eq_ignore_ascii_case("true") || value == "1" => Ok("true".to_string()),
        _ if value.eq_ignore_ascii_case("false") || value == "0" => Ok("false".to_string()),
        other => Err(invalid(&format!(
            "{name} takes true or false, not '{other}'"
        ))),
    }
}

/// @return The count, which the server also reads as text
fn number(name: &str, value: &str) -> AdbcResult<String> {
    value
        .parse::<i64>()
        .map(|number| number.to_string())
        .map_err(|_| invalid(&format!("{name} takes a whole number, not '{value}'")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_is_sent_for_a_statement_nobody_configured() {
        let mut options = HashMap::new();
        QueryOptions::default().apply(&mut options);

        assert!(options.is_empty());
    }

    #[test]
    fn a_flag_is_sent_under_the_name_the_server_reads() {
        let mut query = QueryOptions::default();
        query.set(options::PLAN_CACHE, "false").unwrap();

        let mut options = HashMap::new();
        query.apply(&mut options);

        assert_eq!(options.get("plan_cache").unwrap(), "false");
    }

    #[test]
    fn options_are_sent_under_the_servers_own_names() {
        let mut query = QueryOptions::default();
        query.set(options::DISTRIBUTED_OPERATIONS, "true").unwrap();
        query
            .set(options::USE_APPROX_COUNT_DISTINCT, "true")
            .unwrap();

        let mut options = HashMap::new();
        query.apply(&mut options);

        assert_eq!(options.get("distributed_operations").unwrap(), "true");
        assert_eq!(options.get("use_approx_count_distinct").unwrap(), "true");
        // The server's deprecated name for the same switch is never sent
        assert!(!options.contains_key("distributed_joins"));
    }

    #[test]
    fn a_count_is_read_back_as_it_was_given() {
        let mut query = QueryOptions::default();
        query.set(options::TTL, "30").unwrap();

        assert_eq!(query.get(options::TTL).unwrap(), "30");
    }

    #[test]
    fn a_value_the_server_would_not_understand_is_refused_here() {
        let mut query = QueryOptions::default();

        assert!(query.set(options::PLAN_CACHE, "sometimes").is_err());
        assert!(query.set(options::TTL, "forever").is_err());
    }

    #[test]
    fn a_name_that_is_not_one_of_these_is_left_to_the_caller() {
        let mut query = QueryOptions::default();

        assert!(!query.set("adbc.kinetica.batch_size", "10").unwrap());
        assert!(query.get("adbc.kinetica.batch_size").is_none());
    }
}
