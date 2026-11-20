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

//! Where the client's log goes, and how much of it.
//!
//! The client logs through the `log` facade, which does nothing at all until
//! something is installed behind it. A driver loaded over the C ABI has no
//! main to install one in, so the first caller to ask for a log installs
//! this; a caller that never asks pays nothing and nothing is printed, which
//! is what a library should do.
//!
//! One logger for the process, since that is what the facade allows. Later
//! callers move its level and its file rather than installing another, and a
//! host that has already installed a logger of its own keeps it: this backs
//! off rather than fighting for a global.

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use adbc_core::error::Result as AdbcResult;

use crate::error::invalid;

/// The logger behind the facade, once a caller has asked for one.
static INSTALLED: OnceLock<&'static Logger> = OnceLock::new();

/// Writes what the client logs, wherever it was last told to.
struct Logger {
    /// The level as `log::LevelFilter` orders them, so it can be read without
    /// taking the lock on every message the client decides not to write.
    level: AtomicUsize,
    /// The file to append to, or None to write to standard error.
    file: Mutex<Option<std::fs::File>>,
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() as usize <= self.level.load(Ordering::Relaxed)
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let line = format!(
            "{} {:<5} {} {}\n",
            now(),
            record.level(),
            record.target(),
            record.args()
        );

        // A log that cannot be written is not worth failing a query over, so
        // the error goes nowhere: there is, by definition, nowhere to put it.
        match self.file.lock().unwrap().as_mut() {
            Some(file) => {
                let _ = file.write_all(line.as_bytes());
            }
            None => {
                let _ = std::io::stderr().write_all(line.as_bytes());
            }
        }
    }

    fn flush(&self) {
        if let Some(file) = self.file.lock().unwrap().as_mut() {
            let _ = file.flush();
        }
    }
}

/// Seconds since the epoch, with milliseconds. Enough to line a driver log up
/// against a server log without a date library for the sake of one line.
/// @return The time, as the log carries it
fn now() -> String {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    format!("{}.{:03}", since.as_secs(), since.subsec_millis())
}

/// @return The logger, installing it behind the facade the first time
fn logger() -> &'static Logger {
    INSTALLED.get_or_init(|| {
        let logger: &'static Logger = Box::leak(Box::new(Logger {
            level: AtomicUsize::new(log::LevelFilter::Off as usize),
            file: Mutex::new(None),
        }));

        // A host that got there first keeps its own logger; what this driver
        // was asked for then applies to nothing, which is better than taking
        // the log away from the program that owns it.
        if log::set_logger(logger).is_ok() {
            log::set_max_level(log::LevelFilter::Trace);
        }

        logger
    })
}

/// Sets how much is logged.
/// @return An error if the text names no level
pub fn set_level(value: &str) -> AdbcResult<()> {
    let level = match value {
        _ if value.eq_ignore_ascii_case("off") => log::LevelFilter::Off,
        _ if value.eq_ignore_ascii_case("error") => log::LevelFilter::Error,
        _ if value.eq_ignore_ascii_case("warn") => log::LevelFilter::Warn,
        _ if value.eq_ignore_ascii_case("info") => log::LevelFilter::Info,
        _ if value.eq_ignore_ascii_case("debug") => log::LevelFilter::Debug,
        _ if value.eq_ignore_ascii_case("trace") => log::LevelFilter::Trace,
        other => {
            return Err(invalid(&format!(
                "{} takes off, error, warn, info, debug or trace, not '{other}'",
                crate::options::LOG_LEVEL
            )))
        }
    };

    logger().level.store(level as usize, Ordering::Relaxed);

    Ok(())
}

/// @return How much is being logged
pub fn level() -> String {
    let level = logger().level.load(Ordering::Relaxed);

    match level {
        _ if level == log::LevelFilter::Error as usize => "error",
        _ if level == log::LevelFilter::Warn as usize => "warn",
        _ if level == log::LevelFilter::Info as usize => "info",
        _ if level == log::LevelFilter::Debug as usize => "debug",
        _ if level == log::LevelFilter::Trace as usize => "trace",
        _ => "off",
    }
    .to_string()
}

/// Sends the log to a file, appending to one that is already there. An empty
/// path sends it back to standard error.
/// @return An error if the file cannot be opened, which is said now rather
///         than being discovered as silence later
pub fn set_path(path: &str) -> AdbcResult<()> {
    let opened = match path.is_empty() {
        true => None,
        false => Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|error| invalid(&format!("cannot write the log to '{path}': {error}")))?,
        ),
    };

    *logger().file.lock().unwrap() = opened;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// There is one logger for the process, so these tests take turns with
    /// it rather than moving its level out from under each other.
    fn in_turn() -> std::sync::MutexGuard<'static, ()> {
        static ORDER: Mutex<()> = Mutex::new(());

        ORDER.lock().unwrap_or_else(|held| held.into_inner())
    }

    #[test]
    fn a_level_outside_the_six_is_refused() {
        let _turn = in_turn();
        let error = set_level("verbose").unwrap_err();

        assert!(
            error.message.contains("off, error, warn"),
            "{}",
            error.message
        );
    }

    #[test]
    fn the_level_is_read_back_as_it_was_set() {
        let _turn = in_turn();
        set_level("DEBUG").unwrap();
        assert_eq!(level(), "debug");

        set_level("off").unwrap();
        assert_eq!(level(), "off");
    }

    #[test]
    fn a_file_that_cannot_be_opened_is_said_so_at_once() {
        let _turn = in_turn();
        let error = set_path("/does/not/exist/kinetica.log").unwrap_err();

        assert!(
            error.message.contains("cannot write the log"),
            "{}",
            error.message
        );
    }

    #[test]
    fn a_log_written_to_a_file_is_in_the_file() {
        let _turn = in_turn();
        let path = std::env::temp_dir().join(format!("kinetica-adbc-{}.log", std::process::id()));

        set_path(path.to_str().unwrap()).unwrap();
        set_level("info").unwrap();
        log::info!("a line the test wrote");
        log::logger().flush();

        let written = std::fs::read_to_string(&path).unwrap_or_default();

        set_level("off").unwrap();
        set_path("").unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(
            written.contains("a line the test wrote"),
            "the log should hold what was written to it, found {written:?}"
        );
    }
}
