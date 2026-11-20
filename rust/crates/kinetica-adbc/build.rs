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

//! Records the version the driver reports of itself, as the Rust API records
//! its own.
//!
//! The release version is the crate's own SemVer version, `MAJOR.MINOR.PATCH`,
//! as the Rust API's is: `MAJOR.MINOR` is the Kinetica version the driver works
//! with, and `PATCH` counts the driver's own releases for it. `get_info`
//! reports it alone; the client's log, when a connection opens, adds the
//! build number as SemVer build metadata, `7.2.0+<build>`.
//!
//! The build number, `yyyyMMddHHmmss`, is the time of the build in UTC, as the
//! Rust API takes it, whoever builds the driver.
//!
//! The time is taken when this script runs, which is when the crate is first
//! built and again only when `Cargo.toml` changes, so it can be older than the
//! code it is reported with.
//!
//! The Arrow version `get_info` reports is the one the workspace's lock file
//! resolved, since the manifest accepts a range.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let version = release_version();
    let build = build_number();

    println!("cargo:rustc-env=KINETICA_ADBC_VERSION={version}");
    println!("cargo:rustc-env=KINETICA_ADBC_DRIVER_VERSION={version}+{build}");
    println!(
        "cargo:rustc-env=KINETICA_ADBC_ARROW_VERSION={}",
        arrow_version()
    );
}

/// The Arrow version the build resolved, as ADBC reports it: `v59.3.0`.
///
/// Read from the workspace's lock file, two directories up both here and in
/// the published layout. Empty where there is none to read.
fn arrow_version() -> String {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let lock = Path::new(&manifest_dir).join("../../Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock.display());

    let Ok(text) = std::fs::read_to_string(&lock) else {
        return String::new();
    };

    // The version line follows the name line in each [[package]] entry
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() == r#"name = "arrow-array""# {
            if let Some(version) = lines
                .next()
                .and_then(|next| next.trim().strip_prefix("version = "))
            {
                return format!("v{}", version.trim_matches('"'));
            }
        }
    }

    String::new()
}

/// The release version, the crate's `MAJOR.MINOR.PATCH`.
///
/// Panics if the crate version is a pre-release (`7.2.3-0`): releases are
/// plain SemVer, so one would be a manifest written in another scheme.
fn release_version() -> String {
    let part = |name: &str| std::env::var(format!("CARGO_PKG_VERSION_{name}")).unwrap_or_default();

    if !part("PRE").is_empty() {
        panic!(
            "the crate version {} is a pre-release; release versions are MAJOR.MINOR.PATCH",
            std::env::var("CARGO_PKG_VERSION").unwrap_or_default()
        );
    }

    format!("{}.{}.{}", part("MAJOR"), part("MINOR"), part("PATCH"))
}

/// The build number: the time of the build, `yyyyMMddHHmmss`, in UTC.
fn build_number() -> String {
    // Stamped once per build of the crate, not on every build of whatever
    // depends on it: the manifest changes when the version does.
    println!("cargo:rerun-if-changed=Cargo.toml");

    build_timestamp()
}

/// The time of the build, `yyyyMMddHHmmss`, in UTC.
fn build_timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs()) as i64;
    let (days, time) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}{:02}{:02}{:02}",
        time / 3600,
        time % 3600 / 60,
        time % 60
    )
}
