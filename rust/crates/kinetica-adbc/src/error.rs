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

//! Error mapping from Kinetica and Arrow failures to ADBC statuses.
//!
//! ADBC consumers branch on the status, and increasingly on the SQLSTATE, so
//! both are set in one place.

use adbc_core::error::{Error as AdbcError, Status};
use arrow_schema::ArrowError;
use gpudb_api::KineticaError;

/// @return An ADBC error saying the driver does not implement something
pub fn unsupported(what: &str) -> AdbcError {
    AdbcError::with_message_and_status(
        readable(&format!("{what} is not supported by the Kinetica driver")),
        Status::NotImplemented,
    )
}

/// @return An ADBC error saying an option has no value to read: one the driver
///         doesn't know, or one nobody has set. Setting an unknown option is
///         `unsupported` instead, as ADBC has it.
pub fn not_found(what: &str) -> AdbcError {
    AdbcError::with_message_and_status(
        readable(&format!(
            "{what} is not set, or not an option the Kinetica driver knows"
        )),
        Status::NotFound,
    )
}

/// @return An ADBC error stating this server can't support ADBC requests
pub fn unsupported_server(message: &str) -> AdbcError {
    let mut adbc = AdbcError::with_message_and_status(readable(message), Status::NotImplemented);
    // SQL-client unable to establish SQL-connection.
    adbc.sqlstate = to_sqlstate("08001");
    adbc
}

/// @return An ADBC error saying a table that exists cannot take what was given
///         to it, which is how ADBC reports an incompatible target
pub fn incompatible(what: &str) -> AdbcError {
    AdbcError::with_message_and_status(readable(what), Status::AlreadyExists)
}

/// @return An ADBC error saying something the caller named does not exist
pub fn missing(what: &str) -> AdbcError {
    AdbcError::with_message_and_status(readable(what), Status::NotFound)
}

/// @return An ADBC error saying a call came before what it needs was given
pub fn not_ready(what: &str) -> AdbcError {
    AdbcError::with_message_and_status(readable(what), Status::InvalidState)
}

/// @return An ADBC error saying a value the caller gave cannot be used
pub fn invalid(what: &str) -> AdbcError {
    AdbcError::with_message_and_status(readable(what), Status::InvalidArguments)
}

/// The server answers a rejected request with an Avro-framed body, and the
/// client puts it in the error whole, control bytes and all. A message reaches
/// the caller over the C ABI as a C string, where a NUL ends it early and the
/// binding refuses it outright, so the framing comes out first.
/// @return The message with nothing in it that cannot be carried
fn readable(message: &str) -> String {
    let cleaned: String = message
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();

    cleaned.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Maps a client failure onto the status and SQLSTATE a consumer expects.
/// @return The ADBC error to surface
pub fn to_adbc(error: KineticaError) -> AdbcError {
    let message = readable(&error.to_string());
    let lowered = message.to_lowercase();

    let (status, sqlstate) =
        if lowered.contains("unauthorized") || lowered.contains("authentication") {
            (Status::Unauthenticated, "28000")
        } else if lowered.contains("permission") || lowered.contains("not allowed") {
            (Status::Unauthorized, "42501")
        } else if lowered.contains("does not exist") || lowered.contains("not found") {
            (Status::NotFound, "42P01")
        } else if lowered.contains("already exists") {
            (Status::AlreadyExists, "42P07")
        } else if lowered.contains("cancel") {
            (Status::Cancelled, "57014")
        } else if lowered.contains("timed out") || lowered.contains("timeout") {
            (Status::Timeout, "57014")
        } else {
            (Status::IO, "08006")
        };

    let mut adbc = AdbcError::with_message_and_status(message, status);
    adbc.sqlstate = to_sqlstate(sqlstate);
    adbc
}

/// ADBC carries the SQLSTATE as five C characters rather than a string.
/// @return The five-character form of the given state
fn to_sqlstate(state: &str) -> [std::os::raw::c_char; 5] {
    let mut result = [0 as std::os::raw::c_char; 5];

    for (i, byte) in state.bytes().take(result.len()).enumerate() {
        result[i] = byte as std::os::raw::c_char;
    }

    result
}

/// @return The ADBC error for a malformed or unreadable Arrow stream
pub fn from_arrow(error: ArrowError) -> AdbcError {
    AdbcError::with_message_and_status(
        readable(&format!("unable to read the Arrow stream: {error}")),
        Status::Internal,
    )
}

#[cfg(test)]
mod tests {
    use super::readable;

    #[test]
    fn the_framing_around_a_rejected_request_is_taken_out() {
        // What the server answers a bad request with, verbatim.
        let raw =
            "\nERRORzEmpty request header contentType is not allowed (E/EHRHc:629)\u{8}none\0\0";

        let cleaned = readable(raw);

        assert!(
            !cleaned.contains('\0'),
            "a NUL would end the message early: {cleaned}"
        );
        assert!(cleaned.contains("Empty request header contentType is not allowed"));
    }

    #[test]
    fn an_ordinary_message_is_left_alone() {
        assert_eq!(
            readable("Object 'x' not found (S/SDc:1603)"),
            "Object 'x' not found (S/SDc:1603)"
        );
    }
}
