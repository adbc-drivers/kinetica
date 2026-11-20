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

//! The client, and the server session it holds for as long as anything is
//! still using it.

use std::ops::Deref;
use std::sync::Arc;

use gpudb_api::Kinetica;

use crate::control::Impersonation;
use crate::ingest;
use crate::lookup;
use crate::runtime;

/// What a connection talks to the server through, shared by everything that
/// does so on its behalf: its statements, the readers they hand out, and the
/// closures that fetch later pages.
///
/// Ending the session drops the SQL `#name` temp tables it owns. Left to the
/// server they sit there until the session has been idle for
/// `sql.session_idle_timeout` (an hour by default, and never where that is 0),
/// so a caller opening connections in turn would carry every one of their temp
/// tables until then.
///
/// It ends when the last holder lets go, rather than when the connection is
/// released, because any of them can outlive the connection: a reader still
/// streaming, or a statement opened from a connection the caller no longer
/// holds, goes on needing the client it was given. A session the caller named
/// is not dropped at all; see `options::SESSION_ID`.
pub(crate) struct Session {
    client: Arc<Kinetica>,
    /// The connection's inserters, kept here so that they are flushed before
    /// the client stops sending: rows still queued may be bound for a table
    /// the session owns.
    inserters: Arc<ingest::Inserters>,
    /// What this connection has worked out about the tables it has looked up
    /// by key. They hold no rows, so unlike the inserters nothing has to be
    /// sent before the client stops.
    retrievers: Arc<lookup::Retrievers>,
    /// Who this connection acts as. Held here because the header that says so
    /// is the client's, and because it outlives any one statement.
    impersonation: Impersonation,
}

impl Session {
    /// @return A session over the given client, acting as `user` from the
    ///         start, and with no inserters built yet
    pub(crate) fn new(client: Kinetica, user: &str) -> Self {
        Self {
            client: Arc::new(client),
            inserters: Arc::default(),
            retrievers: Arc::default(),
            impersonation: Impersonation::new(user),
        }
    }

    /// The client as the bulk inserter takes it, which is the one caller that
    /// wants the `Arc` rather than the client it holds.
    /// @return The shared client
    pub(crate) fn client(&self) -> &Arc<Kinetica> {
        &self.client
    }

    /// @return The inserters built for this connection so far
    pub(crate) fn inserters(&self) -> &ingest::Inserters {
        &self.inserters
    }

    /// @return What this connection knows about the tables it has looked up
    pub(crate) fn retrievers(&self) -> &lookup::Retrievers {
        &self.retrievers
    }

    /// @return Who this connection is acting as
    pub(crate) fn impersonation(&self) -> &Impersonation {
        &self.impersonation
    }
}

/// So that a session stands in for the client wherever one is asked for.
impl Deref for Session {
    type Target = Kinetica;

    fn deref(&self) -> &Kinetica {
        &self.client
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.inserters.close();

        runtime::block_on(self.client.close());
    }
}
