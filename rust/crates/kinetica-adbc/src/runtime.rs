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

//! The async/sync boundary.
//!
//! The ADBC traits are synchronous, for Rust callers and over the C ABI alike;
//! `gpudb-api` is Tokio-async throughout. There is no async layer in between:
//! every server call the driver makes, from the trait methods and from the
//! `Drop` and cleanup paths too (closing an inserter, clearing a paging table),
//! goes through [`block_on`] on the runtime this module owns. Nothing else in
//! the driver builds a runtime or blocks on one.
//!
//! The runtime is the driver's own, so a caller does not have to bring one.
//! What a caller cannot do is call the driver from inside a Tokio async
//! context: blocking on a runtime from a thread already driving one panics.
//! An async Rust caller goes through `tokio::task::spawn_blocking`.

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Builder, Runtime};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// The driver's own runtime, created on first use.
///
/// Separate from any runtime the caller owns: `block_on` below must never run
/// on a thread that is already driving someone else's executor.
fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("kinetica-adbc")
            .build()
            .expect("failed to build the kinetica-adbc Tokio runtime")
    })
}

/// Drive `future` to completion on the driver's runtime.
///
/// Panics if the calling thread is already inside a Tokio runtime, which is
/// why the driver must not be called from async code directly.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    runtime().block_on(future)
}
