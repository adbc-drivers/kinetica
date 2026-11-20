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

//! Marks where there is a Kinetica to test against: with `KI_URL` set, the
//! live tests run; without it, as on a machine with no server, they are
//! reported as ignored rather than failing for want of one.

fn main() {
    println!("cargo:rustc-check-cfg=cfg(kinetica_server)");
    println!("cargo:rerun-if-env-changed=KI_URL");

    if std::env::var_os("KI_URL").is_some_and(|url| !url.is_empty()) {
        println!("cargo:rustc-cfg=kinetica_server");
    }
}
