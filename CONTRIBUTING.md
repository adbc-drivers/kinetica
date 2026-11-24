<!--
  Copyright (c) 2026 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# How to Contribute

The driver is developed by Kinetica DB, Inc., and changes made there are
synced into this repository.  Issues and pull requests are welcome here; a
change accepted from a pull request is applied upstream and arrives back with
the next sync.

## Reporting Issues and Making Feature Requests

Open an issue at
[github.com/adbc-drivers/kinetica/issues](https://github.com/adbc-drivers/kinetica/issues).
For a problem, say which driver version you have (the driver version
`get_info` reports), which Kinetica version the server reports, and what you
ran.  Leave credentials and data out.

## Build and Test

The driver is a Rust crate, `kinetica-adbc`, in `rust/crates/kinetica-adbc`.
It needs Rust 1.89 or newer.

```shell
$ cd rust/
$ cargo build --release
$ cargo test -p kinetica-adbc
```

The release build produces the shared library ADBC driver managers load:
`target/release/libadbc_driver_kinetica.so` on Linux, `.dylib` on macOS, and
`adbc_driver_kinetica.dll` on Windows.  It exports `AdbcDriverKineticaInit`.

CI builds and tests it through [pixi](https://pixi.sh), with the tasks in
`rust/pixi.toml`:

```shell
$ cd rust/
$ pixi run make      # the library, built as a release builds it
$ pixi run test      # the unit tests, and the live tests where there is a server
```

The unit tests need no server.  Exercising the driver against a database needs
Kinetica 7.2.3.24 or later.  `rust/compose.yaml` starts one in Docker, which
is what CI does before the tests:

```shell
$ cd rust/
$ docker compose up --detach --wait test-service
```

It answers at `http://127.0.0.1:9191` as `admin`, with the password `KI_PWD`
held when it was started (`kinetica-ci` if unset).  The first start pulls a
2 GB image and takes a minute or two.

Nothing else on the machine may be listening on 9191 to 9193 or 9300, the host
manager's port.  The workers advertise `127.0.0.1`, so with another Kinetica
there, multi-head ingest could reach that instance's workers instead of this
one's; stop it first.

The live tests, in `rust/crates/kinetica-adbc-tests`, find it through `KI_URL`,
`KI_USER` and `KI_PWD`, which `rust/.env.linux` sets for this instance.  With
`KI_URL` unset when they are built they are reported as ignored, which is what
CI shows on macOS and Windows; the OAuth tests answer as an identity provider
themselves and run either way:

```shell
$ cd rust/
$ set -a && . ./.env.linux && set +a
$ cargo test
```

With it running, the [validation suite](https://github.com/adbc-drivers/validation)
in `rust/validation/` checks the driver the way every Foundry driver is checked,
and its results fill in the docs page, `rust/docs/kinetica.md`.  It finds the
database through `KI_URL`, `KI_USER` and `KI_PWD`, which `rust/.env.linux` sets
for this instance.  It signs in as `KI_VALIDATION_USER` / `KI_VALIDATION_PWD`
when those are set, as `rust/.env.linux` sets them: an ordinary user that it
creates as `KI_USER` and grants only the two schemas it uses, so that nothing
the driver does relies on what only an administrator may read.  Unset, it
signs in as `KI_USER`.

```shell
$ cd rust/
$ set -a && . ./.env.linux && set +a
$ pixi run make
$ pixi run validate --vendor-version 7.2
$ pixi run gendocs --output generated
```

Where Kinetica differs from the suite's standard cases (its type names, its
date range of 1000 to 2900, its millisecond times), `rust/validation/queries/`
holds Kinetica's version of the case, with the reason in its metadata.  A case
whose only difference is the type name, which the driver reports under
`KINETICA:type` as Kinetica's SQL writes it, holds just a `field-type-name`.

## Opening a Pull Request

Pull request titles follow [Conventional
Commits](https://www.conventionalcommits.org/), which CI checks, and the
repository's pre-commit hooks must pass:

```shell
$ pre-commit run --all-files
```

Every source file carries the Apache License 2.0 header.
