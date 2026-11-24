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

# Kinetica ADBC Driver Changelog

## Version 7.2

### Version 7.2.1 - 2026-11-24

#### Added
-   Connection by a `kinetica://` URI, which lets a driver manager given only
    the URI load this driver:
    `kinetica://[user[:password]@]host[:port][/path][?name=value&...]`, with
    several comma-separated hosts for an HA ring.  It connects over HTTPS
    unless it says `tls=false`, and takes any option as a query parameter
    (e.g., `?kinetica.query.max_rows=1000`); an option set directly takes
    precedence.  `http://` and `https://` URLs work as before.

#### Changed
-   Options are named `kinetica.*` rather than `adbc.kinetica.*`.
-   Error messages start with `[kinetica]`, and prepend the driver's message.
-   A result column's field metadata names its type as Kinetica's SQL writes
    it, under `KINETICA:type` (e.g., `VARCHAR(16)`, `DECIMAL(10,2)`), and
    gives the server's properties and nullability under `KINETICA:properties`
    and `KINETICA:nullable`, in place of the `kinetica.type`,
    `kinetica.properties` and `kinetica.nullable` keys.  Ingest still reads
    `kinetica.properties`.
-   Updated underlying Rust API to 7.2.4.

#### Fixed
-   A request the server refused with "Access denied" was reported as a
    connection failure (`ADBC_STATUS_IO`, SQLSTATE 08006); it is now
    `ADBC_STATUS_UNAUTHORIZED` (SQLSTATE 42501).

#### Notes
-   7.2.0 was published on GitHub but never distributed through `dbc` or
    PyPI; 7.2.1 is the first release they carry.


### Version 7.2.0 - 2026-11-22

Initial release of the Kinetica ADBC driver, an Arrow Database Connectivity
driver for Kinetica 7.2.3.24 or later, for Rust and for any language with an
ADBC driver manager.  It is installed with `dbc install kinetica`, or for
Python with `pip install adbc-driver-kinetica`.  An older server is refused
when the connection opens, naming the version it found.

The driver's version is SemVer:  `MAJOR.MINOR` is the Kinetica version it
works with, and the patch counts the driver's own releases for it, so 7.2.0,
7.2.1 and so on are all drivers for Kinetica 7.2.

#### Features
-   Every Kinetica column type mapped to a corresponding Arrow type.
-   Paged query results returned as Arrow, with no row-by-row
    conversion, from a single execution.  Partitioned reads let several
    connections share one execution.
-   Distributed bulk ingest with `adbc_ingest`, in the append, create, replace
    and create_append modes. An insert with bound parameters, as well as a local
    file ingest (e.g., `INSERT INTO t SELECT * FROM FILE."data.csv"`), both take
    advantage of distributed ingest.
-   HA fail-over/fail-back across an HA ring of clusters.
-   The catalog through `get_objects`, with each table's primary and foreign
    keys, `get_table_schema`, and `get_statistics`.
-   Distributed lookups of records by shard key value.
-   A SQL session per connection, supporting private `#name` temp tables.
    Using `adbc.kinetica.session_id` allows sharing one session between
    connections.
-   Authentication by password or OAuth, including client credentials, device
    authorization, authorization code with PKCE, JWT bearer and token exchange,
    with Entra ID and Okta presets.  TLS is supported with a trust store or
    bypass.
-   Connection, query and ingest options under `adbc.kinetica.*`, including a
    row cap (`adbc.kinetica.max_rows`); session statements such as `SET USER`
    and `SET SCHEMA`, for tools that can only send SQL; and `UPLOAD` and
    `DOWNLOAD`, for moving files to and from KiFS.
-   A Python package, `adbc-driver-kinetica`, carrying the driver for Linux
    (x86-64, ARM64), macOS (Apple silicon) and Windows (x86-64), for Python
    3.11 or later.
