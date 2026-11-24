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

# ADBC Driver for Kinetica

An [Arrow ADBC](https://arrow.apache.org/adbc/) driver for
[Kinetica](https://www.kinetica.com/), developed by Kinetica DB, Inc. and built
by the [ADBC Driver Foundry](https://adbc-drivers.org).

Results come back from the server as Arrow, with no row-by-row conversion in
between, and bulk ingest goes straight to the cluster's worker ranks.

📥 To install it with [dbc](https://docs.columnar.tech/dbc), run
`dbc install kinetica`.  For Python, `pip install adbc-driver-kinetica` installs
the driver and its `adbc_driver_kinetica.dbapi` module together.

📚 To browse the documentation, go to
[docs.adbc-drivers.org/drivers/kinetica](https://docs.adbc-drivers.org/drivers/kinetica).

🐛 To report an issue, go to
[github.com/adbc-drivers/kinetica/issues](https://github.com/adbc-drivers/kinetica/issues).

## Requirements

Kinetica 7.2.3.24 or later, the first version to return query results as
Arrow.  The driver refuses a connection to an older server when it opens, and
says which version it found.

## Connecting

```python
from adbc_driver_manager import dbapi

conn = dbapi.connect(
    driver="kinetica",
    db_kwargs={
        "uri": "http://localhost:9191",
        "username": "admin",
        "password": "...",
    },
)

with conn.cursor() as cur:
    cur.execute("SELECT 1 AS n")
    print(cur.fetch_arrow_table())
```

The `uri` may also be a `kinetica://` URI, by which a driver manager given
only the URI knows to load this driver:

```text
kinetica://[user[:password]@]host[:port][,host[:port]...][/path][?name=value&...]
```

It connects over HTTPS unless it says `tls=false`. Every other parameter is
an option under its own name, such as `kinetica.query.max_rows=1000`, and an
option set directly takes precedence over the URI's. Several hosts are an HA
ring, tried in the order given.

```python
conn = dbapi.connect(
    "kinetica://localhost:9191?tls=false",
    db_kwargs={"username": "admin", "password": "..."},
)
```

The example uses Python's `adbc-driver-manager` 1.8 or later, which finds a
driver that dbc installed by its name.  Any other ADBC driver manager loads it
the same way.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0; see [LICENSE.txt](LICENSE.txt).
