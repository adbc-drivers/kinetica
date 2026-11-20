---
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
{}
---

{{ cross_reference|safe }}
# Kinetica Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Kinetica][kinetica], a GPU-accelerated
database for real-time analytics.  It is developed by Kinetica DB, Inc.; the
ADBC Driver Foundry builds and distributes it for Linux, macOS, and Windows.

Query results come back from the server as Arrow, and bulk ingest is routed to
the worker ranks that hold each row.

## Installation

The Kinetica driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install kinetica
```

For Python, the driver is also published as a package that bundles it:

```bash
pip install adbc-driver-kinetica
```

## Connecting

To use the driver, give the URL of a Kinetica server as the `uri` option, with
`username` and `password`.

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
```

Note: The example above is for Python using the
[adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but
the process will be similar for other driver managers.

Kinetica-specific options are named `adbc.kinetica.*`.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

The driver needs Kinetica 7.2.3.24 or later, the first version to return query
results as Arrow.  A connection to an older server is refused when it opens,
naming both versions.

{{ compatibility_info|safe }}

{{ footnotes|safe }}

[kinetica]: https://www.kinetica.com/
