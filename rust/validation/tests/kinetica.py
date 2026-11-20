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

import functools
import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class KineticaQuirks(model.DriverQuirks):
    name = "kinetica"
    driver = "adbc_driver_kinetica"
    driver_name = "Kinetica ADBC Driver"
    vendor_name = "Kinetica"
    # The full server version, MAJOR.MINOR.REVISION.ABI and its build
    vendor_version = re.compile(r"7\.2\.\d+\.\d+.*")
    short_version = "7.2"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        # Kinetica has schemas but no catalogs above them; a catalog names a
        # federated data source, which the test instance has none of
        connection_set_current_catalog=False,
        connection_set_current_schema=True,
        connection_transactions=False,
        get_objects=True,
        get_objects_constraints_check=False,
        get_objects_constraints_foreign=True,
        get_objects_constraints_primary=True,
        get_objects_constraints_unique=False,
        get_objects_tabletypes_filter=True,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_schema=True,
        statement_execute_schema=True,
        # One field per placeholder, typed null: the server does not say
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=True,
        # A primary key is a property of each column it is made of, so the
        # server gives its columns in table order, not the order declared
        quirk_get_objects_constraints_primary_normalized=True,
        current_schema="ki_home",
        # Made by the suite when it starts (conftest.py)
        secondary_schema="adbc_secondary",
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": model.FromEnv("KI_URL"),
            "username": model.FromEnv("KI_USER"),
            "password": model.FromEnv("KI_PWD"),
        },
        connection={},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        message = str(error).lower()
        if not any(
            phrase in message
            for phrase in ("does not exist", "not found", "no matching table")
        ):
            return False
        return table_name is None or table_name.lower() in message

    def split_statement(self, statement: str) -> list[str]:
        # The server runs one statement per request. A `--` comment after a
        # statement's `;` would hide that semicolon from the split, so it goes
        # first.
        lines = [_without_comment(line) for line in statement.splitlines()]
        return quirks.split_statement("\n".join(lines))

    @property
    def sample_ddl_constraints(self) -> list[str]:
        # A foreign key needs the referencing table sharded on the columns
        # that point at the other table's primary key, and is added after.
        # Shard key columns are ordered by where they sit in the table, so the
        # referencing columns are declared in the order the key pairs them.
        return [
            "CREATE TABLE constraint_primary (z INT, a INT NOT NULL, b VARCHAR, PRIMARY KEY (a))",
            "CREATE TABLE constraint_primary_multi (z INT, a INT NOT NULL, b VARCHAR NOT NULL, PRIMARY KEY (b, a))",
            "CREATE TABLE constraint_primary_multi2 (z INT, a VARCHAR NOT NULL, b INT NOT NULL, PRIMARY KEY (a, b))",
            "CREATE TABLE constraint_foreign (z INT, a INT, b INT NOT NULL, SHARD KEY (b))",
            "ALTER TABLE constraint_foreign ADD FOREIGN KEY (b) REFERENCES constraint_primary(a)",
            "CREATE TABLE constraint_foreign_multi (z INT, a INT, c VARCHAR NOT NULL, b INT NOT NULL, SHARD KEY (c, b))",
            "ALTER TABLE constraint_foreign_multi ADD FOREIGN KEY (c, b) REFERENCES constraint_primary_multi2(a, b)",
            # Columns dropped, so a column's position differs from its index
            "ALTER TABLE constraint_primary DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi DROP COLUMN z",
            "ALTER TABLE constraint_primary_multi2 DROP COLUMN z",
            "ALTER TABLE constraint_foreign DROP COLUMN z",
            "ALTER TABLE constraint_foreign_multi DROP COLUMN z",
        ]


def _without_comment(line: str) -> str:
    """The line without a trailing `--` comment outside any quoted string."""
    quoted = False
    for i, char in enumerate(line):
        if char == "'":
            quoted = not quoted
        elif not quoted and line.startswith("--", i):
            return line[:i].rstrip()
    return line


@functools.cache
def get_quirks(version: str) -> KineticaQuirks:
    quirks = KineticaQuirks()
    if version != quirks.short_version:
        raise ValueError(f"Unsupported Kinetica version: {version}")
    return quirks
