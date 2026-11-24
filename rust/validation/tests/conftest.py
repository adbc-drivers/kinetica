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

import os
import sys
from pathlib import Path

import adbc_driver_manager.dbapi
import adbc_drivers_validation
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from .kinetica import get_quirks


def pytest_addoption(parser):
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default="7.2")


@pytest.fixture(scope="session")
def driver(request, pytestconfig) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver.startswith("kinetica")
    return get_quirks(pytestconfig.getoption("vendor_version"))


@pytest.fixture(scope="session")
def login(driver, driver_path) -> tuple[str, str]:
    """Prepares the instance as its administrator, KI_USER, and says who the
    suite signs in as.

    The suite runs as KI_VALIDATION_USER when that is set: an ordinary user
    with rights on the two schemas it uses and nothing else, so that nothing
    the driver does relies on what only an administrator may read. Unset, it
    runs as KI_USER.
    """
    missing = [name for name in ("KI_URL", "KI_USER", "KI_PWD") if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Must set {', '.join(missing)}")

    admin, admin_password = os.environ["KI_USER"], os.environ["KI_PWD"]
    user = os.environ.get("KI_VALIDATION_USER")
    password = os.environ.get("KI_VALIDATION_PWD")
    if user and not password:
        pytest.exit("KI_VALIDATION_USER is set, but KI_VALIDATION_PWD is not", returncode=4)

    quote = driver.quote_identifier
    schemas = [driver.features.current_schema, driver.features.secondary_schema]

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": os.environ["KI_URL"], "username": admin, "password": admin_password},
            autocommit=True,
        ) as conn,
        conn.cursor() as cursor,
    ):
        # The second schema the catalog and ingest tests reach into; a new
        # instance has only the user's own, and only an administrator can make one
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(schemas[1])}")

        if not user:
            return admin, admin_password

        try:
            cursor.execute(
                f"CREATE USER {quote(user)} IDENTIFIED BY '{password.replace("'", "''")}'"
            )
        except adbc_driver_manager.dbapi.Error as error:
            # Made already, by an earlier module of the same run: the
            # fixtures are set up again for each one
            if "duplicate name" not in str(error).lower():
                raise

        for schema in schemas:
            cursor.execute(f"GRANT ALL ON SCHEMA {quote(schema)} TO {quote(user)}")

    return user, password


@pytest.fixture(scope="session")
def db_kwargs(login) -> dict[str, str]:
    """What every connection the suite makes is opened with: the instance
    KI_URL names, as the user `login` settled on."""
    user, password = login
    return {"uri": os.environ["KI_URL"], "username": user, "password": password}


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    # Where the build puts the library, beside this directory
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    return str(
        Path(__file__).parent.parent.parent
        / f"build/libadbc_driver_{driver.name}.{ext}"
    )
