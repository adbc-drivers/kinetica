#!/usr/bin/env bash
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

# The container's command: sets the admin password, then hands over to the
# image's own start script.
#
# The image has no admin password and no way to give it one from outside:
# the admin user is created with whatever password is on record when the
# database first starts, and once it has started there is no user to alter
# ("User 'admin' not found"). The image's own answer is to pause before
# starting (KINETICA_INIT_FILE), have the caller exec in and set it, then
# restart the container -- three steps a compose service cannot take. Setting
# it here, before the start script runs, needs none of them.
#
# The persist volume remembers that it was done, so a container started
# again on the same volume keeps the password it has rather than failing
# to set one that is already set.

set -euo pipefail

PERSIST=/opt/gpudb/persist
DONE="$PERSIST/.admin-password-set"
CORE=/opt/gpudb/core/bin

# What the image's command does before its script
ldconfig

if [ ! -f "$DONE" ]; then
    if [ -z "${KINETICA_ADMIN_PASSWORD:-}" ]; then
        echo "KINETICA_ADMIN_PASSWORD is not set: the admin password can only be set before the first start" >&2
        exit 1
    fi

    # The tool reads the password from this variable rather than the
    # command line, so it is in no process listing. Run as gpudb, which owns
    # the persist directory the security store is created in.
    export GPUDB_ALTER_PASSWORD_PASSWORD="$KINETICA_ADMIN_PASSWORD"
    su gpudb -c "$CORE/gpudb_env.sh python3 $CORE/gpudb_alter_password.py admin $CORE/../etc/gpudb.conf"
    unset GPUDB_ALTER_PASSWORD_PASSWORD

    su gpudb -c "touch '$DONE'"
    echo "Admin password set"
fi

exec /opt/gpudb-docker-start.sh
