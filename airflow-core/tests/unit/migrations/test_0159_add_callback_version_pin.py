# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from importlib import import_module

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations

migration = import_module("airflow.migrations.versions.0159_3_5_0_add_callback_version_pin")


@pytest.mark.db_test
def test_sqlite_routing_migration_preserves_pending_callbacks_and_deadlines():
    # A SQLite table rebuild with active foreign keys deletes deadline rows through their cascade.
    engine = sa.create_engine("sqlite://")
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA foreign_keys=ON")
        conn.exec_driver_sql("CREATE TABLE callback (id INTEGER PRIMARY KEY, bundle_name VARCHAR(250))")
        conn.exec_driver_sql(
            "CREATE TABLE deadline (callback_id INTEGER REFERENCES callback(id) ON DELETE CASCADE)"
        )
        conn.exec_driver_sql("INSERT INTO callback VALUES (1, 'legacy')")
        conn.exec_driver_sql("INSERT INTO deadline VALUES (1)")
        conn.commit()
        context = MigrationContext.configure(conn, opts={"transactional_ddl": True})
        with Operations.context(context), context.begin_transaction():
            migration.upgrade()
            assert conn.exec_driver_sql(
                "SELECT is_version_pinned, decode_retry_after FROM callback"
            ).one() == (None, None)
            assert "idx_callback_bundle_version_pinned" in {
                idx["name"] for idx in sa.inspect(conn).get_indexes("callback")
            }
        with Operations.context(context), context.begin_transaction():
            migration.downgrade()
        assert {col["name"] for col in sa.inspect(conn).get_columns("callback")} == {"id", "bundle_name"}
        assert conn.exec_driver_sql("SELECT * FROM callback").one() == (1, "legacy")
        assert conn.exec_driver_sql("SELECT * FROM deadline").one() == (1,)
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
