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

migration = import_module("airflow.migrations.versions.0150_3_5_0_add_deadline_alert_run_type_filter")


@pytest.mark.db_test
def test_run_type_filter_migration_preserves_alerts_and_deadline_links(tmp_path):
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'deadlines.db'}")
    try:
        with engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql("CREATE TABLE deadline_alert (id INTEGER PRIMARY KEY)")
            connection.exec_driver_sql(
                "CREATE TABLE deadline (id INTEGER PRIMARY KEY, deadline_alert_id INTEGER "
                "REFERENCES deadline_alert(id) ON DELETE SET NULL)"
            )
            connection.exec_driver_sql("INSERT INTO deadline_alert VALUES (1)")
            connection.exec_driver_sql("INSERT INTO deadline VALUES (1, 1)")
            connection.commit()
            context = MigrationContext.configure(connection)

            with Operations.context(context):
                migration.upgrade()
            connection.commit()

            assert connection.exec_driver_sql("SELECT run_type_filter FROM deadline_alert").scalar() is None
            connection.exec_driver_sql("UPDATE deadline_alert SET run_type_filter = '[\"scheduled\"]'")
            connection.commit()

            with Operations.context(context):
                migration.downgrade()
            connection.commit()

            assert {column["name"] for column in sa.inspect(connection).get_columns("deadline_alert")} == {
                "id"
            }
            assert connection.exec_driver_sql("SELECT id FROM deadline_alert").scalar() == 1
            assert connection.exec_driver_sql("SELECT deadline_alert_id FROM deadline").scalar() == 1
    finally:
        engine.dispose()
