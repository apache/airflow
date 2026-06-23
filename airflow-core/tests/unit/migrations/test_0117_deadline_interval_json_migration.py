#
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

"""
Regression test for migration 0117 (8812eb67b63c) on MySQL.

The downgrade must convert ``deadline_alert.interval`` from JSON back to FLOAT without
raising ``ER_INVALID_JSON_TEXT`` (3140). The failure only reproduces with at least one
row present, so this seeds rows and runs the migration's own conversion SQL. It is a
no-op on SQLite/PostgreSQL, which take different code paths.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

from airflow import settings

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

pytestmark = pytest.mark.db_test

# Migration filenames start with a digit so they cannot be imported via the
# normal import system; load the module by file path instead.
_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0117_3_3_0_change_deadline_interval_to_json.py"
)
_spec = importlib.util.spec_from_file_location("migration_0117", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

# Isolated table so we run the real conversion SQL without seeding the live deadline_alert
# table (which has FK/NOT NULL columns).
_TABLE = "_test_deadline_interval_dg"

# A serialized timedelta as written by the 0117 upgrade.
_WRAPPED_TIMEDELTA = '{"__classname__": "datetime.timedelta", "__version__": 2, "__data__": 300.0}'


class TestMigration0117Downgrade:
    @pytest.mark.backend("mysql")
    def test_mysql_downgrade_interval_value_update_does_not_reject_on_json_column(self):
        """The downgrade value-conversion UPDATE must not raise 3140, and must round-trip to FLOAT."""
        create = f"CREATE TABLE {_TABLE} (id INT PRIMARY KEY, `interval` JSON NOT NULL)"
        drop = f"DROP TABLE IF EXISTS {_TABLE}"

        with settings.engine.begin() as conn:
            conn.execute(sa.text(drop))
            conn.execute(sa.text(create))
            conn.execute(
                sa.text(f"INSERT INTO {_TABLE} (id, `interval`) VALUES (1, :v)"),
                {"v": _WRAPPED_TIMEDELTA},
            )
            conn.execute(
                sa.text(f"INSERT INTO {_TABLE} (id, `interval`) VALUES (2, CAST(:v AS JSON))"),
                {"v": "60.0"},
            )

        try:
            with settings.engine.begin() as conn:
                # Column is still JSON here; this UPDATE must not raise 3140. The retype casts.
                conn.execute(sa.text(_migration._mysql_downgrade_interval_value_sql(_TABLE)))
                conn.execute(sa.text(f"ALTER TABLE {_TABLE} MODIFY `interval` FLOAT NOT NULL"))

            with settings.engine.connect() as conn:
                rows = dict(conn.execute(sa.text(f"SELECT id, `interval` FROM {_TABLE}")).all())

            assert rows == {1: 300.0, 2: 60.0}
        finally:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(drop))
