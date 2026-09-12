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
Regression test for migration 0096 (b12d4f98a91e).

The upgrade renames ``team_id`` columns to ``team_name`` while the values are still team
UUIDs, then drops ``team.id`` and creates foreign keys onto ``team.name``. Without a value
conversion first, FK validation fails on populated deployments (or silently orphans the
references on SQLite). The conversion helper is exercised here against scratch tables.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

from airflow import settings

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

pytestmark = pytest.mark.db_test

_MIGRATION_PATH = Path(AIRFLOW_CORE_SOURCES_PATH) / "airflow/migrations/versions/0096_3_2_0_remove_team_id.py"
_spec = importlib.util.spec_from_file_location("migration_0096", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

_TEAM_TABLE = "_test_team_conv"
_REF_TABLE = "_test_team_conv_ref"


class TestMigration0096TeamIdConversion:
    """The upgrade must turn team UUID references into names before dropping the mapping."""

    @pytest.fixture
    def scratch_tables(self):
        with settings.engine.begin() as conn:
            for table in (_REF_TABLE, _TEAM_TABLE):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
            conn.execute(
                sa.text(f"CREATE TABLE {_TEAM_TABLE} (id VARCHAR(36) PRIMARY KEY, name VARCHAR(50))")
            )
            conn.execute(sa.text(f"CREATE TABLE {_REF_TABLE} (id INT PRIMARY KEY, team_name VARCHAR(50))"))
        yield
        with settings.engine.begin() as conn:
            for table in (_REF_TABLE, _TEAM_TABLE):
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))

    @pytest.mark.usefixtures("scratch_tables")
    def test_uuid_references_become_names_and_others_are_untouched(self):
        team_id = "355e7498-3dca-4432-9d90-33f1dbc5af3d"
        with settings.engine.begin() as conn:
            conn.execute(
                sa.text(f"INSERT INTO {_TEAM_TABLE} (id, name) VALUES (:i, :n)"),
                {"i": team_id, "n": "team-a"},
            )
            rows = [
                (1, team_id),  # must become the team name
                (2, None),  # unbound row must stay NULL
                (3, "other-uuid-not-a-team"),  # unknown value must stay as it is
            ]
            for row_id, value in rows:
                conn.execute(
                    sa.text(f"INSERT INTO {_REF_TABLE} (id, team_name) VALUES (:i, :v)"),
                    {"i": row_id, "v": value},
                )

            _migration._convert_team_ids_to_names(conn, tables=(_REF_TABLE,), team_table=_TEAM_TABLE)

            result = dict(conn.execute(sa.text(f"SELECT id, team_name FROM {_REF_TABLE}")).fetchall())

        assert result == {1: "team-a", 2: None, 3: "other-uuid-not-a-team"}
