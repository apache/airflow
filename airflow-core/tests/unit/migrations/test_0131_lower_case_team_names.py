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
Tests for migration 0131 (c7f0a5d2e9b4), which lower-cases stored team names.

``team.name`` is a primary key whose foreign keys do not cascade on update, so the rename has
to carry every referring row with it. A row left behind would either break the constraint or
strand that team's Connections, Variables and Pools under a name nothing resolves.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command

from airflow import settings
from airflow.models import Connection, Pool, Variable
from airflow.models.team import Team
from airflow.utils.db import _get_alembic_config
from airflow.utils.session import create_session

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

pytestmark = pytest.mark.db_test

# Migration filenames start with a digit so they cannot be imported via the normal import
# system; load the module by file path instead.
_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH) / "airflow/migrations/versions/0131_3_4_0_lower_case_team_names.py"
)
_spec = importlib.util.spec_from_file_location("migration_0131", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

_OLD = "Data-Eng"
_NEW = "data-eng"


def _team_names(conn) -> set[str]:
    return {row.name for row in conn.execute(sa.text("SELECT name FROM team"))}


class TestMigration0131:
    def test_rename_carries_referring_rows_with_the_team(self):
        with create_session() as session:
            session.add(Team(name=_OLD))
            session.flush()
            session.add_all(
                [
                    Connection(conn_id="c_0131", conn_type="http", team_name=_OLD),
                    Variable(key="v_0131", val="x", team_name=_OLD),
                    Pool(pool="p_0131", slots=1, description="", include_deferred=False, team_name=_OLD),
                ]
            )
            session.commit()

        try:
            with settings.engine.begin() as conn:
                for statement in _migration.build_lower_casing_statements():
                    conn.execute(statement)

            with settings.engine.connect() as conn:
                assert _OLD not in _team_names(conn)
                assert _NEW in _team_names(conn)
                for table, key_column, key in (
                    ("connection", "conn_id", "c_0131"),
                    ("variable", "key", "v_0131"),
                    ("slot_pool", "pool", "p_0131"),
                ):
                    team_name = conn.execute(
                        sa.text(f"SELECT team_name FROM {table} WHERE {key_column} = :k"), {"k": key}
                    ).scalar()
                    assert team_name == _NEW, table
        finally:
            with settings.engine.begin() as conn:
                conn.execute(sa.text("DELETE FROM connection WHERE conn_id = 'c_0131'"))
                conn.execute(sa.text("DELETE FROM variable WHERE key = 'v_0131'"))
                conn.execute(sa.text("DELETE FROM slot_pool WHERE pool = 'p_0131'"))
                conn.execute(
                    sa.text("DELETE FROM team WHERE name IN (:old, :new)"), {"old": _OLD, "new": _NEW}
                )

    def test_upgrade_emits_sql_offline(self, capsys):
        """``db migrate --show-sql-only`` has no rows to read, so the migration must not read any."""
        command.upgrade(_get_alembic_config(), f"{_migration.down_revision}:{_migration.revision}", sql=True)

        emitted = capsys.readouterr().out
        assert "INSERT INTO team (name) SELECT lower(team.name)" in emitted
        assert "UPDATE team SET name=lower(team.name)" in emitted
