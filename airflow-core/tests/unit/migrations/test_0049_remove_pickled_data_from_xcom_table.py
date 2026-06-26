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
Regression tests for migration 0049 (eed27faa34e3) value sanitization.

The 2.x -> 3.x conversion of ``xcom.value`` from pickled bytea to JSON/JSONB must not
choke on values that are legal in the pickled blob but illegal in strict JSON/JSONB:
non-finite floats (NaN/Infinity/-Infinity) and the U+0000 (NUL) escape. These tests run
the migration's own per-dialect sanitization SQL against an isolated table.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
import sqlalchemy as sa

from airflow import settings

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

# The NUL escape exactly as json.dumps emits it for an embedded null byte (6 chars: backslash u 0000).
# Built via chr() so the escape is unambiguous in source.
_NUL_ESCAPE = chr(92) + "u0000"

# A JSON document carrying every value class the sanitizer must handle.
_RAW = '{"d": "F' + _NUL_ESCAPE + 'oo", "a": NaN, "b": Infinity, "c": -Infinity, "ok": 1.5}'
_EXPECTED = {"d": "Foo", "a": "NaN", "b": "Infinity", "c": "-Infinity", "ok": 1.5}

# Migration filenames start with a digit so they cannot be imported via the normal
# import system; load the module by file path instead.
_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0049_3_0_0_remove_pickled_data_from_xcom_table.py"
)
_spec = importlib.util.spec_from_file_location("migration_0049", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

_TABLE = "_test_xcom_sanitize"


def test_sqlite_sanitize_quotes_nonfinite_and_strips_nul():
    """SQLite branch: the real sanitize SQL yields strict-valid JSON. Backend-independent."""
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE TABLE {_TABLE} (value BLOB)"))
        conn.execute(
            sa.text(f"INSERT INTO {_TABLE} (value) VALUES (:v)"),
            {"v": _RAW.encode("utf-8")},
        )
        conn.execute(sa.text(_migration._xcom_sqlite_sanitize_sql(_TABLE)))
        # Mirror the migration's own json(...) conversion; raises if the value is still invalid JSON.
        got = conn.execute(sa.text(f"SELECT json(CAST(value AS TEXT)) FROM {_TABLE}")).scalar_one()
    assert json.loads(got) == _EXPECTED


@pytest.mark.db_test
class TestPostgresSanitize:
    @pytest.mark.backend("postgres")
    def test_nul_escape_blocks_jsonb_cast_until_sanitized(self):
        drop = f"DROP TABLE IF EXISTS {_TABLE}"
        cast = f"SELECT CAST(CONVERT_FROM(value, 'UTF8') AS JSONB) FROM {_TABLE}"
        with settings.engine.begin() as conn:
            conn.execute(sa.text(drop))
            conn.execute(sa.text(f"CREATE TABLE {_TABLE} (value bytea)"))
            conn.execute(
                sa.text(f"INSERT INTO {_TABLE} (value) VALUES (convert_to(:v, 'UTF8'))"),
                {"v": _RAW},
            )
        try:
            # Before sanitizing, the JSONB cast fails on the NUL escape (the reported bug).
            with settings.engine.connect() as conn:
                with pytest.raises(sa.exc.DataError):
                    conn.execute(sa.text(cast)).all()
                conn.rollback()
            # After the migration's sanitize SQL, the cast succeeds and the values are preserved.
            with settings.engine.begin() as conn:
                conn.execute(sa.text(_migration._xcom_pg_sanitize_sql(_TABLE)))
                conn.execute(sa.text(cast)).all()
                got = conn.execute(
                    sa.text(f"SELECT CONVERT_FROM(value, 'UTF8') FROM {_TABLE}")
                ).scalar_one()
            assert json.loads(got) == _EXPECTED
        finally:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(drop))


@pytest.mark.db_test
class TestMysqlSanitize:
    @pytest.mark.backend("mysql")
    def test_sanitize_allows_json_cast(self):
        drop = f"DROP TABLE IF EXISTS {_TABLE}"
        cast = f"SELECT CAST(CONVERT(value USING utf8mb4) AS JSON) FROM {_TABLE}"
        with settings.engine.begin() as conn:
            conn.execute(sa.text(drop))
            conn.execute(sa.text(f"CREATE TABLE {_TABLE} (value LONGBLOB)"))
            conn.execute(
                sa.text(f"INSERT INTO {_TABLE} (value) VALUES (CONVERT(:v USING utf8mb4))"),
                {"v": _RAW},
            )
        try:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(_migration._xcom_mysql_sanitize_sql(_TABLE)))
                # Must now cast to JSON without raising (bare NaN would be rejected).
                conn.execute(sa.text(cast)).all()
                got = conn.execute(
                    sa.text(f"SELECT CONVERT(value USING utf8mb4) FROM {_TABLE}")
                ).scalar_one()
            assert json.loads(got) == _EXPECTED
        finally:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(drop))
