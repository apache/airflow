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

The 2.x -> 3.x conversion of ``xcom.value`` from pickled bytea to JSON/JSONB must not choke
on values that are legal in the pickled blob but illegal in strict JSON/JSONB: non-finite
floats (NaN/Infinity/-Infinity) and the U+0000 (NUL) escape. It must also NOT corrupt a
genuinely escaped backslash sequence (a literal backslash-u-0000 in the data). These tests
run the migration's own per-dialect sanitization SQL against an isolated table.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
import sqlalchemy as sa

from airflow import settings

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

# A single backslash, built via chr() so no literal escape appears in the source.
_BS = chr(92)

# Row 1: every value class the sanitizer must clean. chr(0) is a real embedded null byte;
# json.dumps serializes it to the 6-char NUL escape, which is what the migration must strip.
_RAW = json.dumps(
    {"d": "F" + chr(0) + "oo", "a": float("nan"), "b": float("inf"), "c": float("-inf"), "ok": 1.5}
)
_EXPECTED = {"d": "Foo", "a": "NaN", "b": "Infinity", "c": "-Infinity", "ok": 1.5}

# Row 2: a string that literally contains backslash-u-0000 (no null byte). It serializes to
# an escaped backslash sequence (\\u0000) and MUST survive sanitization unchanged.
_LITERAL_VALUE = "x" + _BS + "u0000y"
_LITERAL_RAW = json.dumps({"k": _LITERAL_VALUE})
_LITERAL_EXPECTED = {"k": _LITERAL_VALUE}

# Migration filenames start with a digit so they cannot be imported via the normal import
# system; load the module by file path instead.
_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0049_3_0_0_remove_pickled_data_from_xcom_table.py"
)
_spec = importlib.util.spec_from_file_location("migration_0049", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

_TABLE = "_test_xcom_sanitize"


def test_sqlite_sanitize_quotes_nonfinite_strips_nul_and_keeps_literal():
    """SQLite branch: real sanitize SQL on an in-memory db. Backend-independent."""
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE TABLE {_TABLE} (id INTEGER PRIMARY KEY, value BLOB)"))
        conn.execute(
            sa.text(f"INSERT INTO {_TABLE} (id, value) VALUES (1, :v)"),
            {"v": _RAW.encode("utf-8")},
        )
        conn.execute(
            sa.text(f"INSERT INTO {_TABLE} (id, value) VALUES (2, :v)"),
            {"v": _LITERAL_RAW.encode("utf-8")},
        )
        conn.execute(sa.text(_migration._xcom_sqlite_sanitize_sql(_TABLE)))
        # json(...) mirrors the migration's own conversion and raises if still invalid JSON.
        rows = dict(conn.execute(sa.text(f"SELECT id, json(CAST(value AS TEXT)) FROM {_TABLE}")).all())
    assert json.loads(rows[1]) == _EXPECTED
    assert json.loads(rows[2]) == _LITERAL_EXPECTED


@pytest.mark.db_test
class TestPostgresSanitize:
    @pytest.mark.backend("postgres")
    def test_nul_blocks_jsonb_cast_until_sanitized_and_literal_survives(self):
        drop = f"DROP TABLE IF EXISTS {_TABLE}"
        cast = f"SELECT CAST(CONVERT_FROM(value, 'UTF8') AS JSONB) FROM {_TABLE}"
        with settings.engine.begin() as conn:
            conn.execute(sa.text(drop))
            conn.execute(sa.text(f"CREATE TABLE {_TABLE} (id int PRIMARY KEY, value bytea)"))
            conn.execute(sa.text(f"INSERT INTO {_TABLE} VALUES (1, convert_to(:v, 'UTF8'))"), {"v": _RAW})
            conn.execute(
                sa.text(f"INSERT INTO {_TABLE} VALUES (2, convert_to(:v, 'UTF8'))"),
                {"v": _LITERAL_RAW},
            )
        try:
            # Before sanitizing, the JSONB cast fails on the NUL escape (the reported bug).
            with settings.engine.connect() as conn:
                with pytest.raises(sa.exc.DataError):
                    conn.execute(sa.text(cast)).all()
                conn.rollback()
            # After the migration's sanitize SQL, the cast succeeds and values are correct.
            with settings.engine.begin() as conn:
                conn.execute(sa.text(_migration._xcom_pg_sanitize_sql(_TABLE)))
                conn.execute(sa.text(cast)).all()
                rows = dict(
                    conn.execute(sa.text(f"SELECT id, CONVERT_FROM(value, 'UTF8') FROM {_TABLE}")).all()
                )
            assert json.loads(rows[1]) == _EXPECTED
            assert json.loads(rows[2]) == _LITERAL_EXPECTED
        finally:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(drop))


@pytest.mark.db_test
class TestMysqlSanitize:
    @pytest.mark.backend("mysql")
    def test_sanitize_allows_json_cast_and_literal_survives(self):
        drop = f"DROP TABLE IF EXISTS {_TABLE}"
        cast = f"SELECT CAST(CONVERT(value USING utf8mb4) AS JSON) FROM {_TABLE}"
        with settings.engine.begin() as conn:
            conn.execute(sa.text(drop))
            conn.execute(sa.text(f"CREATE TABLE {_TABLE} (id int PRIMARY KEY, value LONGBLOB)"))
            conn.execute(sa.text(f"INSERT INTO {_TABLE} VALUES (1, CONVERT(:v USING utf8mb4))"), {"v": _RAW})
            conn.execute(
                sa.text(f"INSERT INTO {_TABLE} VALUES (2, CONVERT(:v USING utf8mb4))"),
                {"v": _LITERAL_RAW},
            )
        try:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(_migration._xcom_mysql_sanitize_sql(_TABLE)))
                conn.execute(sa.text(cast)).all()  # must not raise (bare NaN would be rejected)
                rows = dict(
                    conn.execute(sa.text(f"SELECT id, CONVERT(value USING utf8mb4) FROM {_TABLE}")).all()
                )
            assert json.loads(rows[1]) == _EXPECTED
            assert json.loads(rows[2]) == _LITERAL_EXPECTED
        finally:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(drop))
