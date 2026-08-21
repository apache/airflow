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

The 2.x -> 3.x conversion of ``xcom.value`` from pickled bytea to JSON/JSONB must not choke on
values that are legal in the pickled blob but illegal in strict JSON/JSONB: non-finite floats
(NaN/Infinity/-Infinity) and the U+0000 (NUL) escape. It must leave a value that already parses
as JSON untouched, including one that wraps another JSON document with its interior quotes
escaped, and it must not corrupt a literal backslash-u-0000 in the data. These tests run the
migration's own per-dialect SQL against an isolated table.
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
_EXPECTED = {"d": "Foo", "a": None, "b": None, "c": None, "ok": 1.5}

# Row 2: a string that literally contains backslash-u-0000 (no null byte). It serializes to an
# escaped backslash sequence and must survive unchanged.
_LITERAL_VALUE = "x" + _BS + "u0000y"
_LITERAL_RAW = json.dumps({"k": _LITERAL_VALUE})
_LITERAL_EXPECTED = {"k": _LITERAL_VALUE}

# Rows 3 and 4: a task pushed already-serialized JSON, so the value is a JSON string wrapping
# another document with its interior quotes escaped. Both already parse, so the non-finite
# rewrite must skip them: quoting the token would close the wrapping string early and abort the
# cast, and nulling it would rewrite data the migration has no reason to touch.
_INNER = json.dumps({"amount": 604441.0, "commission": float("nan"), "rate": float("-inf")})
_ESCAPED_RAW = json.dumps(_INNER)
_NESTED_RAW = json.dumps({"report": _INNER})

# Row 5: invalid at the top level and wrapping an escaped document. The rewrite has to run, so
# the inner tokens go too; ``null`` keeps the result parseable where a quote would not.
_MIXED_RAW = json.dumps({"top": float("nan"), "report": _INNER})
_MIXED_EXPECTED = {
    "top": None,
    "report": json.dumps({"amount": 604441.0, "commission": None, "rate": None}),
}

# Row 6: invalid only because of the NUL escape, and wrapping an escaped document. Proves the
# strip runs before the rewrite: stripping first makes the value parse, so the inner NaN is
# preserved. Guarding before stripping would send this row through the rewrite and corrupt it.
_ORDER_RAW = json.dumps({"n": "x" + chr(0) + "y", "report": _INNER})
_ORDER_EXPECTED = {"n": "xy", "report": _INNER}

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

# id -> serialized value, inserted into the isolated table by every dialect test.
_ROWS = {
    1: _RAW,
    2: _LITERAL_RAW,
    3: _ESCAPED_RAW,
    4: _NESTED_RAW,
    5: _MIXED_RAW,
    6: _ORDER_RAW,
}


def _assert_sanitized(rows: dict[int, str]) -> None:
    """Check the sanitized text of every row. Rows 3, 4 and 6 compare the inner document as an
    exact string, so dropped whitespace inside it fails here rather than passing a loads() check.
    """
    assert json.loads(rows[1]) == _EXPECTED
    assert json.loads(rows[2]) == _LITERAL_EXPECTED
    assert json.loads(rows[3]) == _INNER
    assert json.loads(rows[4]) == {"report": _INNER}
    assert json.loads(rows[5]) == _MIXED_EXPECTED
    assert json.loads(rows[6]) == _ORDER_EXPECTED
    # Rows 3 and 4 already parsed, so they must be byte-identical to what was stored.
    assert rows[3] == _ESCAPED_RAW
    assert rows[4] == _NESTED_RAW


def _sqlite_sanitized(json1: bool = True) -> dict[int, str]:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE TABLE {_TABLE} (id INTEGER PRIMARY KEY, value BLOB)"))
        for row_id, value in _ROWS.items():
            conn.execute(
                sa.text(f"INSERT INTO {_TABLE} (id, value) VALUES (:i, :v)"),
                {"i": row_id, "v": value.encode("utf-8")},
            )
        for stmt in _migration._xcom_sqlite_sanitize_statements(_TABLE, json1=json1):
            conn.execute(sa.text(stmt))
        # json(...) mirrors the migration's own conversion and raises if still invalid JSON.
        # It also re-serializes, so the assertions read the stored text instead.
        rows = conn.execute(
            sa.text(f"SELECT id, CAST(value AS TEXT), json(CAST(value AS TEXT)) FROM {_TABLE}")
        ).all()
        return {row[0]: row[1] for row in rows}


def test_sqlite_sanitize():
    """SQLite branch: real sanitize SQL on an in-memory db. Backend-independent."""
    _assert_sanitized(_sqlite_sanitized())


def test_sqlite_sanitize_without_json1():
    """Without JSON1 there is no guard, so already-valid values are rewritten too. The result
    still has to be valid JSON, which is what keeps the migration completing on old builds.
    """
    rows = _sqlite_sanitized(json1=False)
    assert json.loads(rows[1]) == _EXPECTED
    assert json.loads(rows[3]) == _MIXED_EXPECTED["report"]


@pytest.mark.db_test
class TestPostgresSanitize:
    @pytest.mark.backend("postgres")
    def test_nul_and_nan_block_jsonb_cast_until_sanitized(self):
        drop = f"DROP TABLE IF EXISTS {_TABLE}"
        cast = f"SELECT CAST(CONVERT_FROM(value, 'UTF8') AS JSONB) FROM {_TABLE}"
        with settings.engine.begin() as conn:
            conn.execute(sa.text(drop))
            conn.execute(sa.text(f"CREATE TABLE {_TABLE} (id int PRIMARY KEY, value bytea)"))
            for row_id, value in _ROWS.items():
                conn.execute(
                    sa.text(f"INSERT INTO {_TABLE} VALUES (:i, convert_to(:v, 'UTF8'))"),
                    {"i": row_id, "v": value},
                )
        try:
            # Before sanitizing, the JSONB cast fails (the reported upgrade failure).
            with settings.engine.connect() as conn:
                with pytest.raises(sa.exc.DataError):
                    conn.execute(sa.text(cast)).all()
                conn.rollback()
            # pg_temp is per-session, so the helper and the UPDATE share one connection.
            with settings.engine.begin() as conn:
                for stmt in _migration._xcom_pg_sanitize_statements(_TABLE):
                    conn.execute(sa.text(stmt))
                conn.execute(sa.text(cast)).all()
                rows = dict(
                    conn.execute(sa.text(f"SELECT id, CONVERT_FROM(value, 'UTF8') FROM {_TABLE}")).all()
                )
            _assert_sanitized(rows)
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
            conn.execute(sa.text(f"CREATE TABLE {_TABLE} (id int PRIMARY KEY, value LONGBLOB)"))
            for row_id, value in _ROWS.items():
                conn.execute(
                    sa.text(f"INSERT INTO {_TABLE} VALUES (:i, CONVERT(:v USING utf8mb4))"),
                    {"i": row_id, "v": value},
                )
        try:
            with settings.engine.begin() as conn:
                for stmt in _migration._xcom_mysql_sanitize_statements(_TABLE):
                    conn.execute(sa.text(stmt))
                conn.execute(sa.text(cast)).all()  # must not raise (bare NaN would be rejected)
                rows = dict(
                    conn.execute(sa.text(f"SELECT id, CONVERT(value USING utf8mb4) FROM {_TABLE}")).all()
                )
            _assert_sanitized(rows)
        finally:
            with settings.engine.begin() as conn:
                conn.execute(sa.text(drop))
