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
from __future__ import annotations

import pytest

from airflow.providers.common.sql.operators import read_only_guard


@pytest.mark.parametrize(
    ("sql", "expected_kind"),
    [
        pytest.param("SELECT * FROM foo", None, id="select"),
        pytest.param("SELECT 1;", None, id="select-trailing-semicolon"),
        pytest.param("INSERT INTO foo VALUES (1)", "INSERT", id="insert"),
        pytest.param("UPDATE foo SET x = 1", "UPDATE", id="update"),
        pytest.param("DELETE FROM foo", "DELETE", id="delete"),
        pytest.param(
            "MERGE INTO foo USING bar ON foo.id = bar.id WHEN MATCHED THEN UPDATE SET x = 1",
            "MERGE",
            id="merge",
        ),
        pytest.param("CREATE TABLE foo (id int)", "CREATE", id="create"),
        pytest.param("ALTER TABLE foo ADD COLUMN y int", "ALTER", id="alter"),
        pytest.param("DROP TABLE foo", "DROP", id="drop"),
        pytest.param("TRUNCATE TABLE foo", "TRUNCATETABLE", id="truncate"),
        pytest.param(
            "WITH cte AS (INSERT INTO foo VALUES (1) RETURNING *) SELECT * FROM cte",
            "INSERT",
            id="write-inside-cte",
        ),
    ],
)
def test_scan_for_writes_detects_write_kind(sql, expected_kind):
    is_write, reason = read_only_guard.scan_for_writes(sql)
    if expected_kind is None:
        assert is_write is False
        assert "no write detected" in reason
    else:
        assert is_write is True
        assert f"proven write ({expected_kind})" in reason


def test_scan_for_writes_detects_write_in_second_of_multiple_statements():
    is_write, reason = read_only_guard.scan_for_writes("SELECT 1; INSERT INTO foo VALUES (1)")
    assert is_write is True
    assert "statement #2" in reason
    assert "INSERT" in reason


def test_scan_for_writes_list_of_read_only_statements():
    is_write, reason = read_only_guard.scan_for_writes(["SELECT 1", "SELECT 2"])
    assert is_write is False
    assert "no write detected" in reason


def test_scan_for_writes_detects_write_across_list_of_statements():
    is_write, reason = read_only_guard.scan_for_writes(["SELECT 1", "INSERT INTO foo VALUES (1)"])
    assert is_write is True
    assert "statement #2" in reason


def test_scan_for_writes_unparsable_sql_defers_to_read_only_transaction():
    is_write, reason = read_only_guard.scan_for_writes("SELECT * FROM foo WHERE x = 'unterminated")
    assert is_write is False
    assert "unparsable" in reason


def test_scan_for_writes_sqlglot_missing_defers_to_read_only_transaction(monkeypatch):
    monkeypatch.setattr(read_only_guard, "_SQLGLOT_AVAILABLE", False)
    is_write, reason = read_only_guard.scan_for_writes("INSERT INTO foo VALUES (1)")
    assert is_write is False
    assert "sqlglot not installed" in reason
