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
"""Tests for ``SASCursor`` and ``SASConnection``: the DB-API objects."""

from __future__ import annotations

import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from airflow.providers.sas.driver.metadata import SASMetadata

from unit.sas.fakes import (
    COLUMN_FORMATS,
    COLUMN_TYPES,
    _cursor,
    _live_connection,
    _raw_frame,
)


def test_execute_returns_plain_python_rows_with_none_for_missing():
    cursor = _cursor()
    typed = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )
    with patch.object(cursor, "_execute_query", return_value=typed):
        cursor.execute("select * from mylib.orders")

    rows = cursor.fetchall()
    assert cursor.rowcount == 5
    assert [d[0] for d in cursor.description] == list(typed.columns)
    assert rows[0] == (
        "Alice",
        1.5,
        datetime.date(2020, 1, 1),
        datetime.datetime(1960, 1, 1),
        None,
    )
    assert rows[1] == ("Bob", None, None, None, None)
    assert all(type(v).__module__ in ("builtins", "datetime") for row in rows for v in row)


def test_select_script_carries_outobs_limit():
    scripts: list[str] = []

    def _capture(script, with_output=False):
        scripts.append(script)

    cursor = _cursor(limit=1000)
    with (
        patch.object(cursor._job, "_run_script", side_effect=_capture),
        patch.object(cursor._job, "_read_sas7bdat", return_value=pd.DataFrame({"A": [1.0]})),
    ):
        cursor.execute("select * from mylib.orders")
        cursor.fetch_dataframe("select * from mylib.orders", limit=5)
        cursor.fetch_dataframe("select * from mylib.orders", limit=0)

    assert "proc sql outobs=1000;" in scripts[0]
    assert "libname mylib '/data/mylib';" in scripts[0]
    assert "proc sql outobs=5;" in scripts[1]
    assert "proc sql;" in scripts[2]
    assert "outobs" not in scripts[2]


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({}, True),
        ({"transport_active": False}, False),
        ({"has_transport": False}, False),
        ({"sftp_closed": True}, False),
    ],
)
def test_connection_is_active(state, expected):
    assert _live_connection(**state).is_active() is expected


def test_cursor_passes_compress_to_the_select_script():
    scripts: list[str] = []
    cursor = _cursor(sas_compress="yes")
    with (
        patch.object(
            cursor._job,
            "_run_script",
            side_effect=lambda script, with_output=False: scripts.append(script),
        ),
        patch.object(cursor._job, "_read_sas7bdat", return_value=pd.DataFrame({"A": [1.0]})),
    ):
        cursor.execute("select * from mylib.x")
    assert "create table out.result (compress=yes) as" in scripts[0]
