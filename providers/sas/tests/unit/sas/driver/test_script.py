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
"""Tests for ``SASScript`` and ``sas_literal``: the ``PROC SQL`` programs the hook writes."""

from __future__ import annotations

import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from airflow.providers.sas.driver.script import SASPage, SASScript, sas_literal


# ---------------------------------------------------------------------------
# Compression of the result on the SAS side and zlib on the SSH transport
# ---------------------------------------------------------------------------
def test_query_script_compress_option():
    plain = SASScript({}).query("select 1", "/o")
    assert "create table out.result as" in plain
    for value in ("yes", "binary"):
        script = SASScript({}, compress=value).query("select 1", "/o")
        assert f"create table out.result (compress={value}) as" in script
    assert SASScript({}, compress="no").query("select 1", "/o") == plain


# ---------------------------------------------------------------------------
# Bind parameters are SAS literals
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("ABC", "'ABC'"),
        ("O'Brien", "'O''Brien'"),
        ("&macro", "'&macro'"),
        (True, "1"),
        (False, "0"),
        (42, "42"),
        (np.int64(7), "7"),
        (1.5, "1.5"),
        (np.float64(2.25), "2.25"),
        (float("nan"), "."),
        (Decimal("12.30"), "12.30"),
        (datetime.date(2026, 10, 1), "'01OCT2026'd"),
        (datetime.datetime(2026, 10, 20, 9, 5, 7), "'20OCT2026:09:05:07'dt"),
        (
            datetime.datetime(2026, 10, 20, 9, 5, 7, 120),
            "'20OCT2026:09:05:07.000120'dt",
        ),
        (pd.Timestamp("2026-01-02 03:04:05"), "'02JAN2026:03:04:05'dt"),
    ],
)
def test_sas_literal(value, expected):
    assert sas_literal(value) == expected


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (None, TypeError),
        (object(), TypeError),
        (b"raw", TypeError),
        (float("inf"), ValueError),
        (Decimal("-Infinity"), ValueError),
    ],
)
def test_sas_literal_refuses_values_without_a_literal(value, error):
    with pytest.raises(error):
        sas_literal(value)


def test_bind_quotes_positional_and_named_values():
    bind = SASScript.bind

    assert (
        bind("select * from t where a = %s and b = %s", ("x'; drop", 3))
        == "select * from t where a = 'x''; drop' and b = 3"
    )
    assert (
        bind("select * from t where d >= %(since)s", {"since": datetime.date(2026, 1, 1)})
        == "select * from t where d >= '01JAN2026'd"
    )
    assert bind("select * from t where a like 'A%%' and b = %s", ["B"]) == (
        "select * from t where a like 'A%' and b = 'B'"
    )
    assert bind("select * from t where a like 'A%'", None) == ("select * from t where a like 'A%'"), (
        "without parameters the SQL is left untouched"
    )


# ---------------------------------------------------------------------------
# One LIBNAME per library, quoted paths
# ---------------------------------------------------------------------------
def test_query_over_two_libraries_assigns_both():
    script = SASScript({"mylib": "/data/mylib", "otherlib": "/data/otherlib"})

    libnames = script.libnames(
        "select * from MYLIB.a a join otherlib.b b on a.id = b.id "
        "join mylib.c c on c.id = a.id join dictionary.tables t on 1=1"
    )
    comma_join = script.libnames("select a.id, b.* from mylib.a(obs=1) a, otherlib.b(obs=1) b")

    assert libnames == (
        ("mylib", "/data/mylib"),
        ("otherlib", "/data/otherlib"),
    )
    assert comma_join == libnames, "aliases a. and b. are not libraries"
    assert script.query("select * from mylib.a, otherlib.b", "/o").startswith(
        "libname mylib '/data/mylib';\nlibname otherlib '/data/otherlib';\nlibname out '/o';\n"
    )


def test_libname_paths_are_sas_strings():
    script = SASScript({"lib": "/data/o'neil"})

    query = script.query("select * from lib.t", "/out dir")
    assert "libname lib '/data/o''neil';" in query
    assert "libname out '/out dir';" in query
    assert script.statement("delete from lib.t") == (
        "libname lib '/data/o''neil';\n\nproc sql;\ndelete from lib.t;\nquit;\n"
    )
    assert script.statement("delete from t") == "proc sql;\ndelete from t;\nquit;\n"


# ---------------------------------------------------------------------------
# Query detection past leading comments
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("sql", "is_query"),
    [
        ("select 1", True),
        ("  WITH x AS (select 1) select * from x", True),
        ("/* extract */\nselect * from t", True),
        ("-- extract\n/* multi\nline */ (select * from t)", True),
        ("((select 1))", True),
        ("delete from t", False),
        ("/* select */ delete from t", False),
        ("-- select\nupdate t set a = 1", False),
    ],
)
def test_is_query_skips_leading_comments(sql, is_query):
    assert SASScript.is_query(sql) is is_query


# ---------------------------------------------------------------------------
# Async path: SASAsyncJob, SASAsyncConnection, SASHook.aget_*
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Paging: LIMIT n OFFSET m (GenericTransfer) served from one SAS run
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "select * from mylib.t LIMIT 10 OFFSET 20",
            SASPage("select * from mylib.t", 10, 20),
        ),
        (
            "select * from mylib.t limit 10 offset 0;",
            SASPage("select * from mylib.t", 10, 0),
        ),
        (
            "select * from mylib.t\n LIMIT 5 OFFSET 5 ;  ",
            SASPage("select * from mylib.t", 5, 5),
        ),
        ("select * from mylib.t order by id", None),
        ("select * from mylib.t LIMIT 10", None),
        ("select * from mylib.t where offset = 3", None),
    ],
)
def test_page_is_a_trailing_limit_offset(sql, expected):
    assert SASScript.page(sql) == expected
