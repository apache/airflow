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
from fastapi import HTTPException
from sqlalchemy import column, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import ColumnElement

from airflow.providers.fab.auth_manager.api_fastapi.sorting import build_ordering


def _sql(expr) -> str:
    return str(expr.compile(dialect=postgresql.dialect()))


def test_build_ordering_returns_asc_by_default():
    allowed = {"name": column("name"), "id": column("id")}
    expr = build_ordering("name", allowed=allowed)

    assert isinstance(expr, ColumnElement)
    sql = _sql(expr)
    assert "name" in sql
    assert "ASC" in sql
    assert "DESC" not in sql


def test_build_ordering_desc_when_prefixed_with_dash():
    allowed = {"name": column("name"), "id": column("id")}
    expr = build_ordering("-id", allowed=allowed)

    sql = _sql(expr)
    assert "id" in sql
    assert "DESC" in sql


def test_build_ordering_supports_sql_expressions():
    # Ensure mapping can include arbitrary SQL expressions (e.g., lower(name))
    allowed = {"name_i": func.lower(column("name"))}
    expr = build_ordering("-name_i", allowed=allowed)

    sql = _sql(expr)
    assert "lower(" in sql
    assert "DESC" in sql


def test_build_ordering_raises_http_400_for_disallowed_key():
    allowed = {"name": column("name")}
    with pytest.raises(HTTPException) as ex:
        build_ordering("unknown", allowed=allowed)

    assert ex.value.status_code == 400
    assert "disallowed" in str(ex.value.detail) or "does not exist" in str(ex.value.detail)
