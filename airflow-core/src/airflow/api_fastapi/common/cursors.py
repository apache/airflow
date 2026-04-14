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
Cursor-based (keyset) pagination helpers.

:meta private:
"""

from __future__ import annotations

import base64
import uuid as uuid_mod
from typing import Any

import msgspec
from fastapi import HTTPException, status
from sqlalchemy import and_, or_
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.sqltypes import Uuid

from airflow.api_fastapi.common.parameters import SortParam


def _b64url_decode_padded(token: str) -> bytes:
    padding = 4 - (len(token) % 4)
    if padding != 4:
        token = token + ("=" * padding)
    return base64.urlsafe_b64decode(token.encode("ascii"))


def _nonstrict_bound(col: ColumnElement, value: Any, is_desc: bool) -> ColumnElement[bool]:
    """Inclusive range edge on the leading column at each nesting level (``>=`` / ``<=``)."""
    return col <= value if is_desc else col >= value


def _strict_bound(col: ColumnElement, value: Any, is_desc: bool) -> ColumnElement[bool]:
    """Strict inequality for ``or_`` branches (``<`` / ``>``)."""
    return col < value if is_desc else col > value


def _nested_keyset_predicate(
    resolved: list[tuple[str, ColumnElement, bool]], values: list[Any]
) -> ColumnElement[bool]:
    """
    Keyset predicate for rows strictly after the cursor in ``ORDER BY`` order.

    Uses nested ``and_(non-strict, or_(strict, ...))`` so leading sort keys use
    inclusive range bounds and inner branches use strict inequalities—friendly
    for composite index range scans. Logically equivalent to an OR-of-prefix-
    equalities formulation.
    """
    n = len(resolved)
    _, col, is_desc = resolved[n - 1]
    inner: ColumnElement[bool] = _strict_bound(col, values[n - 1], is_desc)
    for i in range(n - 2, -1, -1):
        _, col_i, is_desc_i = resolved[i]
        inner = and_(
            _nonstrict_bound(col_i, values[i], is_desc_i),
            or_(_strict_bound(col_i, values[i], is_desc_i), inner),
        )
    return inner


def _coerce_value(column: ColumnElement, value: Any) -> Any:
    """Normalize decoded values for SQL bind parameters (e.g. UUID columns)."""
    if value is None or not isinstance(value, str):
        return value
    ctype = getattr(column, "type", None)
    if isinstance(ctype, Uuid):
        try:
            return uuid_mod.UUID(value)
        except ValueError:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid cursor token")
    return value


def encode_cursor(row: Any, sort_param: SortParam) -> str:
    """
    Encode cursor token from the boundary row of a result set.

    The token is a url-safe base64 encoding of a MessagePack list of sort-key
    values (no padding ``=``), so the cursor is compact and safe in query strings.
    Binary msgpack is not URL-safe by itself, so base64 is still required.
    """
    resolved = sort_param.get_resolved_columns()
    if not resolved:
        raise ValueError("SortParam has no resolved columns.")

    parts = [getattr(row, attr_name, None) for attr_name, _col, _desc in resolved]
    payload = msgspec.msgpack.encode(parts)
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_cursor(token: str) -> list[Any]:
    """Decode a cursor token to the list of sort-key values."""
    try:
        raw = _b64url_decode_padded(token)
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid cursor token")

    try:
        data: Any = msgspec.msgpack.decode(raw)
    except Exception:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid cursor token")

    if not isinstance(data, list):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid cursor token structure")

    return data


def apply_cursor_filter(statement: Select, cursor: str, sort_param: SortParam) -> Select:
    """
    Apply a keyset pagination WHERE clause from a cursor token.

    Uses nested ``and_(col <=/>= v, or_(col </> v, ...))`` so each leading sort
    key carries a range-friendly non-strict bound, with strict inequalities on
    the ``or_`` branches—aligned with common composite index range scans.
    """
    raw_values = decode_cursor(cursor)

    resolved = sort_param.get_resolved_columns()
    if len(raw_values) != len(resolved):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cursor token does not match current query shape")

    parsed_values = [_coerce_value(col, val) for (_, col, _), val in zip(resolved, raw_values, strict=True)]

    return statement.where(_nested_keyset_predicate(resolved, parsed_values))
