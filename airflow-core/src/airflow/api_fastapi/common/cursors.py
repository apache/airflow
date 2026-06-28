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
from sqlalchemy import and_, false, or_, true
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.sqltypes import Uuid

from airflow.api_fastapi.common.parameters import SortParam


def _b64url_decode_padded(token: str) -> bytes:
    padding = 4 - (len(token) % 4)
    if padding != 4:
        token = token + ("=" * padding)
    return base64.urlsafe_b64decode(token.encode("ascii"))


def _dialect_nulls_last(is_desc: bool, dialect: str) -> bool:
    """
    Where a plain ``ORDER BY col`` puts NULLs on this backend.

    PostgreSQL sorts NULLs last for ASC, first for DESC; MySQL and SQLite treat
    NULL as the lowest value, so NULLs come first for ASC and last for DESC.
    """
    return (not is_desc) if dialect == "postgresql" else is_desc


def _bounds(
    col: ColumnElement, value: Any, is_desc: bool, dialect: str
) -> tuple[ColumnElement[bool], ColumnElement[bool]]:
    """
    ``(non_strict, strict)`` keyset bounds matching the backend's native NULL placement.

    A NULL *value* means the cursor sits in the NULL block; otherwise a trailing
    NULL block (when NULLs sort last) is also admitted after a non-NULL cursor.
    The ``col IS NULL`` terms are vacuous for non-nullable columns.
    """
    nulls_last = _dialect_nulls_last(is_desc, dialect)
    if value is None:
        if nulls_last:
            return col.is_(None), false()
        return true(), col.is_not(None)
    ge = col <= value if is_desc else col >= value
    gt = col < value if is_desc else col > value
    if nulls_last:
        return or_(ge, col.is_(None)), or_(gt, col.is_(None))
    return ge, gt


def _nested_keyset_predicate(
    resolved: list[tuple[str, ColumnElement, bool]], values: list[Any], dialect: str
) -> ColumnElement[bool]:
    """
    Keyset predicate for rows strictly after the cursor in ``ORDER BY`` order.

    Uses nested ``and_(non-strict, or_(strict, ...))`` so leading sort keys use
    inclusive range bounds and inner branches use strict inequalities—friendly
    for composite index range scans. NULL placement follows each backend's
    native ordering for a plain ``ORDER BY`` (see :func:`_bounds`), so the
    ``ORDER BY`` stays a bare column and can still use an index.
    """
    n = len(resolved)
    _, col, is_desc = resolved[n - 1]
    _, inner = _bounds(col, values[n - 1], is_desc, dialect)
    for i in range(n - 2, -1, -1):
        _, col_i, is_desc_i = resolved[i]
        non_strict, strict = _bounds(col_i, values[i], is_desc_i, dialect)
        inner = and_(non_strict, or_(strict, inner))
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


_BACKWARD_PREFIX = "~"


def encode_cursor(row: Any, sort_param: SortParam) -> str:
    """
    Encode cursor token from the boundary row of a result set.

    The token is a URL-safe base64 encoding of a MessagePack list of sort-key
    values (no padding ``=``).
    """
    resolved = sort_param.get_resolved_columns()
    if not resolved:
        raise ValueError("SortParam has no resolved columns.")

    parts = [sort_param.row_value(row, attr_name) for attr_name, _col, _desc in resolved]
    payload = msgspec.msgpack.encode(parts)
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def make_backward_cursor(token: str) -> str:
    """Prefix a cursor token with the backward direction marker (``~``)."""
    return f"{_BACKWARD_PREFIX}{token}"


def parse_cursor(cursor: str) -> tuple[str, bool]:
    """
    Parse a raw cursor string into ``(token, is_backward)``.

    Strips the ``~`` prefix if present and returns whether the cursor
    represents a backward (previous-page) direction.
    """
    if cursor.startswith(_BACKWARD_PREFIX):
        return cursor[len(_BACKWARD_PREFIX) :], True
    return cursor, False


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


def apply_cursor_filter(
    statement: Select, token: str, sort_param: SortParam, dialect: str, *, is_backward: bool = False
) -> Select:
    """
    Apply a keyset pagination WHERE clause from a cursor token.

    For forward cursors the predicate selects rows strictly *after* the cursor
    in ORDER BY order.  When *is_backward* is True the ``is_desc`` flags are
    flipped so the predicate selects rows strictly *before* the cursor in the
    original sort order.  The caller is responsible for reversing the ORDER BY
    and the final result list when using a backward cursor.

    *dialect* is the backend dialect name (``session.get_bind().dialect.name``);
    the predicate matches that backend's native NULL placement, so the keyset
    ``ORDER BY`` stays a bare column and keeps using indexes.
    """
    raw_values = decode_cursor(token)

    resolved = sort_param.get_resolved_columns()
    if len(raw_values) != len(resolved):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cursor token does not match current query shape")

    parsed_values = [_coerce_value(col, val) for (_, col, _), val in zip(resolved, raw_values, strict=True)]

    if is_backward:
        resolved = [(name, col, not is_desc) for name, col, is_desc in resolved]

    return statement.where(_nested_keyset_predicate(resolved, parsed_values, dialect))
