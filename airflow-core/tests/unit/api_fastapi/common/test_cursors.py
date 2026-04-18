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

import base64
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import msgspec
import pytest
from fastapi import HTTPException
from sqlalchemy import select

from airflow.api_fastapi.common.cursors import apply_cursor_filter, decode_cursor, encode_cursor
from airflow.api_fastapi.common.parameters import SortParam
from airflow.models.taskinstance import TaskInstance


def _msgpack_cursor_token(payload: object) -> str:
    """Match production: msgpack + base64url without padding."""
    return base64.urlsafe_b64encode(msgspec.msgpack.encode(payload)).decode("ascii").rstrip("=")


class TestCursorPagination:
    """Tests for cursor-based pagination helpers."""

    def _make_sort_param_with_resolved_columns(self, order_by_values=None):
        """Build a SortParam for TaskInstance and resolve its columns."""
        sp = SortParam(["id", "start_date", "map_index"], TaskInstance)
        sp.set_value(order_by_values or ["map_index"])
        sp.to_orm(select(TaskInstance))
        return sp

    def test_encode_decode_cursor_roundtrip(self):
        sp = self._make_sort_param_with_resolved_columns(["start_date"])
        row = MagicMock(spec=["start_date", "id"])
        row.start_date = "2024-01-15T10:00:00+00:00"
        row.id = "019462ab-1234-5678-9abc-def012345678"

        token = encode_cursor(row, sp)
        decoded = decode_cursor(token)

        assert decoded == [
            "2024-01-15T10:00:00+00:00",
            "019462ab-1234-5678-9abc-def012345678",
        ]

    def test_decode_cursor_invalid_base64(self):
        with pytest.raises(HTTPException, match="Invalid cursor token"):
            decode_cursor("not-valid-base64!!!")

    def test_decode_cursor_invalid_msgpack(self):
        token = base64.urlsafe_b64encode(b"not-msgpack").decode().rstrip("=")
        with pytest.raises(HTTPException, match="Invalid cursor token"):
            decode_cursor(token)

    def test_decode_cursor_not_a_list(self):
        token = _msgpack_cursor_token({"wrong": "type"})
        with pytest.raises(HTTPException, match="Invalid cursor token structure"):
            decode_cursor(token)

    def test_encode_cursor_works_without_prior_to_orm(self):
        """get_resolved_columns now lazily resolves, so to_orm is no longer required before encode."""
        sp = SortParam(["id"], TaskInstance)
        sp.set_value(["id"])
        row = MagicMock(spec=["id"])
        row.id = "019462ab-1234-5678-9abc-def012345678"
        token = encode_cursor(row, sp)
        decoded = decode_cursor(token)
        assert decoded == ["019462ab-1234-5678-9abc-def012345678"]

    def test_apply_cursor_filter_wrong_value_count(self):
        sp = self._make_sort_param_with_resolved_columns(["start_date"])
        token = _msgpack_cursor_token(["only-one-value"])

        with pytest.raises(HTTPException, match="does not match"):
            apply_cursor_filter(select(TaskInstance), token, sp)

    def test_apply_cursor_filter_ascending(self):
        sp = self._make_sort_param_with_resolved_columns(["start_date"])
        values = [
            datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            uuid.UUID("019462ab-1234-5678-9abc-def012345678"),
        ]
        token = _msgpack_cursor_token(values)

        stmt = apply_cursor_filter(select(TaskInstance), token, sp)
        sql = str(stmt)
        assert ">" in sql

    def test_apply_cursor_filter_descending(self):
        sp = self._make_sort_param_with_resolved_columns(["-start_date"])
        values = [
            datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            uuid.UUID("019462ab-1234-5678-9abc-def012345678"),
        ]
        token = _msgpack_cursor_token(values)

        stmt = apply_cursor_filter(select(TaskInstance), token, sp)
        sql = str(stmt)
        assert "<" in sql

    def test_sort_param_get_resolved_columns(self):
        sp = self._make_sort_param_with_resolved_columns(["start_date"])
        resolved = sp.get_resolved_columns()

        assert len(resolved) == 2
        assert resolved[0][0] == "start_date"
        assert resolved[0][2] is False
        assert resolved[1][0] == "id"
        assert resolved[1][2] is False

    def test_sort_param_get_resolved_columns_descending(self):
        sp = self._make_sort_param_with_resolved_columns(["-start_date"])
        resolved = sp.get_resolved_columns()

        assert len(resolved) == 2
        assert resolved[0][0] == "start_date"
        assert resolved[0][2] is True
        assert resolved[1][0] == "id"
        assert resolved[1][2] is True

    def test_sort_param_pk_not_duplicated_when_sorting_by_id(self):
        sp = self._make_sort_param_with_resolved_columns(["id"])
        resolved = sp.get_resolved_columns()

        assert len(resolved) == 1
        assert resolved[0][0] == "id"
