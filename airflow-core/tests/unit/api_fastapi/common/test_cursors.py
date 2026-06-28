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
from types import SimpleNamespace
from unittest.mock import MagicMock

import msgspec
import pytest
from fastapi import HTTPException
from sqlalchemy import Column, String, select

from airflow.api_fastapi.common.cursors import apply_cursor_filter, decode_cursor, encode_cursor
from airflow.api_fastapi.common.parameters import SortParam
from airflow.models.dagrun import DagRun
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

    def test_encode_cursor_with_column_form_to_replace_falls_back_to_row_attr(self):
        """Column-form ``to_replace`` should still allow cursor encoding via row attribute access."""
        sp = SortParam(["id", "run_after"], TaskInstance, {"run_after": DagRun.run_after})
        sp.set_value(["run_after"])

        row = SimpleNamespace(
            run_after="2026-06-04T10:00:00+00:00",
            id="019462ab-1234-5678-9abc-def012345678",
        )

        token = encode_cursor(row, sp)
        decoded = decode_cursor(token)
        assert decoded == [
            "2026-06-04T10:00:00+00:00",
            "019462ab-1234-5678-9abc-def012345678",
        ]

    def test_encode_cursor_column_form_to_replace_raises_when_attribute_absent(self):
        """
        ``encode_cursor`` must raise ``NotImplementedError`` (not silently encode ``None``)
        when a column-form ``to_replace`` key has no corresponding attribute on the row object.
        A ``None`` token would cause the next-page WHERE to compare against NULL and drop rows.
        """
        sp = SortParam(
            ["id", "data_interval_start"], TaskInstance, {"data_interval_start": DagRun.data_interval_start}
        )
        sp.set_value(["data_interval_start"])

        # Row without data_interval_start — TaskInstance does not expose this as an attribute.
        row = SimpleNamespace(id="019462ab-1234-5678-9abc-def012345678")

        with pytest.raises(NotImplementedError, match="data_interval_start"):
            encode_cursor(row, sp)

    def test_apply_cursor_filter_wrong_value_count(self):
        sp = self._make_sort_param_with_resolved_columns(["start_date"])
        token = _msgpack_cursor_token(["only-one-value"])

        with pytest.raises(HTTPException, match="does not match"):
            apply_cursor_filter(select(TaskInstance), token, sp, "sqlite")

    def test_apply_cursor_filter_ascending(self):
        sp = self._make_sort_param_with_resolved_columns(["start_date"])
        values = [
            datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            uuid.UUID("019462ab-1234-5678-9abc-def012345678"),
        ]
        token = _msgpack_cursor_token(values)

        stmt = apply_cursor_filter(select(TaskInstance), token, sp, "sqlite")
        sql = str(stmt)
        assert ">" in sql

    def test_apply_cursor_filter_descending(self):
        sp = self._make_sort_param_with_resolved_columns(["-start_date"])
        values = [
            datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            uuid.UUID("019462ab-1234-5678-9abc-def012345678"),
        ]
        token = _msgpack_cursor_token(values)

        stmt = apply_cursor_filter(select(TaskInstance), token, sp, "sqlite")
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

    def test_apply_cursor_filter_null_value_does_not_raise(self):
        """Cursor tokens with None values (nullable sort columns) must not crash.

        When order_by=rendered_map_index and no map_index_template is set,
        _rendered_map_index is NULL for all rows.  The cursor encodes None and
        the next-page filter must use IS NULL / IS NOT NULL instead of >= None.
        """
        sp = SortParam(
            ["_rendered_map_index", "map_index", "id"],
            TaskInstance,
        )
        sp.set_value(["_rendered_map_index", "map_index"])
        token = _msgpack_cursor_token([None, 49, "019462ab-1234-5678-9abc-def012345678"])

        # Should not raise ArgumentError from SQLAlchemy; the NULL boundary is
        # expressed with IS [NOT] NULL rather than a comparison against NULL.
        stmt = apply_cursor_filter(select(TaskInstance), token, sp, "sqlite")
        sql = str(stmt)
        assert "IS NOT NULL" in sql


class TestKeysetPaginationNullableColumn:
    """End-to-end: cursor pagination over a nullable sort column returns every row exactly once.

    When the keyset predicate and the ORDER BY disagree on where NULLs fall, one side
    of the NULL/non-NULL boundary is silently dropped. The predicate matches each
    backend's native NULL placement so the ORDER BY stays a bare (indexable) column.
    """

    @staticmethod
    def _seed_session(rows):
        """Build an in-memory model with one nullable column and seed it with ``(id, val)`` rows."""
        from sqlalchemy import Integer, create_engine
        from sqlalchemy.orm import Session, declarative_base

        base = declarative_base()

        class Item(base):
            __tablename__ = "keyset_items"
            id = Column(Integer, primary_key=True)
            val = Column(String, nullable=True)

        engine = create_engine("sqlite://")
        base.metadata.create_all(engine)
        session = Session(engine)
        session.add_all([Item(id=i, val=v) for i, v in rows])
        session.commit()
        return Item, session

    @staticmethod
    def _walk_forward(session, model, sort, page_size):
        collected: list[int] = []
        token = None
        for _ in range(50):  # guard against an infinite paging loop
            stmt = sort.to_orm(select(model)).limit(page_size)
            if token is not None:
                stmt = apply_cursor_filter(stmt, token, sort, "sqlite")
            rows = list(session.scalars(stmt))
            if not rows:
                break
            collected.extend(r.id for r in rows)
            token = encode_cursor(rows[-1], sort)
        return collected

    @pytest.mark.parametrize(
        ("order_by", "expected_order"),
        [
            pytest.param(["val"], [1, 2, 3, 4, 5, 6], id="ascending-nulls-first"),
            pytest.param(["-val"], [6, 5, 4, 3, 2, 1], id="descending-nulls-last"),
        ],
    )
    def test_forward_pagination_returns_all_rows(self, order_by, expected_order):
        model, session = self._seed_session([(1, None), (2, None), (3, None), (4, "a"), (5, "b"), (6, "c")])
        try:
            sort = SortParam(["val"], model)
            sort.set_value(order_by)
            collected = self._walk_forward(session, model, sort, page_size=2)
        finally:
            session.close()

        assert sorted(collected) == [1, 2, 3, 4, 5, 6], f"rows dropped/duplicated: {collected}"
        assert len(collected) == len(set(collected)), f"rows duplicated: {collected}"
        assert collected == expected_order

    def test_multiple_nullable_columns_no_rows_dropped(self):
        """Two nullable sort columns: every NULL/non-NULL combination paged exactly once."""
        from sqlalchemy import Integer, create_engine
        from sqlalchemy.orm import Session, declarative_base

        base = declarative_base()

        class Pair(base):
            __tablename__ = "keyset_pairs"
            id = Column(Integer, primary_key=True)
            a = Column(String, nullable=True)
            b = Column(String, nullable=True)

        engine = create_engine("sqlite://")
        base.metadata.create_all(engine)
        rows = [
            (1, None, None),
            (2, None, "y"),
            (3, "p", None),
            (4, "p", "y"),
            (5, "q", None),
            (6, "q", "z"),
        ]
        with Session(engine) as session:
            session.add_all([Pair(id=i, a=a, b=b) for i, a, b in rows])
            session.commit()
            sort = SortParam(["a", "b"], Pair)
            sort.set_value(["a", "b"])
            collected = self._walk_forward(session, Pair, sort, page_size=2)

        assert sorted(collected) == [1, 2, 3, 4, 5, 6], f"rows dropped/duplicated: {collected}"
        assert len(collected) == len(set(collected)), f"rows duplicated: {collected}"

    def test_nullable_column_without_nulls_unaffected(self):
        """The common case (a nullable column that happens to hold no NULLs) still pages correctly."""
        model, session = self._seed_session([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")])
        try:
            sort = SortParam(["val"], model)
            sort.set_value(["val"])
            collected = self._walk_forward(session, model, sort, page_size=2)
        finally:
            session.close()

        assert collected == [1, 2, 3, 4, 5]
