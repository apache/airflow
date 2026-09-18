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

import datetime
import json
from decimal import Decimal

import pytest

from airflow.providers.common.ai.utils.query_results import build_query_result


def _build(columns, rows, *, max_rows=50, max_result_bytes=65_536, more=False, total=None) -> dict:
    return json.loads(
        build_query_result(
            columns,
            rows,
            max_rows=max_rows,
            max_result_bytes=max_result_bytes,
            more_rows_available=more,
            total_rows=total,
        )
    )


class TestShape:
    def test_columns_are_named_once(self):
        data = _build(["id", "name"], [(1, "a"), (2, "b")])
        assert data == {"columns": ["id", "name"], "rows": [[1, "a"], [2, "b"]], "row_count": 2}

    def test_columnar_is_smaller_than_a_dict_per_row(self):
        """The saving that motivates the shape: on a wide table the repeated column
        names, not the values, are the bulk of the payload."""
        columns = [f"column_name_{i}" for i in range(500)]
        rows = [tuple(range(500))] * 10

        columnar = build_query_result(
            columns, rows, max_rows=50, max_result_bytes=10**9, more_rows_available=False
        )
        per_row_dicts = json.dumps(
            {"rows": [dict(zip(columns, row)) for row in rows], "count": 10},
            separators=(",", ":"),
        )
        # The header is serialized once rather than once per row, so the gap widens
        # with row count; at 10 rows it is already a factor of three.
        assert len(columnar) * 3 < len(per_row_dicts)

    def test_non_json_types_are_stringified(self):
        data = _build(
            ["at", "amount"],
            [(datetime.datetime(2026, 8, 8, 12, 0), Decimal("1.50"))],
        )
        assert data["rows"] == [["2026-08-08 12:00:00", "1.50"]]


class TestTotalRows:
    def test_included_when_known(self):
        assert _build(["id"], [(1,)], total=97)["total_rows"] == 97

    def test_absent_when_the_driver_reports_none(self):
        assert "total_rows" not in _build(["id"], [(1,)], total=None)


class TestByteBudget:
    def test_budget_is_accounted_exactly(self):
        """The per-row measurement uses the same serializer and separators as the final
        dump, so the emitted columns-plus-rows must land inside the budget."""
        columns = [f"col_{i}" for i in range(20)]
        rows = [tuple(f"value_{i}" for i in range(20))] * 100
        budget = 4096

        data = _build(columns, rows, max_rows=100, max_result_bytes=budget)
        measured = len(json.dumps(data["columns"], separators=(",", ":"))) + len(
            json.dumps(data["rows"], separators=(",", ":"))
        )
        row_size = len(json.dumps(list(rows[0]), separators=(",", ":")))

        # Never over budget (the +2 is the rows array's own brackets, which the per-row
        # accounting does not charge for) and never more than one unplaced row under it.
        assert measured <= budget + 2
        assert measured > budget - row_size
        assert data["truncated_by"] == "max_result_bytes"

    def test_max_rows_wins_when_it_bites_first(self):
        data = _build(["id"], [(1,)], more=True)
        assert data["truncated"] is True
        assert data["truncated_by"] == "max_rows"

    def test_a_single_oversized_row_yields_a_narrowing_hint(self):
        data = _build(["blob"], [("x" * 400,)], max_result_bytes=100)
        assert data["rows"] == []
        assert data["truncated_by"] == "max_result_bytes"
        assert "first row alone exceeds max_result_bytes (100)" in data["hint"]

    def test_an_oversized_row_stops_the_result_rather_than_being_skipped(self):
        """Packing later rows around a wide one would hand the agent a prefix with a
        hole in it. Stopping keeps 'rows 1..n' meaning what it says."""
        rows = [("small",), ("x" * 5000,), ("small",), ("small",)]

        data = _build(["blob"], rows, max_result_bytes=1000)
        assert data["rows"] == [["small"]]
        assert data["truncated_by"] == "max_result_bytes"

    def test_partial_byte_truncation_still_guides_the_agent(self):
        """The partial case is the common one; it used to carry no hint at all, so the
        agent saw a short result with no reason to change its query."""
        rows = [("small",), ("x" * 5000,), ("small",)]

        data = _build(["blob"], rows, max_result_bytes=1000)
        assert "Stopped after 1 row:" in data["hint"]
        assert "Select fewer columns" in data["hint"]

    def test_budget_counts_bytes_not_escape_sequences(self):
        """With ensure_ascii the budget charges six characters per CJK character, so a
        Japanese result would be truncated several times earlier than an English one
        carrying the same information."""
        rows = [("日本語のテキスト",)] * 40

        data = _build(["text"], rows, max_result_bytes=2048)
        assert data["rows"][0] == ["日本語のテキスト"]
        # Three bytes per character, not the six an escaped \uXXXX would cost.
        assert data["row_count"] > 40 / 2

    def test_reported_size_is_bytes_for_non_ascii(self):
        rows = [("é" * 200,)]
        data = _build(["text"], rows, max_result_bytes=300)
        # 400 bytes of payload cannot fit a 300-byte budget, though it is 200 characters.
        assert data["rows"] == []

    def test_columns_over_budget_report_the_shape_without_the_names(self):
        columns = [f"column_name_{i}" for i in range(3000)]
        data = _build(columns, [tuple(range(3000))], max_result_bytes=512, total=9)

        assert "columns" not in data
        assert data["column_count"] == 3000
        assert data["row_count"] == 0
        assert data["total_rows"] == 9
        assert "3000 columns" in data["hint"]

    @pytest.mark.parametrize("max_result_bytes", [0, -1])
    def test_non_positive_budget_returns_no_rows_rather_than_raising(self, max_result_bytes):
        data = _build(["id"], [(1,)], max_result_bytes=max_result_bytes)
        assert data["row_count"] == 0

    def test_empty_result_is_not_reported_as_truncated(self):
        data = _build(["id", "name"], [])
        assert data == {"columns": ["id", "name"], "rows": [], "row_count": 0}
