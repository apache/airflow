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

from airflow.providers.common.ai.utils.query_results import (
    _SCHEMA_SAMPLE_SIZE,
    build_query_result,
    build_schema_result,
)


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


@pytest.mark.enable_redact
def test_a_secret_that_json_escapes_is_masked_in_the_rows(register_secret):
    secret = register_secret('db-pa"ss-91c3')

    result = _build(["user", "password"], [["admin", secret]])

    assert result["rows"] == [["admin", "***"]]


def _schema(columns, *, max_columns=100, max_result_bytes=65_536, name_contains=None) -> dict:
    return json.loads(
        build_schema_result(
            columns,
            max_columns=max_columns,
            max_result_bytes=max_result_bytes,
            name_contains=name_contains,
        )
    )


def _cols(n: int, *, type_: str = "VARCHAR", prefix: str = "col") -> list[dict[str, str]]:
    return [{"name": f"{prefix}_{i}", "type": type_} for i in range(n)]


class TestSchemaResult:
    def test_small_table_returns_every_column(self):
        cols = _cols(3)
        assert _schema(cols) == {"columns": cols, "column_count": 3}

    def test_name_contains_filters_case_insensitively(self):
        cols = [
            {"name": "CustomerId", "type": "INT"},
            {"name": "amount", "type": "NUMERIC"},
            {"name": "customer_name", "type": "VARCHAR"},
        ]
        data = _schema(cols, name_contains="customer")
        assert data["columns"] == [cols[0], cols[2]]
        assert data["column_count"] == 2
        assert data["name_contains"] == "customer"
        assert data["total_columns"] == 3
        assert "truncated" not in data

    def test_empty_name_contains_is_treated_as_no_filter(self):
        cols = _cols(3)
        assert _schema(cols, name_contains="") == {"columns": cols, "column_count": 3}

    def test_name_contains_with_no_matches_guides_without_erroring(self):
        data = _schema(_cols(5), name_contains="zzz")
        assert data["columns"] == []
        assert data["column_count"] == 0
        assert data["name_contains"] == "zzz"
        assert data["total_columns"] == 5
        assert "error" not in data
        assert "all 5 columns" in data["hint"]

    def test_no_match_on_a_wide_table_does_not_point_back_at_the_filter(self):
        """When the full list would itself summarize, the hint must not say 'call without it'."""
        data = _schema(_cols(300), max_columns=100, name_contains="zzz")
        assert data["columns"] == []
        assert data["total_columns"] == 300
        assert "without name_contains" not in data["hint"]
        assert "different substring" in data["hint"]

    def test_too_many_columns_are_summarized_not_listed(self):
        raw = build_schema_result(_cols(250), max_columns=100, max_result_bytes=65_536)
        data = json.loads(raw)
        assert data["truncated"] is True
        assert data["truncated_by"] == "max_columns"
        assert data["column_count"] == 250
        assert "columns" not in data
        assert data["type_histogram"] == {"VARCHAR": 250}
        # A small fixed preview, not the first max_columns, so the summary is far smaller than the
        # list it replaces rather than a near-identical prefix with the tail dropped.
        assert len(data["sample_columns"]) == _SCHEMA_SAMPLE_SIZE
        assert data["sample_columns"][0] == {"name": "col_0", "type": "VARCHAR"}
        assert len(raw.encode("utf-8")) < len(json.dumps(_cols(250)).encode("utf-8"))
        assert "name_contains" in data["hint"]

    @pytest.mark.parametrize("budget", [500, 2000, 65_536])
    def test_summary_never_exceeds_the_byte_budget(self, budget):
        """The contract the docstring promises: a summary that fits the budget stays within it."""
        cols = [{"name": f"customer_attribute_{i}", "type": "VARCHAR(255)"} for i in range(500)]
        raw = build_schema_result(cols, max_columns=100, max_result_bytes=budget)
        assert json.loads(raw)["truncated"] is True
        assert len(raw.encode("utf-8")) <= budget

    def test_histogram_is_dropped_but_the_sample_is_still_filled(self):
        """A histogram too big for the budget must not strip the preview off every column too."""
        # Many distinct long type names make the histogram large. The budget holds the core plus a
        # few short-named columns but not the histogram.
        cols = [{"name": f"c{i}", "type": f"CUSTOM_STRUCT_TYPE_NUMBER_{i:04d}"} for i in range(200)]
        data = _schema(cols, max_columns=100, max_result_bytes=600)
        assert data["truncated"] is True
        assert "type_histogram" not in data
        assert len(data["sample_columns"]) >= 1

    def test_long_names_blow_the_byte_budget_despite_few_columns(self):
        cols = [{"name": "x" * 500, "type": "VARCHAR"} for _ in range(10)]
        data = _schema(cols, max_columns=100, max_result_bytes=512)
        assert data["truncated"] is True
        assert data["truncated_by"] == "max_result_bytes"

    def test_max_columns_takes_precedence_when_both_bounds_are_exceeded(self):
        data = _schema(_cols(300), max_columns=100, max_result_bytes=256)
        assert data["truncated_by"] == "max_columns"

    def test_summary_hint_is_worded_from_the_limit_it_hit(self):
        over_count = _schema(_cols(250), max_columns=100)
        assert "250 columns, more than the max_columns limit of 100" in over_count["hint"]

        # One column whose name alone blows a tiny budget: the count fits, the bytes do not, so the
        # wording must say so and read "1 column" (not "1 columns").
        over_bytes = _schema([{"name": "x" * 500, "type": "VARCHAR"}], max_columns=100, max_result_bytes=200)
        assert over_bytes["truncated_by"] == "max_result_bytes"
        assert "1 column that did not fit max_result_bytes" in over_bytes["hint"]
        assert "1 columns" not in over_bytes["hint"]

    def test_filtered_result_still_too_wide_is_summarized(self):
        cols = [{"name": f"customer_{i}", "type": "VARCHAR"} for i in range(200)]
        data = _schema(cols, max_columns=100, name_contains="customer")
        assert data["truncated"] is True
        assert data["truncated_by"] == "max_columns"
        assert data["name_contains"] == "customer"
        assert data["total_columns"] == 200
        assert "more specific name_contains" in data["hint"]

    def test_type_histogram_is_capped_and_ordered_by_count(self):
        cols: list[dict[str, str]] = []
        for t in range(30):
            cols.extend({"name": f"c_{t}_{i}", "type": f"T{t:02d}"} for i in range(t + 1))
        histogram = _schema(cols, max_columns=1)["type_histogram"]
        keys = list(histogram.keys())
        assert len(histogram) == 21  # 20 most common types plus the folded remainder
        assert keys[0] == "T29"  # highest count first
        assert keys[-1] == "(other)"
        assert histogram["(other)"] == sum(range(1, 11))  # T00..T09 => 1 + 2 + ... + 10

    def test_summary_core_survives_a_pathologically_small_budget(self):
        data = _schema(_cols(300), max_columns=100, max_result_bytes=1)
        assert data["column_count"] == 300
        assert data["truncated"] is True
        assert data["truncated_by"] == "max_columns"
        assert data["sample_columns"] == []
        assert "type_histogram" not in data
        assert data["hint"]
