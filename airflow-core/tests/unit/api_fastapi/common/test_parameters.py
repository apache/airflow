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

import re
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Annotated

import pytest
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import select

from airflow.api_fastapi.common.parameters import (
    FilterParam,
    NullableDatetimeRangeFilter,
    RangeFilter,
    SortParam,
    _AssetDependencyFilter,
    _ConsumingAssetFilter,
    _escape_like_pattern,
    _OwnersFilter,
    _PrefixPatternParam,
    _PrefixSearchParam,
    _SearchParam,
    _TaskDisplayNamePrefixPatternParam,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.models import DagModel, DagRun, Log
from airflow.models.errors import ParseImportError
from airflow.models.taskinstance import TaskInstance


class TestFilterParam:
    def test_filter_param_factory_description(self):
        app = FastAPI()  # Create a FastAPI app to test OpenAPI generation
        expected_descriptions = {
            "dag_id": "Filter by Dag ID Description",
            "task_id": "Filter by Task ID Description",
            "map_index": None,  # No description for map_index
            "run_id": "Filter by Run ID Description",
        }

        @app.get("/test")
        def test_route(
            dag_id: Annotated[
                FilterParam[str | None],
                Depends(
                    filter_param_factory(Log.dag_id, str | None, description="Filter by Dag ID Description")
                ),
            ],
            task_id: Annotated[
                FilterParam[str | None],
                Depends(
                    filter_param_factory(Log.task_id, str | None, description="Filter by Task ID Description")
                ),
            ],
            map_index: Annotated[
                FilterParam[int | None],
                Depends(filter_param_factory(Log.map_index, int | None)),
            ],
            run_id: Annotated[
                FilterParam[str | None],
                Depends(
                    filter_param_factory(
                        DagRun.run_id, str | None, description="Filter by Run ID Description"
                    )
                ),
            ],
        ):
            return {"message": "test"}

        # Get the OpenAPI spec
        openapi_spec = app.openapi()

        # Check if the description is in the parameters
        parameters = openapi_spec["paths"]["/test"]["get"]["parameters"]
        for param_name, expected_description in expected_descriptions.items():
            param = next((p for p in parameters if p.get("name") == param_name), None)
            assert param is not None, f"{param_name} parameter not found in OpenAPI"

            if expected_description is None:
                assert "description" not in param, (
                    f"Description should not be present in {param_name} parameter"
                )
            else:
                assert "description" in param, f"Description not found in {param_name} parameter"
                assert param["description"] == expected_description, (
                    f"Expected description '{expected_description}', got '{param['description']}'"
                )


class TestSortParam:
    def test_sort_param_max_number_of_filers(self):
        param = SortParam([], None, None)
        n_filters = param.MAX_SORT_PARAMS + 1
        param.value = [f"filter_{i}" for i in range(n_filters)]

        with pytest.raises(
            HTTPException,
            match=re.escape(
                f"400: Ordering with more than {param.MAX_SORT_PARAMS} parameters is not allowed. Provided: {param.value}"
            ),
        ):
            param.to_orm(None)

    def test_aliased_sort_resolves_row_value_via_to_replace(self):
        """
        Cursor encoding must read the sort value through ``to_replace`` when the
        user-facing sort name is an alias (e.g. ``dag_run_id`` → ``run_id``). A naive
        ``getattr(row, user_facing_name)`` would silently yield ``None`` and break
        keyset pagination on the next page.
        """
        param = SortParam(["id", "run_id"], DagRun, {"dag_run_id": "run_id"}).set_value(["dag_run_id"])
        attr_names = [name for name, _col, _desc in param.get_resolved_columns()]
        assert attr_names == ["dag_run_id", "id"]

        row = SimpleNamespace(run_id="manual__2026-04-22", id=42)
        assert param.row_value(row, "dag_run_id") == "manual__2026-04-22"
        assert param.row_value(row, "id") == 42

    def test_row_value_column_form_to_replace_resolves_via_row_attribute(self):
        """
        Column-form ``to_replace`` resolves through the primary model's attribute so
        association proxies (e.g. ``TaskInstance.run_after``) are usable for cursor encoding.
        """
        param = SortParam(["dag_id"], DagModel, {"last_run_state": DagRun.state}).set_value(
            ["last_run_state"]
        )
        row = SimpleNamespace(id="test_dag", last_run_state="success")
        assert param.row_value(row, "last_run_state") == "success"

    def test_row_value_column_form_to_replace_raises_when_attribute_absent(self):
        """
        Column-form ``to_replace`` must raise ``NotImplementedError`` (not return ``None``)
        when the primary model exposes no such attribute. A ``None`` cursor token would cause
        the next-page ``WHERE`` to compare against ``NULL`` and silently drop rows.
        """
        param = SortParam(
            ["dag_id"], DagModel, {"data_interval_start": DagRun.data_interval_start}
        ).set_value(["data_interval_start"])
        row = SimpleNamespace(id="test_dag")  # deliberately no data_interval_start attribute
        with pytest.raises(NotImplementedError, match="data_interval_start"):
            param.row_value(row, "data_interval_start")

    def test_primary_key_is_not_duplicated_when_alias_maps_to_pk(self):
        """Sorting by an alias that resolves to the PK must not append the PK a second time."""
        param = SortParam(["id"], ParseImportError, {"import_error_id": "id"}).set_value(["import_error_id"])
        resolved = param.get_resolved_columns()
        assert [name for name, _col, _desc in resolved] == ["import_error_id"]


def _compile(statement):
    return str(statement.compile(compile_kwargs={"literal_binds": True})).lower()


def _has_ilike(sql: str, term: str) -> bool:
    """Return True if ``sql`` contains an ILIKE (or dialect-equivalent) match for ``%term%``."""
    return f"'%{term.lower()}%'" in sql


class TestSearchParam:
    """Substring search (``ILIKE '%term%'``) — full-match, case-insensitive."""

    def test_to_orm_single_value(self):
        param = _SearchParam(DagModel.dag_id).set_value("example_bash")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert _has_ilike(sql, "example_bash")

    def test_to_orm_none_value_is_noop(self):
        """A ``None`` value with ``skip_none`` leaves the statement unchanged."""
        param = _SearchParam(DagModel.dag_id).set_value(None)
        statement = select(DagModel)
        result = param.to_orm(statement)
        assert result is statement

    def test_to_orm_tilde_alias_matches_all(self):
        """``~`` is aliased to ``%`` so the ILIKE expression matches all rows."""
        param = _SearchParam(DagModel.dag_id)
        aliased = param.transform_aliases("~")
        param.set_value(aliased)
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert _has_ilike(sql, "%")

    def test_to_orm_multiple_values_or(self):
        """Test search with multiple terms using the pipe | operator."""
        param = _SearchParam(DagModel.dag_id).set_value("example_bash | example_python")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "or" in sql
        assert _has_ilike(sql, "example_bash")
        assert _has_ilike(sql, "example_python")

    def test_to_orm_pipe_with_trailing_pipe(self):
        """Test that a trailing pipe is ignored and only the valid term is searched."""
        param = _SearchParam(DagModel.dag_id).set_value("example_bash|")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert _has_ilike(sql, "example_bash")
        assert " or " not in sql

    def test_to_orm_pipe_with_leading_pipe(self):
        """Test that a leading pipe is ignored and only the valid term is searched."""
        param = _SearchParam(DagModel.dag_id).set_value("|example_bash")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert _has_ilike(sql, "example_bash")
        assert " or " not in sql

    def test_to_orm_pipe_as_or_false_treats_pipe_as_literal(self):
        """With ``pipe_as_or=False``, the pipe character is passed through literally."""
        param = _SearchParam(DagModel.dag_id, pipe_as_or=False).set_value("2026-01-01|us")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert _has_ilike(sql, "2026-01-01|us")
        assert " or " not in sql

    def test_to_orm_pipe_as_or_false_tilde_alias_still_works(self):
        """``pipe_as_or=False`` must not interfere with the ``~`` → ``%`` alias."""
        param = _SearchParam(DagModel.dag_id, pipe_as_or=False)
        param.set_value(param.transform_aliases("~"))
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert _has_ilike(sql, "%")


class TestEscapeLikePattern:
    """The escape helper turns user input into a literal substring pattern.

    Filter parameters that do *not* document wildcard semantics must call this so a user-supplied
    ``%`` or ``_`` does not widen the match beyond the filter's intent. Search parameters that
    explicitly expose wildcard semantics (see ``_SearchParam``) deliberately do not call it.
    """

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("plain", "plain"),
            ("a%b", r"a\%b"),
            ("a_b", r"a\_b"),
            (r"a\b", r"a\\b"),
            (r"a\%b", r"a\\\%b"),
            ("%_\\", r"\%\_\\"),
            ("", ""),
        ],
    )
    def test_escapes_metacharacters(self, raw, expected):
        assert _escape_like_pattern(raw) == expected


class TestNonSearchFilterEscaping:
    """``_OwnersFilter`` / ``_AssetDependencyFilter`` / ``_ConsumingAssetFilter`` escape ``%`` and ``_``.

    Compile-time check: the rendered SQL must wrap the *escaped* user value in ``%...%`` and
    declare an ``ESCAPE`` clause so the database treats user-supplied wildcards literally.
    """

    def test_owners_filter_escapes_user_wildcards(self):
        param = _OwnersFilter().set_value(["100%_alice"])
        statement = param.to_orm(select(DagModel))
        sql = _compile(statement)
        assert r"'%100\%\_alice%'" in sql
        assert "escape" in sql

    def test_asset_dependency_filter_escapes_user_wildcards(self):
        param = _AssetDependencyFilter().set_value("ledger_%")
        statement = param.to_orm(select(DagModel))
        sql = _compile(statement)
        assert r"'%ledger\_\%%'" in sql
        assert "escape" in sql

    def test_consuming_asset_filter_escapes_user_wildcards(self):
        param = _ConsumingAssetFilter().set_value("foo_%bar")
        statement = param.to_orm(select(DagRun))
        sql = _compile(statement)
        assert r"'%foo\_\%bar%'" in sql
        assert "escape" in sql

    def test_search_param_does_not_escape_user_wildcards(self):
        """Counter-test: ``_SearchParam`` deliberately passes wildcards through."""
        param = _SearchParam(DagModel.dag_id).set_value("foo_%bar")
        statement = param.to_orm(select(DagModel))
        sql = _compile(statement)
        # Raw user wildcards are present, not the escaped form.
        assert "'%foo_%bar%'" in sql


class TestPrefixSearchParam:
    """Prefix search using range comparison (``attribute >= lower AND < upper``)."""

    def test_to_orm_single_value(self):
        param = _PrefixSearchParam(DagModel.dag_id).set_value("example_bash")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "dag_id >= 'example_bash'" in sql
        assert "dag_id < 'example_basi'" in sql

    def test_prefix_range_upper_boundary(self):
        """Prefix range bumps the last alphanumeric character."""
        assert _PrefixPatternParam._prefix_range_upper("xy") == "xz"
        assert _PrefixPatternParam._prefix_range_upper("test_dag") == "test_dah"
        assert _PrefixPatternParam._prefix_range_upper("") is None

    def test_prefix_range_upper_strips_trailing_punctuation(self):
        """Trailing non-alphanumeric characters are dropped before bumping."""
        assert _PrefixPatternParam._prefix_range_upper("test_") == "tesu"
        assert _PrefixPatternParam._prefix_range_upper("s3://") == "s4"
        assert _PrefixPatternParam._prefix_range_upper("bbc!!") == "bbd"
        # All-punctuation → no usable range
        assert _PrefixPatternParam._prefix_range_upper("///") is None

    def test_prefix_range_upper_cascades_past_non_alnum_bump(self):
        """If bumping would produce a non-alphanumeric char, strip and retry."""
        # 'z' → '{' (non-alphanumeric), so fall back to incrementing the previous char.
        assert _PrefixPatternParam._prefix_range_upper("abz") == "ac"
        # 'Z' → '[' (non-alphanumeric), so fall back.
        assert _PrefixPatternParam._prefix_range_upper("abZ") == "ac"
        # '9' → ':' (non-alphanumeric), so fall back.
        assert _PrefixPatternParam._prefix_range_upper("a9") == "b"

    def test_to_orm_strips_trailing_underscore_prefix(self):
        """``'test_'`` is treated as the prefix ``'test'`` so the range is locale-safe."""
        param = _PrefixSearchParam(DagModel.dag_id).set_value("test_")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "dag_id >= 'test'" in sql
        assert "dag_id < 'tesu'" in sql

    def test_to_orm_empty_value_matches_all(self):
        """An empty value matches all non-null rows (used via the ``~`` alias)."""
        param = _PrefixSearchParam(DagModel.dag_id).set_value("")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "is not null" in sql

    def test_to_orm_all_punctuation_value_matches_all(self):
        """A term with no alphanumeric characters also degenerates to the ``~`` behavior."""
        param = _PrefixSearchParam(DagModel.dag_id).set_value("///")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "is not null" in sql

    def test_to_orm_tilde_alias_matches_all(self):
        param = _PrefixSearchParam(DagModel.dag_id)
        aliased = param.transform_aliases("~")
        param.set_value(aliased)
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "is not null" in sql

    def test_to_orm_multiple_values_or(self):
        param = _PrefixSearchParam(DagModel.dag_id).set_value("example_bash | example_python")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "or" in sql
        assert "example_bash" in sql
        assert "example_python" in sql

    def test_to_orm_none_value_is_noop(self):
        param = _PrefixSearchParam(DagModel.dag_id).set_value(None)
        statement = select(DagModel)
        result = param.to_orm(statement)
        assert result is statement

    def test_to_orm_none_value_is_noop_when_skip_none_false(self):
        """With ``skip_none=False`` the filter slot is required by the route but the
        predicate must still be a no-op when the caller didn't provide a value."""
        param = _PrefixSearchParam(DagModel.dag_id, skip_none=False).set_value(None)
        statement = select(DagModel)
        result = param.to_orm(statement)
        assert result is statement

    def test_to_orm_pipe_as_or_false_treats_pipe_as_literal(self):
        """With ``pipe_as_or=False``, the pipe character is part of the prefix and not an OR delimiter."""
        param = _PrefixSearchParam(DagModel.dag_id, pipe_as_or=False).set_value("2026-01-01|us")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        # Use " or " (with spaces) to avoid false positives from column-name substrings like "owners".
        assert " or " not in sql
        # Range scan uses the full composite-key value as the lower bound.
        assert "2026-01-01|us" in sql

    def test_to_orm_pipe_as_or_false_tilde_alias_still_works(self):
        """``pipe_as_or=False`` must not interfere with the ``~`` → empty alias."""
        param = _PrefixSearchParam(DagModel.dag_id, pipe_as_or=False)
        param.set_value(param.transform_aliases("~"))
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "is not null" in sql


class TestTaskDisplayNamePrefixPatternParam:
    """Prefix filter splits on NULL override so ``task_id`` can use indexes."""

    def test_to_orm_uses_task_id_when_override_null(self):
        param = _TaskDisplayNamePrefixPatternParam().set_value("test_task_hello")
        statement = select(TaskInstance)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "task_display_name is null" in sql
        assert "task_id >=" in sql
        assert "task_id <" in sql
        assert "task_display_name is not null" in sql

    def test_to_orm_empty_matches_all(self):
        param = _TaskDisplayNamePrefixPatternParam().set_value("")
        statement = select(TaskInstance)
        statement = param.to_orm(statement)

        sql = _compile(statement)
        assert "true" in sql or "1 = 1" in sql


def _make_datetime_filter(filter_name, model=TaskInstance, attribute_name=None, **kwargs):
    """Call datetime_range_filter_factory outside FastAPI by supplying None for all omitted bounds."""
    defaults = dict(lower_bound_gte=None, lower_bound_gt=None, upper_bound_lte=None, upper_bound_lt=None)
    defaults.update(kwargs)
    return datetime_range_filter_factory(filter_name, model, attribute_name)(**defaults)


class TestDatetimeRangeFilterFactory:
    """datetime_range_filter_factory dispatches to NullableDatetimeRangeFilter for start/end dates."""

    def test_start_date_returns_nullable_filter(self):
        rf = _make_datetime_filter("start_date")
        assert isinstance(rf, NullableDatetimeRangeFilter)

    def test_end_date_returns_nullable_filter(self):
        rf = _make_datetime_filter("end_date")
        assert isinstance(rf, NullableDatetimeRangeFilter)

    def test_aliased_filter_name_returns_plain_filter(self):
        """dag_run_start_date uses attribute_name='start_date' via outer join; NULL means 'no run',
        not 'currently running', so it must return a plain RangeFilter to avoid inflating counts."""
        rf = _make_datetime_filter("dag_run_start_date", model=DagRun, attribute_name="start_date")
        assert type(rf) is RangeFilter

    def test_aliased_end_date_returns_plain_filter(self):
        """dag_run_end_date uses attribute_name='end_date' via outer join; must return plain RangeFilter."""
        rf = _make_datetime_filter("dag_run_end_date", model=DagRun, attribute_name="end_date")
        assert type(rf) is RangeFilter

    def test_other_column_returns_plain_filter(self):
        rf = _make_datetime_filter("queued_dttm")
        assert type(rf) is RangeFilter

    def test_lower_bound_does_not_include_now(self):
        """NULL branch on lower bounds passes unconditionally — no now() call."""
        bound = datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc)
        rf = _make_datetime_filter("start_date", lower_bound_gte=bound)
        sql = _compile(rf.to_orm(select(TaskInstance)))
        assert "is null" in sql
        assert "now()" not in sql
        assert "coalesce" not in sql

    def test_upper_bound_includes_now_for_running_tasks(self):
        """NULL branch on upper bounds uses now() to proxy the in-progress task's current time."""
        bound = datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc)
        rf = _make_datetime_filter("end_date", upper_bound_lte=bound)
        sql = _compile(rf.to_orm(select(TaskInstance)))
        assert "is null" in sql
        assert "now()" in sql
        assert "coalesce" not in sql

    def test_no_coalesce_for_start_date(self):
        bound = datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc)
        rf = _make_datetime_filter("start_date", upper_bound_lte=bound)
        sql = _compile(rf.to_orm(select(TaskInstance)))
        assert "coalesce" not in sql
