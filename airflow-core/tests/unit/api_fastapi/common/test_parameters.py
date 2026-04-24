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
from types import SimpleNamespace
from typing import Annotated

import pytest
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import select

from airflow.api_fastapi.common.parameters import (
    FilterParam,
    SortParam,
    _SearchParam,
    filter_param_factory,
)
from airflow.models import DagModel, DagRun, Log
from airflow.models.errors import ParseImportError


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

    def test_row_value_raises_on_column_form_to_replace(self):
        """
        Column-form ``to_replace`` is not supported by cursor encoding. The helper must
        fail loudly so a future endpoint doesn't silently ship ``None`` cursor tokens.
        """
        param = SortParam(["dag_id"], DagModel, {"last_run_state": DagRun.state}).set_value(
            ["last_run_state"]
        )
        row = SimpleNamespace(id="test_dag")
        with pytest.raises(NotImplementedError, match="column-form ``to_replace``"):
            param.row_value(row, "last_run_state")

    def test_primary_key_is_not_duplicated_when_alias_maps_to_pk(self):
        """Sorting by an alias that resolves to the PK must not append the PK a second time."""
        param = SortParam(["id"], ParseImportError, {"import_error_id": "id"}).set_value(["import_error_id"])
        resolved = param.get_resolved_columns()
        assert [name for name, _col, _desc in resolved] == ["import_error_id"]


class TestSearchParam:
    def test_to_orm_single_value(self):
        """Test search with a single term."""
        param = _SearchParam(DagModel.dag_id).set_value("example_bash")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = str(statement.compile(compile_kwargs={"literal_binds": True})).lower()
        assert "dag_id" in sql
        assert "like" in sql

    def test_to_orm_multiple_values_or(self):
        """Test search with multiple terms using the pipe | operator."""
        param = _SearchParam(DagModel.dag_id).set_value("example_bash | example_python")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = str(statement.compile(compile_kwargs={"literal_binds": True}))
        assert "OR" in sql
        assert "example_bash" in sql
        assert "example_python" in sql

    def test_to_orm_pipe_with_trailing_pipe(self):
        """Test that a trailing pipe is ignored and only the valid term is searched."""
        param = _SearchParam(DagModel.dag_id).set_value("example_bash|")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = str(statement.compile(compile_kwargs={"literal_binds": True}))
        assert "example_bash" in sql
        assert "|" not in sql

    def test_to_orm_pipe_with_leading_pipe(self):
        """Test that a leading pipe is ignored and only the valid term is searched."""
        param = _SearchParam(DagModel.dag_id).set_value("|example_bash")
        statement = select(DagModel)
        statement = param.to_orm(statement)

        sql = str(statement.compile(compile_kwargs={"literal_binds": True}))
        assert "example_bash" in sql
        assert "|" not in sql
