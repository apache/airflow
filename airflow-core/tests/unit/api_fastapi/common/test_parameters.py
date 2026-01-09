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
from typing import Annotated

import pytest
from fastapi import Depends, FastAPI, HTTPException

from airflow.api_fastapi.common.parameters import FilterParam, SortParam, filter_param_factory
from airflow.models import DagRun, Log


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
