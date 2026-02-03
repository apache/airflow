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

from typing import Literal

from fastapi import Depends, status
from fastapi.exceptions import HTTPException

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.common import BaseGraphResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.dependencies import (
    extract_single_connected_component,
    get_data_dependencies,
    get_scheduling_dependencies,
)

dependencies_router = AirflowRouter(tags=["Dependencies"])


@dependencies_router.get(
    "/dependencies",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.DEPENDENCIES))],
)
def get_dependencies(
    session: SessionDep,
    node_id: str | None = None,
    dependency_type: Literal["scheduling", "data"] = "scheduling",
) -> BaseGraphResponse:
    """Dependencies graph."""
    if dependency_type == "data":
        if node_id is None or not node_id.startswith("asset:"):
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Data dependencies require an asset node_id (e.g., 'asset:123')"
            )

        try:
            asset_id = int(node_id.replace("asset:", ""))
        except ValueError:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid asset node_id: {node_id}")

        data = get_data_dependencies(asset_id, session)
        return BaseGraphResponse(**data)

    data = get_scheduling_dependencies()

    if node_id is not None:
        try:
            data = extract_single_connected_component(node_id, data["nodes"], data["edges"])
        except ValueError as e:
            raise HTTPException(404, str(e))

    return BaseGraphResponse(**data)
