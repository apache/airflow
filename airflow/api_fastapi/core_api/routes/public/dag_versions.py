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

from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_versions import DAGVersionCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models.dag import DAG
from airflow.models.dag_version import DagVersion

dag_versions_router = AirflowRouter(tags=["DagVersion"], prefix="/dags/{dag_id}/dagVersions")


@dag_versions_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_dag_versions(
    dag_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    session: SessionDep,
    request: Request,
    version_number: Annotated[
        FilterParam[int], Depends(filter_param_factory(DagVersion.version_number, int))
    ],
    bundle_name: Annotated[FilterParam[str], Depends(filter_param_factory(DagVersion.bundle_name, str))],
    bundle_version: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(DagVersion.bundle_version, str | None))
    ],
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(["id", "version_number", "bundle_name", "bundle_version"], DagVersion).dynamic_depends()
        ),
    ],
) -> DAGVersionCollectionResponse:
    """
    Get all DAG Versions.

    This endpoint allows specifying `~` as the dag_id to retrieve DAG Versions for all DAGs.
    """
    query = select(DagVersion)

    if dag_id != "~":
        dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
        if not dag:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"The DAG with dag_id: `{dag_id}` was not found")

        query = query.filter(DagVersion.dag_id == dag_id)

    dag_versions_select, total_entries = paginated_select(
        statement=query,
        filters=[version_number, bundle_name, bundle_version],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    dag_versions = session.scalars(dag_versions_select)

    return DAGVersionCollectionResponse(
        dag_versions=dag_versions,
        total_entries=total_entries,
    )
