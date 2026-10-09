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
from uuid import UUID

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.execution import (
    ExecutionCollectionResponse,
    ExecutionRegionResponse,
    ExecutionTaskResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import DagAccessEntity, requires_access_dag
from airflow.api_fastapi.core_api.services.public.task_coordinates import CoordinateResolverDep
from airflow.api_fastapi.core_api.services.ui.execution import get_execution_members
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import load_region_ancestry
from airflow.models.taskinstance import TaskInstance as TI

execution_router = AirflowRouter(
    tags=["DagRun"],
    prefix="/dags/{dag_id}/dagRuns/{dag_run_id}",
)


@execution_router.get(
    "/execution",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag("GET", DagAccessEntity.TASK_INSTANCE))],
)
def get_execution(
    dag_id: str,
    dag_run_id: str,
    session: SessionDep,
    resolver: CoordinateResolverDep,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "task_id", "map_index", "state", "try_number", "start_date", "end_date", "duration"],
                TI,
                to_replace={"map_index": "region_index"},
            ).dynamic_depends()
        ),
    ],
    task_id: str | None = None,
    region_id: UUID | None = None,
    region_index: Annotated[int | None, Query(ge=-1)] = None,
    try_number: Annotated[int | None, Query(ge=0)] = None,
) -> ExecutionCollectionResponse:
    """
    List a Dag run's live task instances with the region structure that locates them.

    Supplying ``try_number`` selects the tries with that number instead, including archived
    ones. Use ``region_id``, ``region_index`` and ``try_number`` for exact links.
    """
    if region_index is not None and region_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "region_index requires region_id")
    run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))
    if run is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Dag run not found")
    members, total = get_execution_members(
        run,
        session=session,
        order_by=order_by,
        task_id=task_id,
        region_id=region_id,
        region_index=region_index,
        try_number=try_number,
        limit=limit.value or 0,
        offset=offset.value or 0,
    )
    resolver.prefetch_regions([ti for ti, _ in members])
    regions = load_region_ancestry(
        {ti.region_id for ti, _ in members}, dag_id=dag_id, run_id=dag_run_id, session=session
    )
    tasks = [
        ExecutionTaskResponse.model_validate(
            {
                "id": ti.id,
                "dag_id": ti.dag_id,
                "dag_run_id": ti.run_id,
                "task_id": ti.task_id,
                "task_display_name": ti.task_display_name,
                "region_id": ti.region_id,
                "region_index": ti.region_index,
                "map_index": map_index,
                # Clearing offers loop options only for work a loop produced; a mapped expansion
                # has a region without being one.
                "in_loop": resolver.get_loop_iteration(ti) is not None,
                "try_number": ti.try_number,
                "state": ti.state,
                "start_date": ti.start_date,
                "end_date": ti.end_date,
                "duration": ti.duration,
                "dag_version_id": ti.dag_version_id,
                "operator": ti.operator,
                "note": ti.note,
            }
        )
        for ti, map_index in members
    ]
    return ExecutionCollectionResponse(
        task_instances=tasks,
        regions=[ExecutionRegionResponse.model_validate(regions[key]) for key in sorted(regions)],
        total_entries=total,
    )
