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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import contains_eager, noload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.deadline import (
    DeadlineAlertCollectionResponse,
    DeadlineCollectionResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import ReadableDagRunsFilterDep, requires_access_dag
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.deadline_alert import DeadlineAlert
from airflow.models.serialized_dag import SerializedDagModel

deadlines_router = AirflowRouter(prefix="/dags/{dag_id}", tags=["Deadlines"])


@deadlines_router.get(
    "/dagRuns/{dag_run_id}/deadlines",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
)
def get_deadlines(
    dag_id: str,
    dag_run_id: str,
    session: SessionDep,
    limit: QueryLimit,
    offset: QueryOffset,
    readable_dag_runs_filter: ReadableDagRunsFilterDep,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "deadline_time", "created_at", "last_updated_at", "missed"],
                Deadline,
                to_replace={
                    "dag_id": DagRun.dag_id,
                    "dag_run_id": DagRun.run_id,
                    "alert_name": DeadlineAlert.name,
                },
            ).dynamic_depends(default="deadline_time")
        ),
    ],
    missed: Annotated[FilterParam[bool | None], Depends(filter_param_factory(Deadline.missed, bool | None))],
    deadline_time: Annotated[RangeFilter, Depends(datetime_range_filter_factory("deadline_time", Deadline))],
    last_updated_at: Annotated[
        RangeFilter, Depends(datetime_range_filter_factory("last_updated_at", Deadline))
    ],
) -> DeadlineCollectionResponse:
    """
    Get deadlines for a DAG run.

    This endpoint allows specifying `~` as the dag_id and dag_run_id to retrieve Deadlines for all
    DAGs and DAG runs.
    """
    query = (
        select(Deadline)
        .join(Deadline.dagrun)
        .outerjoin(Deadline.deadline_alert)
        .options(
            contains_eager(Deadline.dagrun).options(noload(DagRun.deadlines)),
            contains_eager(Deadline.deadline_alert),
            noload(Deadline.callback),
        )
    )

    if dag_run_id != "~":
        if dag_id == "~":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "dag_id is required when dag_run_id is specified",
            )
        query = query.where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id)
    elif dag_id != "~":
        query = query.where(DagRun.dag_id == dag_id)

    deadlines_select, total_entries = paginated_select(
        statement=query,
        filters=[readable_dag_runs_filter, missed, deadline_time, last_updated_at],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    deadlines = session.scalars(deadlines_select).unique()

    if dag_run_id != "~" and total_entries == 0:
        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))
        if not dag_run:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
            )

    return DeadlineCollectionResponse(deadlines=deadlines, total_entries=total_entries)


@deadlines_router.get(
    "/deadlineAlerts",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(
            requires_access_dag(
                method="GET",
            )
        ),
    ],
)
def get_dag_deadline_alerts(
    dag_id: str,
    session: SessionDep,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "created_at", "name", "interval"],
                DeadlineAlert,
            ).dynamic_depends(default="created_at")
        ),
    ],
) -> DeadlineAlertCollectionResponse:
    """Get all deadline alerts defined on a DAG."""
    serialized_dag = session.scalar(
        select(SerializedDagModel)
        .where(SerializedDagModel.dag_id == dag_id)
        .order_by(SerializedDagModel.id.desc())
    )

    if not serialized_dag:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Dag with id {dag_id} was not found",
        )

    query = select(DeadlineAlert).where(
        DeadlineAlert.serialized_dag_id == serialized_dag.id,
    )

    alerts_select, total_entries = paginated_select(
        statement=query,
        filters=None,
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    alerts = session.scalars(alerts_select)

    return DeadlineAlertCollectionResponse(deadline_alerts=alerts, total_entries=total_entries)
