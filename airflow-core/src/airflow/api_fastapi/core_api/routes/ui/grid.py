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

import collections
from typing import TYPE_CHECKING, Annotated

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.common import (
    GridNodeResponse,
    GridRunsResponse,
    LatestRunResponse,
)
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridTISummaries,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.grid import (
    _find_aggregates,
    _merge_node_dicts,
)
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils.task_group import (
    get_task_group_children_getter,
    task_group_to_dict_grid,
)

log = structlog.get_logger(logger_name=__name__)
grid_router = AirflowRouter(prefix="/grid", tags=["Grid"])


def _get_latest_serdag(dag_id, session):
    serdag = session.scalar(
        select(SerializedDagModel)
        .where(
            SerializedDagModel.dag_id == dag_id,
        )
        .order_by(SerializedDagModel.id.desc())
        .limit(1)
    )
    if not serdag:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Dag with id {dag_id} was not found",
        )
    return serdag


def _get_serdag(dag_id, dag_version_id, session) -> SerializedDagModel | None:
    # this is a simplification - we account for structure based on the first task
    version = session.scalar(select(DagVersion).where(DagVersion.id == dag_version_id))
    if not version:
        version = session.scalar(
            select(DagVersion)
            .where(
                DagVersion.dag_id == dag_id,
            )
            .order_by(DagVersion.id)  # ascending cus this is mostly for pre-3.0 upgrade
            .limit(1)
        )
    if not (serdag := version.serialized_dag):
        log.error(
            "No serialized dag found",
            dag_id=dag_id,
            version_id=version.id,
            version_number=version.version_number,
        )
    return serdag


@grid_router.get(
    "/structure/{dag_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN)),
    ],
    response_model_exclude_none=True,
)
def get_dag_structure(
    dag_id: str,
    session: SessionDep,
    offset: QueryOffset,
    limit: QueryLimit,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["run_after", "logical_date", "start_date", "end_date"], DagRun).dynamic_depends()),
    ],
    run_after: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DagRun))],
) -> list[GridNodeResponse]:
    """Return dag structure for grid view."""
    latest_serdag = _get_latest_serdag(dag_id, session)
    latest_dag = latest_serdag.dag

    # Retrieve, sort the previous DAG Runs
    base_query = select(DagRun.id).where(DagRun.dag_id == dag_id)
    # This comparison is to fall back to DAG timetable when no order_by is provided
    if order_by.value == order_by.get_primary_key_string():
        ordering = list(latest_dag.timetable.run_ordering)
        order_by = SortParam(
            allowed_attrs=ordering,
            model=DagRun,
        ).set_value(ordering[0])
    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        order_by=order_by,
        offset=offset,
        filters=[run_after],
        limit=limit,
    )
    run_ids = list(session.scalars(dag_runs_select_filter))

    task_group_sort = get_task_group_children_getter()
    if not run_ids:
        nodes = [task_group_to_dict_grid(x) for x in task_group_sort(latest_dag.task_group)]
        return nodes

    serdags = session.scalars(
        select(SerializedDagModel).where(
            SerializedDagModel.dag_version_id.in_(
                select(TaskInstance.dag_version_id)
                .join(TaskInstance.dag_run)
                .where(
                    DagRun.id.in_(run_ids),
                    SerializedDagModel.id != latest_serdag.id,
                )
            )
        )
    )
    merged_nodes: list[GridNodeResponse] = []
    dags = [latest_dag]
    for serdag in serdags:
        if serdag:
            dags.append(serdag.dag)
    for dag in dags:
        nodes = [task_group_to_dict_grid(x) for x in task_group_sort(dag.task_group)]
        _merge_node_dicts(merged_nodes, nodes)

    return merged_nodes


@grid_router.get(
    "/runs/{dag_id}",
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
                access_entity=DagAccessEntity.TASK_INSTANCE,
            )
        ),
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
    response_model_exclude_none=True,
)
def get_grid_runs(
    dag_id: str,
    session: SessionDep,
    offset: QueryOffset,
    limit: QueryLimit,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "run_after",
                    "logical_date",
                    "start_date",
                    "end_date",
                ],
                DagRun,
            ).dynamic_depends()
        ),
    ],
    run_after: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DagRun))],
) -> list[GridRunsResponse]:
    """Get info about a run for the grid."""
    # Retrieve, sort the previous DAG Runs
    base_query = select(
        DagRun.dag_id,
        DagRun.run_id,
        DagRun.queued_at,
        DagRun.start_date,
        DagRun.end_date,
        DagRun.run_after,
        DagRun.state,
        DagRun.run_type,
    ).where(DagRun.dag_id == dag_id)

    # This comparison is to fall back to DAG timetable when no order_by is provided
    if order_by.value == order_by.get_primary_key_string():
        latest_serdag = _get_latest_serdag(dag_id, session)
        latest_dag = latest_serdag.dag
        ordering = list(latest_dag.timetable.run_ordering)
        order_by = SortParam(
            allowed_attrs=ordering,
            model=DagRun,
        ).set_value(ordering[0])
    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        order_by=order_by,
        offset=offset,
        filters=[run_after],
        limit=limit,
    )
    return session.execute(dag_runs_select_filter)


@grid_router.get(
    "/ti_summaries/{dag_id}/{run_id}",
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
                access_entity=DagAccessEntity.TASK_INSTANCE,
            )
        ),
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
)
def get_grid_ti_summaries(
    dag_id: str,
    run_id: str,
    session: SessionDep,
) -> GridTISummaries:
    """
    Get states for TIs / "groups" of TIs.

    Essentially this is to know what color to put in the squares in the grid.

    The tricky part here is that we aggregate the state for groups and mapped tasks.

    We don't add all the TIs for mapped TIs -- we only add one entry for the mapped task and
    its state is an aggregate of its TI states.

    And for task groups, we add a "task" for that which is not really a task but is just
    an entry that represents the group (so that we can show a filled in box when the group
    is not expanded) and its state is an agg of those within it.
    """
    tis_of_dag_runs, _ = paginated_select(
        statement=(
            select(
                TaskInstance.task_id,
                TaskInstance.state,
                TaskInstance.dag_version_id,
                TaskInstance.start_date,
                TaskInstance.end_date,
            )
            .where(TaskInstance.dag_id == dag_id)
            .where(
                TaskInstance.run_id == run_id,
            )
        ),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance).set_value("task_id"),
        limit=None,
        return_total_entries=False,
    )
    task_instances = list(session.execute(tis_of_dag_runs))
    if not task_instances:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"No task instances for dag_id={dag_id} run_id={run_id}"
        )
    ti_details = collections.defaultdict(list)
    for ti in task_instances:
        ti_details[ti.task_id].append(
            {
                "state": ti.state,
                "start_date": ti.start_date,
                "end_date": ti.end_date,
            }
        )
    serdag = _get_serdag(
        dag_id=dag_id,
        dag_version_id=task_instances[0].dag_version_id,
        session=session,
    )
    if TYPE_CHECKING:
        assert serdag

    def get_node_sumaries():
        for node in _find_aggregates(
            node=serdag.dag.task_group,
            parent_node=None,
            ti_details=ti_details,
        ):
            if node["type"] == "task":
                node["child_states"] = None
                node["min_start_date"] = None
                node["max_end_date"] = None
            yield node

    return {  # type: ignore[return-value]
        "run_id": run_id,
        "dag_id": dag_id,
        "task_instances": list(get_node_sumaries()),
    }


@grid_router.get(
    "/latest_run/{dag_id}",
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
                access_entity=DagAccessEntity.TASK_INSTANCE,
            )
        ),
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
    response_model_exclude_none=True,
)
def get_latest_run(
    dag_id: str,
    session: SessionDep,
) -> LatestRunResponse | None:
    """
    Get information about the latest dag run by run_after.

    This is used by the UI to figure out if it needs to rerun queries and resume auto refresh.
    """
    return session.execute(
        select(
            DagRun.id,
            DagRun.dag_id,
            DagRun.run_id,
            DagRun.run_after,
        )
        .where(DagRun.dag_id == dag_id)
        .order_by(DagRun.run_after.desc())
        .limit(1)
    ).one_or_none()
