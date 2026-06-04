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

from fastapi import APIRouter, HTTPException, Query, status

from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.dags import (
    DagResponse,
    DagTaskGroupsExistenceResponse,
    DagTasksExistenceResponse,
)
from airflow.models.dag import DagModel

router = APIRouter()


@router.get(
    "/{dag_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
    },
)
def get_dag(
    dag_id: str,
    session: SessionDep,
) -> DagResponse:
    """Get a DAG."""
    dag_model: DagModel | None = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"The Dag with dag_id: `{dag_id}` was not found",
            },
        )

    return DagResponse(
        dag_id=dag_model.dag_id,
        is_paused=dag_model.is_paused,
        bundle_name=dag_model.bundle_name,
        bundle_version=dag_model.bundle_version,
        relative_fileloc=dag_model.relative_fileloc,
        owners=dag_model.owners,
        tags=sorted(tag.name for tag in dag_model.tags),
        next_dagrun=dag_model.next_dagrun,
    )


@router.get(
    "/{dag_id}/task-groups/existence",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
    },
)
def get_dag_task_groups_existence(
    dag_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    task_group_ids: list[str] = Query(
        default_factory=list, description="Task group ids to check for existence"
    ),
) -> DagTaskGroupsExistenceResponse:
    """Get the list of existing and missing Dag task group ids from the given ids."""
    if not session.get(DagModel, dag_id):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"The Dag with dag_id: `{dag_id}` was not found",
            },
        )

    dag = get_latest_version_of_dag(dag_bag, dag_id, session, include_reason=True)

    existing: list[str] = []
    missing: list[str] = []
    for task_group_id in task_group_ids:
        (existing if task_group_id in dag.task_group_dict else missing).append(task_group_id)

    return DagTaskGroupsExistenceResponse(existing=existing, missing=missing)


@router.get(
    "/{dag_id}/tasks/existence",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
    },
)
def get_dag_tasks_existence(
    dag_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    task_ids: list[str] = Query(default_factory=list, description="Task ids to check for existence"),
) -> DagTasksExistenceResponse:
    """Get the list of existing and missing Dag task ids from the given ids."""
    if not session.get(DagModel, dag_id):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"The Dag with dag_id: `{dag_id}` was not found",
            },
        )

    dag = get_latest_version_of_dag(dag_bag, dag_id, session, include_reason=True)

    existing: list[str] = []
    missing: list[str] = []
    for task_id in task_ids:
        (existing if dag.has_task(task_id) else missing).append(task_id)

    return DagTasksExistenceResponse(existing=existing, missing=missing)
