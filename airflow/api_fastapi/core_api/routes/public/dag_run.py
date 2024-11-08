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

from fastapi import Depends, HTTPException, Query, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.dag_run import (
    DAGRunClearBody,
    DAGRunPatchBody,
    DAGRunPatchStates,
    DAGRunResponse,
)
from airflow.api_fastapi.core_api.serializers.task_instances import (
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
)
from airflow.models import DAG, DagRun

dag_run_router = AirflowRouter(tags=["DagRun"], prefix="/dags/{dag_id}/dagRuns")


@dag_run_router.get(
    "/{dag_run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_dag_run(
    dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]
) -> DAGRunResponse:
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    return DAGRunResponse.model_validate(dag_run, from_attributes=True)


@dag_run_router.delete(
    "/{dag_run_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def delete_dag_run(dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]):
    """Delete a DAG Run entry."""
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))

    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    session.delete(dag_run)


@dag_run_router.patch(
    "/{dag_run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def patch_dag_run_state(
    dag_id: str,
    dag_run_id: str,
    patch_body: DAGRunPatchBody,
    session: Annotated[Session, Depends(get_session)],
    request: Request,
    update_mask: list[str] | None = Query(None),
) -> DAGRunResponse:
    """Modify a DAG Run."""
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)

    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    if update_mask:
        if update_mask != ["state"]:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Only `state` field can be updated through the REST API"
            )
    else:
        update_mask = ["state"]

    for attr_name in update_mask:
        if attr_name == "state":
            state = getattr(patch_body, attr_name)
            if state == DAGRunPatchStates.SUCCESS:
                set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True)
            elif state == DAGRunPatchStates.QUEUED:
                set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True)
            else:
                set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True)

    session.refresh(dag_run)

    return DAGRunResponse.model_validate(dag_run, from_attributes=True)


@dag_run_router.post("/{dag_run_id}/clear", responses=create_openapi_http_exception_doc([401, 403, 404]))
def clear_dag_run(
    dag_id: str,
    dag_run_id: str,
    patch_body: DAGRunClearBody,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
) -> TaskInstanceCollectionResponse | DAGRunResponse:
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            404, f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found"
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)

    if patch_body.dry_run:
        task_instances = dag.clear(
            start_date=dag_run.logical_date,
            end_date=dag_run.logical_date,
            task_ids=None,
            only_failed=False,
            dry_run=True,
            session=session,
        )

        return TaskInstanceCollectionResponse(
            task_instances=[
                TaskInstanceResponse.model_validate(ti, from_attributes=True)
                for ti in task_instances  # type: ignore[union-attr]
            ],
            total_entries=len(task_instances),  # type: ignore[arg-type]
        )
    else:
        dag.clear(
            start_date=dag_run.start_date,
            end_date=dag_run.end_date,
            task_ids=None,
            only_failed=False,
            session=session,
        )
        dag_run_cleared = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
        return DAGRunResponse.model_validate(dag_run_cleared, from_attributes=True)
