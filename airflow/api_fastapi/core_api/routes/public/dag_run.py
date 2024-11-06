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
from sqlalchemy.orm import Session, joinedload
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
    DAGRunPatchBody,
    DAGRunPatchStates,
    DAGRunResponse,
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
async def get_dag_run(
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
async def delete_dag_run(dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]):
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
async def patch_dag_run(
    dag_id: str,
    dag_run_id: str,
    patch_body: DAGRunPatchBody,
    session: Annotated[Session, Depends(get_session)],
    request: Request,
    update_mask: list[str] | None = Query(None),
) -> DAGRunResponse:
    """Modify a DAG Run."""
    ALLOWED_FIELD_MASK = ["state", "note"]
    dag_run = session.scalar(
        select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id).options(joinedload(DagRun.dag_run_note))
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)

    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    if update_mask:
        update_mask_set = set(update_mask)
        validation_errors = []

        invalid_fields = update_mask_set - set(ALLOWED_FIELD_MASK)
        if invalid_fields:
            validation_errors.append(f"Invalid fields in update mask: {', '.join(invalid_fields)}")

        missing_fields = update_mask_set - patch_body.model_fields_set
        if missing_fields:
            validation_errors.append(f"Fields not present in request body: {', '.join(missing_fields)}")

        if validation_errors:
            raise HTTPException(400, "; ".join(validation_errors))

    fields_to_update = {
        field
        for field in ALLOWED_FIELD_MASK
        if field in patch_body.model_fields_set and (update_mask is None or field in update_mask)
    }

    for attr_name in fields_to_update:
        attr_value = getattr(patch_body, attr_name)
        if attr_name == "state":
            attr_value = getattr(patch_body, "state")
            if attr_value == DAGRunPatchStates.SUCCESS:
                set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
            elif attr_value == DAGRunPatchStates.QUEUED:
                set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
            elif attr_value == DAGRunPatchStates.FAILED:
                set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
        elif attr_name == "note":
            # Once Authentication is implemented in this FastAPI app,
            # user id will be added when updating dag run note
            # Refer to https://github.com/apache/airflow/issues/43534
            dag_run = session.get(DagRun, dag_run.id)
            if dag_run.dag_run_note is None:
                dag_run.note = (attr_value, None)
            else:
                dag_run.dag_run_note.content = attr_value

    dag_run = session.get(DagRun, dag_run.id)

    return DAGRunResponse.model_validate(dag_run, from_attributes=True)
