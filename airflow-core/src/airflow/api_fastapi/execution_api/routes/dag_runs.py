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

import logging
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import func, select

from airflow.api.common.trigger_dag import trigger_dag
from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.execution_api.datamodels.dagrun import DagRunStateResponse, TriggerDAGRunPayload
from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun
from airflow.exceptions import DagRunAlreadyExists
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun as DagRunModel
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType

router = APIRouter()


log = logging.getLogger(__name__)


@router.post(
    "/{dag_id}/{run_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "DAG has import errors and cannot be triggered"},
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
        status.HTTP_409_CONFLICT: {"description": "DAG Run already exists for the given dag_id"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid payload"},
    },
)
def trigger_dag_run(
    dag_id: str,
    run_id: str,
    payload: TriggerDAGRunPayload,
    session: SessionDep,
):
    """Trigger a DAG Run."""
    dm = session.scalar(select(DagModel).where(~DagModel.is_stale, DagModel.dag_id == dag_id).limit(1))
    if not dm:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"DAG with dag_id: '{dag_id}' not found"},
        )

    if dm.has_import_errors:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail={
                "reason": "import_errors",
                "message": f"DAG with dag_id: '{dag_id}' has import errors and cannot be triggered",
            },
        )

    try:
        trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf=payload.conf,
            logical_date=payload.logical_date,
            triggered_by=DagRunTriggeredByType.OPERATOR,
            replace_microseconds=False,
            session=session,
        )
    except DagRunAlreadyExists:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={
                "reason": "already_exists",
                "message": f"A DAG Run already exists for DAG {dag_id} with run id {run_id}",
            },
        )


@router.post(
    "/{dag_id}/{run_id}/clear",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "DAG has import errors and cannot be triggered"},
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid payload"},
    },
)
def clear_dag_run(
    dag_id: str,
    run_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
):
    """Clear a DAG Run."""
    dm = session.scalar(select(DagModel).where(~DagModel.is_stale, DagModel.dag_id == dag_id).limit(1))
    if not dm:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"DAG with dag_id: '{dag_id}' not found"},
        )

    if dm.has_import_errors:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail={
                "reason": "import_errors",
                "message": f"DAG with dag_id: '{dag_id}' has import errors and cannot be triggered",
            },
        )

    dag_run = session.scalar(
        select(DagRunModel).where(DagRunModel.dag_id == dag_id, DagRunModel.run_id == run_id)
    )
    dag = get_dag_for_run(dag_bag, dag_run=dag_run, session=session)

    dag.clear(run_id=run_id)


@router.get(
    "/{dag_id}/{run_id}/state",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "DAG not found for the given dag_id"},
    },
)
def get_dagrun_state(
    dag_id: str,
    run_id: str,
    session: SessionDep,
) -> DagRunStateResponse:
    """Get a DAG Run State."""
    dag_run = session.scalar(
        select(DagRunModel).where(DagRunModel.dag_id == dag_id, DagRunModel.run_id == run_id)
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"The DagRun with dag_id: `{dag_id}` and run_id: `{run_id}` was not found",
            },
        )

    return DagRunStateResponse(state=dag_run.state)


@router.get("/count", status_code=status.HTTP_200_OK)
def get_dr_count(
    dag_id: str,
    session: SessionDep,
    logical_dates: Annotated[list[UtcDateTime] | None, Query()] = None,
    run_ids: Annotated[list[str] | None, Query()] = None,
    states: Annotated[list[str] | None, Query()] = None,
) -> int:
    """Get the count of DAG runs matching the given criteria."""
    query = select(func.count()).select_from(DagRunModel).where(DagRunModel.dag_id == dag_id)

    if logical_dates:
        query = query.where(DagRunModel.logical_date.in_(logical_dates))

    if run_ids:
        query = query.where(DagRunModel.run_id.in_(run_ids))

    if states:
        query = query.where(DagRunModel.state.in_(states))

    count = session.scalar(query)
    return count or 0


@router.get("/{dag_id}/previous", status_code=status.HTTP_200_OK)
def get_previous_dagrun(
    dag_id: str,
    logical_date: UtcDateTime,
    session: SessionDep,
    state: Annotated[DagRunState | None, Query()] = None,
) -> DagRun | None:
    """Get the previous DAG run before the given logical date, optionally filtered by state."""
    query = (
        select(DagRunModel)
        .where(
            DagRunModel.dag_id == dag_id,
            DagRunModel.logical_date < logical_date,
        )
        .order_by(DagRunModel.logical_date.desc())
        .limit(1)
    )

    if state:
        query = query.where(DagRunModel.state == state)

    dag_run = session.scalar(query)

    if not dag_run:
        return None

    return DagRun.model_validate(dag_run)
