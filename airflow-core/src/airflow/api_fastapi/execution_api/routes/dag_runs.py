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

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.exc import NoResultFound

from airflow.api.common.trigger_dag import trigger_dag
from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.api_fastapi.execution_api.datamodels.dagrun import DagRunStateResponse, TriggerDAGRunPayload
from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun
from airflow.exceptions import DagRunAlreadyExists
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun as DagRunModel
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType

router = VersionedAPIRouter()

log = logging.getLogger(__name__)


@router.only_exists_in_older_versions
@router.get("/{dag_id}/previous")
def get_previous_dagrun_compat(
    dag_id: str,
    logical_date: UtcDateTime,
    session: SessionDep,
    state: DagRunState | None = None,
):
    """
    Redirect old previous dag run request to the new endpoint.

    This endpoint must be put before ``get_dag_run`` so not to be shadowed.
    Newer client versions would not see this endpoint, and be routed to
    ``get_dag_run`` below instead.
    """
    return get_previous_dagrun(dag_id, logical_date, session, state)


@router.get(
    "/{dag_id}/{run_id}",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Dag run not found"}},
)
def get_dag_run(dag_id: str, run_id: str, session: SessionDep) -> DagRun:
    """Get detail of a Dag run."""
    dr = session.scalar(select(DagRunModel).where(DagRunModel.dag_id == dag_id, DagRunModel.run_id == run_id))
    if dr is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Dag run with dag_id '{dag_id}' and run_id '{run_id}' was not found",
            },
        )
    return DagRun.model_validate(dr)


@router.post(
    "/{dag_id}/{run_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Dag has import errors and cannot be triggered"},
        status.HTTP_404_NOT_FOUND: {"description": "Dag not found for the given dag_id"},
        status.HTTP_409_CONFLICT: {"description": "Dag run already exists for the given dag_id"},
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid payload"},
    },
)
def trigger_dag_run(
    dag_id: str,
    run_id: str,
    payload: TriggerDAGRunPayload,
    session: SessionDep,
) -> None:
    """Trigger a Dag run."""
    dm = session.scalar(select(DagModel).where(~DagModel.is_stale, DagModel.dag_id == dag_id).limit(1))
    if not dm:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Dag with dag_id: '{dag_id}' not found"},
        )

    if dm.has_import_errors:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail={
                "reason": "import_errors",
                "message": f"Dag with dag_id '{dag_id}' has import errors and cannot be triggered",
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
                "message": f"A run already exists for Dag '{dag_id}' with run_id '{run_id}'",
            },
        )


@router.post(
    "/{dag_id}/{run_id}/clear",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Dag has import errors and cannot be triggered"},
        status.HTTP_404_NOT_FOUND: {"description": "Dag not found for the given dag_id"},
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid payload"},
    },
)
def clear_dag_run(
    dag_id: str,
    run_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> None:
    """Clear a Dag run."""
    dm = session.scalar(select(DagModel).where(~DagModel.is_stale, DagModel.dag_id == dag_id).limit(1))
    if not dm:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Dag with dag_id: '{dag_id}' not found"},
        )

    if dm.has_import_errors:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail={
                "reason": "import_errors",
                "message": f"Dag with dag_id '{dag_id}' has import errors and cannot be triggered",
            },
        )

    dag_run = session.scalar(
        select(DagRunModel).where(DagRunModel.dag_id == dag_id, DagRunModel.run_id == run_id)
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Dag run with run_id: '{run_id}' not found"},
        )
    dag = get_dag_for_run(dag_bag, dag_run=dag_run, session=session)

    dag.clear(run_id=run_id)


@router.get(
    "/{dag_id}/{run_id}/state",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Dag run not found"}},
)
def get_dagrun_state(
    dag_id: str,
    run_id: str,
    session: SessionDep,
) -> DagRunStateResponse:
    """Get a Dag run State."""
    try:
        state = session.scalars(
            select(DagRunModel.state).where(DagRunModel.dag_id == dag_id, DagRunModel.run_id == run_id)
        ).one()
    except NoResultFound:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Dag run with dag_id '{dag_id}' and run_id '{run_id}' was not found",
            },
        )
    return DagRunStateResponse(state=state)


@router.get("/count", status_code=status.HTTP_200_OK)
def get_dr_count(
    dag_id: str,
    session: SessionDep,
    logical_dates: Annotated[list[UtcDateTime] | None, Query()] = None,
    run_ids: Annotated[list[str] | None, Query()] = None,
    states: Annotated[list[str] | None, Query()] = None,
) -> int:
    """Get the count of Dag runs matching the given criteria."""
    stmt = select(func.count()).select_from(DagRunModel).where(DagRunModel.dag_id == dag_id)
    if logical_dates:
        stmt = stmt.where(DagRunModel.logical_date.in_(logical_dates))
    if run_ids:
        stmt = stmt.where(DagRunModel.run_id.in_(run_ids))
    if states:
        stmt = stmt.where(DagRunModel.state.in_(states))
    return session.scalar(stmt) or 0


@router.get("/previous", status_code=status.HTTP_200_OK)
def get_previous_dagrun(
    dag_id: str,
    logical_date: UtcDateTime,
    session: SessionDep,
    state: Annotated[DagRunState | None, Query()] = None,
) -> DagRun | None:
    """Get the previous Dag run before the given logical date, optionally filtered by state."""
    stmt = (
        select(DagRunModel)
        .where(DagRunModel.dag_id == dag_id, DagRunModel.logical_date < logical_date)
        .order_by(DagRunModel.logical_date.desc())
        .limit(1)
    )
    if state:
        stmt = stmt.where(DagRunModel.state == state)
    if not (dag_run := session.scalar(stmt)):
        return None
    return DagRun.model_validate(dag_run)
