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
from fastapi.exceptions import RequestValidationError
from sqlalchemy import select, update

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.backfills import (
    BackfillCollectionResponse,
    BackfillPostBody,
    BackfillResponse,
    DryRunBackfillCollectionResponse,
    DryRunBackfillResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import (
    create_openapi_http_exception_doc,
)
from airflow.api_fastapi.core_api.security import requires_access_backfill, requires_access_dag
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import DagNotFound
from airflow.models import DagRun
from airflow.models.backfill import (
    AlreadyRunningBackfill,
    Backfill,
    BackfillDagRun,
    DagNoScheduleException,
    InvalidBackfillDate,
    InvalidBackfillDirection,
    InvalidReprocessBehavior,
    _create_backfill,
    _do_dry_run,
)
from airflow.utils import timezone
from airflow.utils.state import DagRunState

backfills_router = AirflowRouter(tags=["Backfill"], prefix="/backfills")


@backfills_router.get(
    path="",
    dependencies=[
        Depends(requires_access_backfill(method="GET")),
    ],
)
def list_backfills(
    dag_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id"], Backfill).dynamic_depends()),
    ],
    session: SessionDep,
) -> BackfillCollectionResponse:
    select_stmt, total_entries = paginated_select(
        statement=select(Backfill).where(Backfill.dag_id == dag_id),
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    backfills = session.scalars(select_stmt)
    return BackfillCollectionResponse(
        backfills=backfills,
        total_entries=total_entries,
    )


@backfills_router.get(
    path="/{backfill_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_backfill(method="GET")),
    ],
)
def get_backfill(
    backfill_id: str,
    session: SessionDep,
) -> BackfillResponse:
    backfill = session.get(Backfill, backfill_id)
    if backfill:
        return backfill
    raise HTTPException(status.HTTP_404_NOT_FOUND, "Backfill not found")


@backfills_router.put(
    path="/{backfill_id}/pause",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_backfill(method="PUT")),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.RUN)),
    ],
)
def pause_backfill(backfill_id, session: SessionDep) -> BackfillResponse:
    b = session.get(Backfill, backfill_id)
    if not b:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Could not find backfill with id {backfill_id}")
    if b.completed_at:
        raise HTTPException(status.HTTP_409_CONFLICT, "Backfill is already completed.")
    if b.is_paused is False:
        b.is_paused = True
    session.commit()
    return b


@backfills_router.put(
    path="/{backfill_id}/unpause",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_backfill(method="PUT")),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.RUN)),
    ],
)
def unpause_backfill(backfill_id, session: SessionDep) -> BackfillResponse:
    b = session.get(Backfill, backfill_id)
    if not b:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Could not find backfill with id {backfill_id}")
    if b.completed_at:
        raise HTTPException(status.HTTP_409_CONFLICT, "Backfill is already completed.")
    if b.is_paused:
        b.is_paused = False
    return b


@backfills_router.put(
    path="/{backfill_id}/cancel",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.RUN)),
        Depends(requires_access_backfill(method="PUT")),
    ],
)
def cancel_backfill(backfill_id, session: SessionDep) -> BackfillResponse:
    b: Backfill = session.get(Backfill, backfill_id)
    if not b:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Could not find backfill with id {backfill_id}")
    if b.completed_at is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, "Backfill is already completed.")

    # first, pause, and commit immediately to ensure no other dag runs are started
    if not b.is_paused:
        b.is_paused = True
        session.commit()  # ensure no new runs started

    query = (
        update(DagRun)
        .where(
            DagRun.id.in_(
                select(
                    BackfillDagRun.dag_run_id,
                ).where(
                    BackfillDagRun.backfill_id == b.id,
                ),
            ),
            DagRun.state == DagRunState.QUEUED,
        )
        .values(state=DagRunState.FAILED)
        .execution_options(synchronize_session=False)
    )
    session.execute(query)
    session.commit()  # this will fail all the queued dag runs in this backfill

    # this is in separate transaction just to avoid potential conflicts
    session.refresh(b)
    b.completed_at = timezone.utcnow()
    return b


@backfills_router.post(
    path="",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.RUN)),
        Depends(requires_access_backfill(method="POST")),
    ],
)
def create_backfill(
    backfill_request: BackfillPostBody,
) -> BackfillResponse:
    from_date = timezone.coerce_datetime(backfill_request.from_date)
    to_date = timezone.coerce_datetime(backfill_request.to_date)
    try:
        backfill_obj = _create_backfill(
            dag_id=backfill_request.dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=backfill_request.max_active_runs,
            reverse=backfill_request.run_backwards,
            dag_run_conf=backfill_request.dag_run_conf,
            reprocess_behavior=backfill_request.reprocess_behavior,
        )
        return BackfillResponse.model_validate(backfill_obj)

    except AlreadyRunningBackfill:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"There is already a running backfill for dag {backfill_request.dag_id}",
        )

    except DagNotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Could not find dag {backfill_request.dag_id}",
        )
    except (
        InvalidReprocessBehavior,
        InvalidBackfillDirection,
        DagNoScheduleException,
        InvalidBackfillDate,
    ) as e:
        raise RequestValidationError(str(e))


@backfills_router.post(
    path="/dry_run",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]),
    dependencies=[
        Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.RUN)),
        Depends(requires_access_backfill(method="POST")),
    ],
)
def create_backfill_dry_run(
    body: BackfillPostBody,
    session: SessionDep,
) -> DryRunBackfillCollectionResponse:
    from_date = timezone.coerce_datetime(body.from_date)
    to_date = timezone.coerce_datetime(body.to_date)

    try:
        backfills_dry_run = _do_dry_run(
            dag_id=body.dag_id,
            from_date=from_date,
            to_date=to_date,
            reverse=body.run_backwards,
            reprocess_behavior=body.reprocess_behavior,
            session=session,
        )
        backfills = [DryRunBackfillResponse(logical_date=d) for d in backfills_dry_run]

        return DryRunBackfillCollectionResponse(backfills=backfills, total_entries=len(backfills_dry_run))

    except DagNotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Could not find dag {body.dag_id}",
        )

    except (
        InvalidReprocessBehavior,
        InvalidBackfillDirection,
        DagNoScheduleException,
        InvalidBackfillDate,
    ) as e:
        raise RequestValidationError(str(e))
