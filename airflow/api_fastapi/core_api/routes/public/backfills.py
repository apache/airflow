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
from sqlalchemy import desc, select, update

from airflow.api_fastapi.common.db.common import (
    AsyncSessionDep,
    SessionDep,
    paginated_select_async,
)
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.backfills import (
    BackfillCollectionResponse,
    BackfillDryRunResponse,
    BackfillPostBody,
    BackfillResponse,
    BackfillRunInfo,
)
from airflow.api_fastapi.core_api.openapi.exceptions import (
    create_openapi_http_exception_doc,
)
from airflow.models import DagRun
from airflow.models.backfill import (
    AlreadyRunningBackfill,
    Backfill,
    BackfillDagRun,
    BackfillDagRunExceptionReason,
    ReprocessBehavior,
    _create_backfill,
    _get_info_list,
)
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.sqlalchemy import nulls_first
from airflow.utils.state import DagRunState

backfills_router = AirflowRouter(tags=["Backfill"], prefix="/backfills")


@backfills_router.get(
    path="",
)
async def list_backfills(
    dag_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id"], Backfill).dynamic_depends()),
    ],
    session: AsyncSessionDep,
) -> BackfillCollectionResponse:
    select_stmt, total_entries = await paginated_select_async(
        statement=select(Backfill).where(Backfill.dag_id == dag_id),
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    backfills = await session.scalars(select_stmt)
    return BackfillCollectionResponse(
        backfills=backfills,
        total_entries=total_entries,
    )


@backfills_router.get(
    path="/{backfill_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
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
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
)
def create_backfill(
    backfill_request: BackfillPostBody,
    session: SessionDep,
) -> BackfillResponse | BackfillDryRunResponse:
    from_date = timezone.coerce_datetime(backfill_request.from_date)
    to_date = timezone.coerce_datetime(backfill_request.to_date)
    if not backfill_request.dry_run:
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
    else:
        serdag = session.scalar(SerializedDagModel.latest_item_select_object(backfill_request.dag_id))
        if not serdag:
            raise HTTPException(status_code=404, detail=f"Could not find dag {backfill_request.dag_id}")

        info_list = _get_info_list(
            dag=serdag.dag,
            from_date=from_date,
            to_date=to_date,
            reverse=backfill_request.run_backwards,
        )
        backfill_response_item = []
        print(info_list)
        for info in info_list:
            print(info.logical_date)
            dr = session.scalar(
                select(DagRun)
                .where(DagRun.logical_date == info.logical_date)
                .order_by(nulls_first(desc(DagRun.start_date), session))
                .limit(1)
            )

            if dr:
                non_create_reason = None
                if dr.state not in (DagRunState.SUCCESS, DagRunState.FAILED):
                    non_create_reason = BackfillDagRunExceptionReason.IN_FLIGHT
                elif backfill_request.reprocess_behavior is ReprocessBehavior.NONE:
                    non_create_reason = BackfillDagRunExceptionReason.ALREADY_EXISTS
                elif backfill_request.reprocess_behavior is ReprocessBehavior.COMPLETED:
                    if dr.state == DagRunState.FAILED:
                        non_create_reason = BackfillDagRunExceptionReason.UNKNOWN
                elif backfill_request.reprocess_behavior is ReprocessBehavior.FAILED:
                    if dr.state != DagRunState.FAILED:
                        non_create_reason = BackfillDagRunExceptionReason.ALREADY_EXISTS
                if not non_create_reason:
                    backfill_response_item.append(BackfillRunInfo(logical_date=info.logical_date))

            else:
                backfill_response_item.append(BackfillRunInfo(logical_date=info.logical_date))

        return BackfillDryRunResponse(run_info_list=backfill_response_item)
