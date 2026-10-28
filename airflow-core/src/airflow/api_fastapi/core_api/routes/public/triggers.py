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

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import contains_eager, defer, selectinload

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.db.triggers import (
    TriggerKind,
    build_has_owner_expression,
    get_trigger_kind,
    get_trigger_status,
)
from airflow.api_fastapi.common.parameters.base import QueryLimit, QueryOffset
from airflow.api_fastapi.common.parameters.sort import SortParam
from airflow.api_fastapi.common.parameters.trigger import (
    QueryTriggerAssetIdFilter,
    QueryTriggerDagIdFilter,
    QueryTriggererHostnameFilter,
    QueryTriggerKindFilter,
    QueryTriggerStatusFilter,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.trigger import (
    TriggerAssetWatcherReferenceResponse,
    TriggerCallbackReferenceResponse,
    TriggerCollectionResponse,
    TriggerDetailResponse,
    TriggerTaskInstanceReferenceResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import ReadableTriggersFilterDep
from airflow.models.asset import AssetWatcherModel
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.task_coordinates import get_public_map_index
from airflow.models.trigger import Trigger

if TYPE_CHECKING:
    from collections.abc import Iterable
    from uuid import UUID

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

triggers_router = AirflowRouter(tags=["Trigger"], prefix="/triggers")


def _triggers_select() -> Select:
    # Only triggers their owner still refers to: the rest are deleted on the triggerer's next cleanup. The
    # kwargs are never returned, so they are not even loaded.
    return (
        select(Trigger)
        .outerjoin(Trigger.triggerer_job)
        .where(build_has_owner_expression())
        .options(
            defer(Trigger.encrypted_kwargs),
            contains_eager(Trigger.triggerer_job),
            selectinload(Trigger.asset_watchers).joinedload(AssetWatcherModel.asset),
            selectinload(Trigger.callback),
        )
    )


def _get_callback_dag_runs(triggers: Iterable[Trigger], *, session: Session) -> dict[UUID, tuple[str, str]]:
    """Return the Dag id and run id of each callback's deadline; a callback names its run only there."""
    callback_ids = [trigger.callback.id for trigger in triggers if trigger.callback is not None]
    if not callback_ids:
        return {}
    rows = session.execute(
        select(Deadline.callback_id, DagRun.dag_id, DagRun.run_id)
        .join(Deadline.dagrun)
        .where(Deadline.callback_id.in_(callback_ids))
    )
    return {callback_id: (dag_id, run_id) for callback_id, dag_id, run_id in rows}


def _build_response(
    trigger: Trigger,
    *,
    readable_asset_ids: set[int],
    callback_dag_runs: dict[UUID, tuple[str, str]],
    session: Session,
) -> TriggerDetailResponse | None:
    """
    Describe a trigger and its owner, the same way for every kind; ``None`` if the owner just let go.

    A trigger can serve watchers on several assets; only those on assets the caller can read are listed.
    """
    if (kind := get_trigger_kind(trigger)) is None:
        return None
    ti = trigger.task_instance if kind == TriggerKind.TASK else None
    callback = trigger.callback if kind == TriggerKind.CALLBACK else None
    callback_dag_id, callback_dag_run_id = (
        callback_dag_runs.get(callback.id, (None, None)) if callback is not None else (None, None)
    )
    job = trigger.triggerer_job
    return TriggerDetailResponse(
        id=trigger.id,
        kind=kind,
        classpath=trigger.classpath,
        created_date=trigger.created_date,
        queue=trigger.queue,
        team_name=trigger.team_name,
        status=get_trigger_status(trigger),
        triggerer_hostname=job.hostname if job is not None else None,
        dag_id=ti.dag_id if ti is not None else callback_dag_id,
        task_instance=None
        if ti is None
        else TriggerTaskInstanceReferenceResponse(
            dag_id=ti.dag_id,
            dag_run_id=ti.run_id,
            task_id=ti.task_id,
            map_index=get_public_map_index(ti, session=session),
            try_number=ti.try_number,
            state=ti.state,
        ),
        callback=None
        if callback is None
        else TriggerCallbackReferenceResponse(
            id=str(callback.id),
            dag_id=callback_dag_id,
            dag_run_id=callback_dag_run_id,
            state=callback.state,
        ),
        asset_watchers=[
            TriggerAssetWatcherReferenceResponse(
                asset_id=watcher.asset_id,
                asset_name=watcher.asset.name,
                asset_uri=watcher.asset.uri,
                watcher_name=watcher.name,
            )
            for watcher in sorted(trigger.asset_watchers, key=lambda w: (w.asset_id, w.name))
            if watcher.asset_id in readable_asset_ids
        ],
    )


@triggers_router.get("", responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]))
def get_triggers(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "created_date"], Trigger).dynamic_depends(default="id")),
    ],
    kind: QueryTriggerKindFilter,
    trigger_status: QueryTriggerStatusFilter,
    triggerer_hostname: QueryTriggererHostnameFilter,
    dag_id: QueryTriggerDagIdFilter,
    asset_id: QueryTriggerAssetIdFilter,
    readable_triggers_filter: ReadableTriggersFilterDep,
    session: SessionDep,
) -> TriggerCollectionResponse:
    """
    Get the triggers the caller can see, of every kind, and how each is doing.

    A trigger is visible to whoever can see its owner: the Dag of its task instance or callback, or an asset
    one of its watchers watches.
    """
    if asset_id.value is not None and asset_id.value not in readable_triggers_filter.asset_ids:
        # Which triggers watch an asset is the asset's to show.
        return TriggerCollectionResponse(triggers=[], total_entries=0)
    triggers_select, total_entries = paginated_select(
        statement=_triggers_select(),
        filters=[kind, trigger_status, triggerer_hostname, dag_id, asset_id, readable_triggers_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    triggers = session.scalars(triggers_select).unique().all()
    callback_dag_runs = _get_callback_dag_runs(triggers, session=session)
    responses = (
        _build_response(
            trigger,
            readable_asset_ids=readable_triggers_filter.asset_ids,
            callback_dag_runs=callback_dag_runs,
            session=session,
        )
        for trigger in triggers
    )
    return TriggerCollectionResponse(
        triggers=[response for response in responses if response is not None],
        total_entries=total_entries,
    )


@triggers_router.get(
    "/{trigger_id}", responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND])
)
def get_trigger(
    trigger_id: int,
    readable_triggers_filter: ReadableTriggersFilterDep,
    session: SessionDep,
) -> TriggerDetailResponse:
    """Get a trigger, who it runs for, and how it is doing."""
    trigger = session.scalars(
        readable_triggers_filter.to_orm(_triggers_select()).where(Trigger.id == trigger_id).limit(1)
    ).one_or_none()
    response = (
        None
        if trigger is None
        else _build_response(
            trigger,
            readable_asset_ids=readable_triggers_filter.asset_ids,
            callback_dag_runs=_get_callback_dag_runs([trigger], session=session),
            session=session,
        )
    )
    if response is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The trigger with ID `{trigger_id}` was not found")
    return response
