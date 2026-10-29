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

from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select, union_all
from sqlalchemy.orm import contains_eager, defer, selectinload

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.headers import HeaderAcceptJsonOrNdjson
from airflow.api_fastapi.common.parameters.base import QueryLimit, QueryOffset
from airflow.api_fastapi.common.parameters.sort import SortParam
from airflow.api_fastapi.common.parameters.trigger import (
    QueryTriggerAssetIdFilter,
    QueryTriggerDagIdFilter,
    QueryTriggererHostnameFilter,
    QueryTriggerKindFilter,
    QueryTriggerStatusFilter,
    QueryTriggerTeamsFilter,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
from airflow.api_fastapi.core_api.datamodels.log import TaskInstancesLogResponse
from airflow.api_fastapi.core_api.datamodels.trigger import (
    TriggerAssetWatcherReferenceResponse,
    TriggerCallbackReferenceResponse,
    TriggerCollectionResponse,
    TriggerDetailResponse,
    TriggerTaskInstanceReferenceResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.routes.public.log import (
    _buffered_ndjson_stream,
    ndjson_example_response_for_get_log,
)
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
    ReadableTriggersFilterDep,
    requires_access_trigger,
)
from airflow.configuration import conf
from airflow.models.asset import (
    AssetModel,
    AssetWatcherModel,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
)
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.dynamic_region import SENTINEL_REGION_ID
from airflow.models.task_coordinates import public_map_index_expression
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import Trigger, TriggerKind
from airflow.utils.log.trigger_log_reader import read_trigger_logs

if TYPE_CHECKING:
    from collections.abc import Iterable
    from uuid import UUID

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import ColumnElement, Select

    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser

triggers_router = AirflowRouter(tags=["Trigger"], prefix="/triggers")


def _sort_null_as_smallest(column: ColumnElement) -> list[ColumnElement]:
    # Postgres sorts NULL as the largest value, MySQL and SQLite as the smallest; without the leading key a
    # descending sort would list the triggers that never failed (or never fired) first on Postgres only.
    return [column.is_not(None).label(f"has_{column.key}"), column]


def _build_triggers_select() -> Select:
    # Only triggers their owner still refers to: the rest are deleted on the triggerer's next cleanup. The
    # kwargs are never returned, so they are not even loaded.
    return (
        select(Trigger)
        .outerjoin(Trigger.triggerer_job)
        .where(Trigger.owner_still_refers_expression())
        .options(
            defer(Trigger.encrypted_kwargs),
            contains_eager(Trigger.triggerer_job),
            selectinload(Trigger.asset_watchers).joinedload(AssetWatcherModel.asset),
            selectinload(Trigger.callback),
        )
    )


def _get_callback_dag_run_ids(triggers: Iterable[Trigger], *, session: Session) -> dict[UUID, str]:
    """Return the run id of each callback's Dag run; a callback names its run only through its deadline."""
    callback_ids = [trigger.callback.id for trigger in triggers if trigger.callback is not None]
    if not callback_ids:
        return {}
    rows = session.execute(
        select(Deadline.callback_id, DagRun.run_id)
        .join(Deadline.dagrun)
        .where(Deadline.callback_id.in_(callback_ids))
    )
    return {callback_id: run_id for callback_id, run_id in rows}


def _get_public_map_indexes(triggers: Iterable[Trigger], *, session: Session) -> dict[UUID, int]:
    """Return the public map index of each trigger's task instance; one query for those in a dynamic region."""
    task_instances = [trigger.task_instance for trigger in triggers if trigger.task_instance is not None]
    map_indexes = {ti.id: ti.region_index for ti in task_instances if ti.region_id == SENTINEL_REGION_ID}
    if regional := [ti.id for ti in task_instances if ti.id not in map_indexes]:
        rows = session.execute(
            select(TaskInstance.id, public_map_index_expression(TaskInstance))
            .where(TaskInstance.id.in_(regional))
            .execution_options(include_all_attempts=True)
        )
        map_indexes.update({ti_id: map_index for ti_id, map_index in rows})
    return map_indexes


def _build_response(
    trigger: Trigger,
    *,
    readable_asset_ids: set[int],
    callback_dag_run_ids: dict[UUID, str],
    public_map_indexes: dict[UUID, int],
) -> TriggerDetailResponse | None:
    """
    Describe a trigger and its owner, the same way for every kind; ``None`` if the owner just let go.

    A trigger can serve watchers on several assets; only those on assets the caller can read are listed.
    """
    if (kind := trigger.kind) is None:
        return None
    task_instance = None
    if kind == TriggerKind.TASK:
        ti = trigger.task_instance
        if ti is None or (map_index := public_map_indexes.get(ti.id)) is None:
            # Its task instance was deleted (with its Dag or run) after the page was read.
            return None
        task_instance = TriggerTaskInstanceReferenceResponse(
            dag_id=ti.dag_id,
            dag_run_id=ti.run_id,
            task_id=ti.task_id,
            map_index=map_index,
            try_number=ti.try_number,
            state=ti.state,
        )
    callback = trigger.callback if kind == TriggerKind.CALLBACK else None
    job = trigger.triggerer_job
    return TriggerDetailResponse(
        id=trigger.id,
        kind=kind,
        classpath=trigger.classpath,
        created_date=trigger.created_date,
        queue=trigger.queue,
        team_name=trigger.team_name,
        status=trigger.status(),
        triggerer_hostname=job.hostname if job is not None else None,
        consecutive_failures=trigger.consecutive_failures or 0,
        first_failed_at=trigger.first_failed_at,
        last_failed_at=trigger.last_failed_at,
        last_error=trigger.last_error,
        last_event_at=trigger.last_event_at,
        dag_id=trigger.dag_id,
        task_instance=task_instance,
        callback=None
        if callback is None
        else TriggerCallbackReferenceResponse(
            id=str(callback.id),
            dag_id=trigger.dag_id,
            dag_run_id=callback_dag_run_ids.get(callback.id),
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


@triggers_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]),
    dependencies=[Depends(requires_access_trigger(method="GET"))],
)
def get_triggers(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",
                    "created_date",
                    "consecutive_failures",
                    "last_failed_at",
                    "last_event_at",
                ],
                Trigger,
                to_replace={
                    name: _sort_null_as_smallest(getattr(Trigger, name))
                    for name in ("consecutive_failures", "last_failed_at", "last_event_at")
                },
            ).dynamic_depends(default="id")
        ),
    ],
    kind: QueryTriggerKindFilter,
    trigger_status: QueryTriggerStatusFilter,
    triggerer_hostname: QueryTriggererHostnameFilter,
    dag_id: QueryTriggerDagIdFilter,
    asset_id: QueryTriggerAssetIdFilter,
    teams: QueryTriggerTeamsFilter,
    readable_triggers_filter: ReadableTriggersFilterDep,
    session: SessionDep,
) -> TriggerCollectionResponse:
    """
    Get the triggers the caller can see, of every kind, and how each is doing.

    A trigger is visible to whoever can see its owner: the task instance or Dag run of its Dag, or an asset
    one of its watchers watches.
    """
    if asset_id.value is not None and asset_id.value not in readable_triggers_filter.asset_ids:
        # Which triggers watch an asset is the asset's to show.
        return TriggerCollectionResponse(triggers=[], total_entries=0)
    triggers_select, total_entries = paginated_select(
        statement=_build_triggers_select(),
        filters=[
            kind,
            trigger_status,
            triggerer_hostname,
            dag_id,
            asset_id,
            teams,
            readable_triggers_filter,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    triggers = session.scalars(triggers_select).unique().all()
    callback_dag_run_ids = _get_callback_dag_run_ids(triggers, session=session)
    public_map_indexes = _get_public_map_indexes(triggers, session=session)
    responses = (
        _build_response(
            trigger,
            readable_asset_ids=readable_triggers_filter.asset_ids,
            callback_dag_run_ids=callback_dag_run_ids,
            public_map_indexes=public_map_indexes,
        )
        for trigger in triggers
    )
    return TriggerCollectionResponse(
        triggers=[response for response in responses if response is not None],
        total_entries=total_entries,
    )


@triggers_router.get(
    "/{trigger_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_trigger(method="GET"))],
)
def get_trigger(
    trigger_id: int,
    readable_triggers_filter: ReadableTriggersFilterDep,
    session: SessionDep,
) -> TriggerDetailResponse:
    """Get a trigger, who it runs for, and how it is doing."""
    trigger = session.scalars(
        readable_triggers_filter.to_orm(_build_triggers_select()).where(Trigger.id == trigger_id).limit(1)
    ).one_or_none()
    response = (
        None
        if trigger is None
        else _build_response(
            trigger,
            readable_asset_ids=readable_triggers_filter.asset_ids,
            callback_dag_run_ids=_get_callback_dag_run_ids([trigger], session=session),
            public_map_indexes=_get_public_map_indexes([trigger], session=session),
        )
    )
    if response is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The trigger with ID `{trigger_id}` was not found")
    return response


def _can_read_trigger_logs(
    trigger: Trigger, user: BaseUser, *, readable_asset_ids: set[int], session: Session
) -> bool:
    """
    Whether the caller, who can see the trigger, may read its logs: whoever may read its owner's logs.

    That is task log access to the Dag of its task instance or callback, the check their own log routes make.
    An asset watcher belongs to no single Dag, so it takes read access to one of its assets and task log access
    to a Dag scheduled on it (directly or by an asset name or URI reference), in the trigger's team.
    """
    auth_manager = get_auth_manager()
    if trigger.kind in (TriggerKind.TASK, TriggerKind.CALLBACK):
        dag_ids = {trigger.dag_id} if trigger.dag_id is not None else set()
        same_team_only = False
    else:
        asset_ids = {w.asset_id for w in trigger.asset_watchers} & readable_asset_ids
        # On the asset itself or on Asset.ref(name=...) / Asset.ref(uri=...) of it, as its events queue them.
        refs = ((DagScheduleAssetNameReference, "name"), (DagScheduleAssetUriReference, "uri"))
        by_ref = [
            select(ref.dag_id)
            .join(AssetModel, getattr(AssetModel, column) == getattr(ref, column))
            .where(AssetModel.id.in_(asset_ids))
            for ref, column in refs
        ]
        direct = select(DagScheduleAssetReference.dag_id).where(
            DagScheduleAssetReference.asset_id.in_(asset_ids)
        )
        dag_ids = set(session.scalars(union_all(direct, *by_ref)))
        same_team_only = conf.getboolean("core", "multi_team")
    dag_teams = DagModel.get_dag_id_to_team_name_mapping(list(dag_ids), session=session)
    return any(
        auth_manager.is_authorized_dag(
            method="GET",
            access_entity=DagAccessEntity.TASK_LOGS,
            details=DagDetails(id=dag_id, team_name=dag_teams.get(dag_id)),
            user=user,
        )
        for dag_id in sorted(dag_ids)
        if not same_team_only or dag_teams.get(dag_id) == trigger.team_name
    )


@triggers_router.get(
    "/{trigger_id}/logs",
    responses={
        **create_openapi_http_exception_doc([status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]),
        status.HTTP_200_OK: {
            "description": "Successful Response",
            "content": ndjson_example_response_for_get_log,
        },
    },
    response_model=TaskInstancesLogResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(requires_access_trigger(method="GET"))],
)
def get_trigger_logs(
    trigger_id: int,
    accept: HeaderAcceptJsonOrNdjson,
    user: GetUserDep,
    readable_triggers_filter: ReadableTriggersFilterDep,
    session: SessionDep,
    tail_lines: Annotated[
        int, Query(ge=1, le=10_000, description="Number of most recent log lines to return.")
    ] = 200,
):
    """
    Get the most recent lines a trigger logged, whatever its kind.

    For a deferred task's trigger, the trigger part of that task try's log; for a callback's trigger, the
    callback's log; for an asset watcher, its own log files. Lines come from the triggerer running it now,
    from remote logging, and from the API server's own log folder, merged in time order. The trigger must be
    visible to the caller, as in GET /triggers/{trigger_id}, and its owner's logs readable.
    """
    # Checked in Python: in SQL the filter binds every readable Dag and asset to read one row.
    trigger = session.scalars(_build_triggers_select().where(Trigger.id == trigger_id).limit(1)).one_or_none()
    if trigger is None or trigger.kind is None or not readable_triggers_filter.permits(trigger):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The trigger with ID `{trigger_id}` was not found")
    if not _can_read_trigger_logs(
        trigger, user, readable_asset_ids=readable_triggers_filter.asset_ids, session=session
    ):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "Reading a trigger's logs needs task log access to its owner's Dag"
        )
    log_stream = read_trigger_logs(trigger, tail_lines=tail_lines, session=session)

    if accept == Mimetype.NDJSON:
        return StreamingResponse(
            media_type="application/x-ndjson",
            content=_buffered_ndjson_stream(f"{log.model_dump_json()}\n" for log in log_stream),
        )
    return TaskInstancesLogResponse.model_construct(content=log_stream, continuation_token=None)
