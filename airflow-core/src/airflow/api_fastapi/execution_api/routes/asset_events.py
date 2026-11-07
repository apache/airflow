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

import hashlib
import json
from collections.abc import Callable
from typing import Annotated, NamedTuple

from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import (
    BaseParam,
    QueryAssetEventPartitionKeyFilter,
    QueryAssetEventPartitionKeyRegex,
)
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.datamodels.asset import AssetResponse
from airflow.api_fastapi.execution_api.datamodels.asset_event import (
    AssetEventReceipt,
    AssetEventResponse,
    AssetEventsResponse,
    EmitAssetEventBody,
)
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.security import (
    _IN_PROCESS_NON_TI_CALLER,
    CurrentTIToken,
    ExecutionAPIRoute,
    skip_auto_ti_attempt_live,
)
from airflow.assets.manager import AssetManager
from airflow.configuration import conf
from airflow.models.asset import AssetAliasModel, AssetEvent, AssetModel, TaskOutletAssetReference
from airflow.models.callback import Callback, ExecutorCallback
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.taskinstance import TaskInstance
from airflow.utils.sqlalchemy import JsonContains
from airflow.utils.state import CallbackState

router = APIRouter(
    route_class=ExecutionAPIRoute,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Asset not found"},
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
    },
)

EMISSION_ORIGIN = "emission"
# Prototype defaults; the AIP leaves the final values open.
MAX_EMISSION_EXTRA_BYTES = 64 * 1024
MAX_EMISSIONS_PER_SOURCE = 10_000


class _EmissionSource(NamedTuple):
    task_instance: TaskInstance | None
    dag_run: DagRun
    # Retries keep these values, so a repeated key stays in one scope; a clear changes clear_number.
    scope: tuple[str | int | None, ...]


def _get_dag_run(dag_id: str | None, run_id: str | None, *, session: Session) -> DagRun:
    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail={"reason": "not_found", "message": "Source Dag run not found"}
        )
    return dag_run


def _build_dag_run_source(dag_run: DagRun) -> _EmissionSource:
    return _EmissionSource(
        task_instance=None,
        dag_run=dag_run,
        scope=("dag_run", dag_run.dag_id, dag_run.run_id, dag_run.clear_number),
    )


def _build_task_source(ti: TaskInstance, *, session: Session) -> _EmissionSource:
    dag_run = _get_dag_run(ti.dag_id, ti.run_id, session=session)
    return _EmissionSource(
        task_instance=ti,
        dag_run=dag_run,
        # map_index alone does not identify a task instance: each region of a loop has its own.
        scope=(
            "task",
            ti.dag_id,
            ti.run_id,
            ti.task_id,
            str(ti.region_id),
            ti.map_index,
            dag_run.clear_number,
        ),
    )


def _require_emitting_attempt(ti: TaskInstance) -> None:
    # Task callbacks need no exception: the supervisor reports a failed or up_for_retry state only
    # after the task process exits, so the attempt is still live while its callbacks run.
    if not ti.working_set:
        raise HTTPException(
            status.HTTP_410_GONE,
            detail={"reason": "attempt_archived", "message": "This task attempt can no longer emit events"},
        )


def _resolve_emission_source(
    body: EmitAssetEventBody, request: Request, token: TIToken, *, session: Session
) -> _EmissionSource:
    if body.source_dag_id is not None or body.source_run_id is not None:
        # Only the trusted, in-process Dag processor transport names a callback's Dag run.
        if not request.scope.get(_IN_PROCESS_NON_TI_CALLER):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                detail={"reason": "forbidden", "message": "Callback provenance requires the Dag processor"},
            )
        return _build_dag_run_source(_get_dag_run(body.source_dag_id, body.source_run_id, session=session))

    ti = session.scalar(
        select(TaskInstance).where(TaskInstance.id == token.id).execution_options(include_all_attempts=True)
    )
    if ti is not None:
        _require_emitting_attempt(ti)
        return _build_task_source(ti, session=session)

    callback = session.get(Callback, token.id)
    if isinstance(callback, ExecutorCallback):
        if callback.state != CallbackState.RUNNING:
            raise HTTPException(
                status.HTTP_410_GONE,
                detail={"reason": "callback_finished", "message": "This callback can no longer emit events"},
            )
        dagrun_id = session.scalar(select(Deadline.dagrun_id).where(Deadline.callback_id == callback.id))
        dag_run = session.get(DagRun, dagrun_id) if dagrun_id is not None else None
        if dag_run is None:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                detail={"reason": "forbidden", "message": "This callback has no Dag run to emit for"},
            )
        return _build_dag_run_source(dag_run)

    raise HTTPException(
        status.HTTP_403_FORBIDDEN,
        detail={"reason": "forbidden", "message": "Asset emission requires a task or a callback"},
    )


def _get_emission_asset(body: EmitAssetEventBody, *, session: Session) -> AssetModel:
    query = select(AssetModel).options(joinedload(AssetModel.active))
    if body.name is not None:
        query = query.where(AssetModel.name == body.name)
    if body.uri is not None:
        query = query.where(AssetModel.uri == body.uri)
    assets = session.scalars(query).unique().all()
    active = [asset for asset in assets if asset.active is not None]
    if len(active) == 1:
        return active[0]
    if len(active) > 1:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={"reason": "asset_ambiguous", "message": "The reference matches more than one asset"},
        )
    if assets:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={"reason": "asset_inactive", "message": "No active Dag references this asset"},
        )
    raise HTTPException(
        status.HTTP_404_NOT_FOUND,
        detail={"reason": "asset_not_found", "message": "The asset is not in the Airflow database"},
    )


def _compute_hash(value: object) -> str:
    """Hash a value as canonical JSON; a NaN or infinite float raises ValueError."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _compute_payload_hash(asset: AssetModel, body: EmitAssetEventBody) -> str:
    try:
        extra_size = len(json.dumps(body.extra, separators=(",", ":"), allow_nan=False).encode())
        payload_hash = _compute_hash(
            {"asset_id": asset.id, "extra": body.extra, "partition_key": body.partition_key}
        )
    except ValueError:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "reason": "invalid_json",
                "message": "extra must be strict JSON, without NaN or Infinity",
            },
        )
    if extra_size > MAX_EMISSION_EXTRA_BYTES:
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE,
            detail={
                "reason": "extra_too_large",
                "message": f"extra is larger than {MAX_EMISSION_EXTRA_BYTES} bytes",
            },
        )
    return payload_hash


def _require_emission_list_entry(source: _EmissionSource, asset: AssetModel, *, session: Session) -> None:
    if not conf.getboolean("core", "multi_team"):
        return
    ti = source.task_instance
    if ti is None:
        # The consumer-team limits come from the emission list, and a Dag does not have one yet.
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "asset_not_in_emission_list",
                "message": "A Dag callback cannot emit while multi_team is on, because a Dag has no emission list",
            },
        )
    declared = session.scalar(
        select(TaskOutletAssetReference.asset_id).where(
            TaskOutletAssetReference.dag_id == ti.dag_id,
            TaskOutletAssetReference.task_id == ti.task_id,
            TaskOutletAssetReference.asset_id == asset.id,
        )
    )
    if declared is None:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "asset_not_in_emission_list",
                "message": "The task does not declare this asset",
            },
        )


def _lock_emission_source(source: _EmissionSource, *, session: Session) -> None:
    """Lock the row of the source until commit, so that its requests count the budget one at a time."""
    if source.task_instance is not None:
        session.execute(
            select(TaskInstance.id)
            .where(TaskInstance.id == source.task_instance.id)
            .with_for_update()
            .execution_options(include_all_attempts=True)
        )
    else:
        session.execute(select(DagRun.id).where(DagRun.id == source.dag_run.id).with_for_update())


def _require_emission_budget(source: _EmissionSource, *, session: Session) -> None:
    query = select(func.count()).select_from(AssetEvent).where(AssetEvent.origin == EMISSION_ORIGIN)
    if source.task_instance is not None:
        query = query.where(AssetEvent.source_task_instance_id == source.task_instance.id)
    else:
        query = query.where(
            AssetEvent.source_dag_id == source.dag_run.dag_id,
            AssetEvent.source_run_id == source.dag_run.run_id,
            AssetEvent.source_task_id.is_(None),
        )
    if (session.scalar(query) or 0) >= MAX_EMISSIONS_PER_SOURCE:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            detail={"reason": "emission_limit", "message": f"More than {MAX_EMISSIONS_PER_SOURCE} emissions"},
        )


def _replay(event: AssetEvent, payload_hash: str, response: Response) -> AssetEventReceipt:
    if event.payload_hash != payload_hash:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={
                "reason": "idempotency_conflict",
                "message": "The key was already used with a different payload",
                "event_id": event.id,
            },
        )
    response.status_code = status.HTTP_200_OK
    return AssetEventReceipt(id=event.id, timestamp=event.timestamp, replayed=True)


@router.post(
    "/emit",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
            status.HTTP_410_GONE,
            status.HTTP_413_CONTENT_TOO_LARGE,
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            status.HTTP_429_TOO_MANY_REQUESTS,
        ]
    ),
)
@skip_auto_ti_attempt_live
def emit_asset_event(
    *,
    body: EmitAssetEventBody,
    request: Request,
    response: Response,
    session: SessionDep,
    token: TIToken = CurrentTIToken,
) -> AssetEventReceipt:
    """Write an asset event now, independent of the outcome of the task or callback that emits it."""
    source = _resolve_emission_source(body, request, token, session=session)
    asset = _get_emission_asset(body, session=session)
    payload_hash = _compute_payload_hash(asset, body)
    _require_emission_list_entry(source, asset, session=session)
    scope_hash = _compute_hash([EMISSION_ORIGIN, asset.id, *source.scope, body.idempotency_key])
    if (
        existing := session.scalar(select(AssetEvent).where(AssetEvent.scope_hash == scope_hash))
    ) is not None:
        return _replay(existing, payload_hash, response)
    _lock_emission_source(source, session=session)
    _require_emission_budget(source, session=session)

    callbacks: list[Callable[[], None]] = []
    try:
        event = AssetManager.register_asset_change(
            asset=asset,
            task_instance=source.task_instance,
            dag_run=None if source.task_instance is not None else source.dag_run,
            extra=body.extra,
            partition_key=body.partition_key,
            origin=EMISSION_ORIGIN,
            idempotency_key=body.idempotency_key,
            scope_hash=scope_hash,
            payload_hash=payload_hash,
            callback_sink=callbacks,
            session=session,
        )
    except IntegrityError:
        # Another request with the same key committed first. This request wrote nothing before
        # the event, so rolling back the whole transaction loses nothing.
        session.rollback()
        existing = session.scalar(select(AssetEvent).where(AssetEvent.scope_hash == scope_hash))
        if existing is None:
            raise
        return _replay(existing, payload_hash, response)
    if event is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"reason": "asset_not_found", "message": "The asset is not in the Airflow database"},
        )
    receipt = AssetEventReceipt(id=event.id, timestamp=event.timestamp)
    # Listeners must not see an event that the database does not commit.
    session.commit()
    for callback in callbacks:
        callback()
    return receipt


def _get_asset_events_through_sql_clauses(
    *,
    join_clause,
    where_clause,
    session: SessionDep,
    ascending: bool = True,
    limit: int | None = None,
    filters: list[BaseParam] | None = None,
) -> AssetEventsResponse:
    order_by_clause = AssetEvent.timestamp.asc() if ascending else AssetEvent.timestamp.desc()
    asset_events_query = select(AssetEvent).join(join_clause).where(where_clause).order_by(order_by_clause)
    for filter_ in filters or []:
        asset_events_query = filter_.to_orm(asset_events_query)
    if limit:
        asset_events_query = asset_events_query.limit(limit)
    # A regexp partition-key filter bounds the query runtime automatically (its dependency applies
    # apply_regex_query_timeout to this request's session), so no explicit wrapping is needed here.
    asset_events = session.scalars(asset_events_query).all()
    return AssetEventsResponse.model_validate(
        {
            "asset_events": [
                AssetEventResponse(
                    id=event.id,
                    timestamp=event.timestamp,
                    extra=event.extra,
                    asset=AssetResponse(
                        name=event.asset.name,
                        uri=event.asset.uri,
                        group=event.asset.group,
                        extra=event.asset.extra,
                    ),
                    created_dagruns=event.created_dagruns,
                    source_task_id=event.source_task_id,
                    source_dag_id=event.source_dag_id,
                    source_run_id=event.source_run_id,
                    source_map_index=event.source_map_index,
                    partition_key=event.partition_key,
                )
                for event in asset_events
            ]
        }
    )


def _parse_extra_params(extra: list[str] | None) -> dict[str, str]:
    """Parse repeated ``key=value`` query params into a dict."""
    result: dict[str, str] = {}
    for item in extra or []:
        if "=" not in item:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "reason": "Invalid parameter",
                    "message": f"Invalid extra parameter format: {item!r}. Expected 'key=value'.",
                },
            )
        k, v = item.split("=", 1)
        result[k] = v
    return result


@router.get(
    "/by-asset",
    responses=create_openapi_http_exception_doc(
        [(status.HTTP_400_BAD_REQUEST, "Neither name nor uri was supplied")]
    ),
)
def get_asset_event_by_asset_name_uri(
    name: Annotated[str | None, Query(description="The name of the Asset")],
    uri: Annotated[str | None, Query(description="The URI of the Asset")],
    session: SessionDep,
    partition_key: QueryAssetEventPartitionKeyFilter,
    partition_key_regexp_pattern: QueryAssetEventPartitionKeyRegex,
    after: Annotated[UtcDateTime | None, Query(description="The start of the time range")] = None,
    before: Annotated[UtcDateTime | None, Query(description="The end of the time range")] = None,
    ascending: Annotated[bool, Query(description="Whether to sort results in ascending order")] = True,
    limit: Annotated[int | None, Query(description="The maximum number of results to return")] = None,
    extra: Annotated[
        list[str] | None,
        Query(
            description="Filter by extra JSON key-value pairs. Format: key=value. Repeat for AND logic.",
        ),
    ] = None,
) -> AssetEventsResponse:
    if name and uri:
        where_clause = and_(AssetModel.name == name, AssetModel.uri == uri)
    elif uri:
        where_clause = and_(AssetModel.uri == uri, AssetModel.active.has())
    elif name:
        where_clause = and_(AssetModel.name == name, AssetModel.active.has())
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "reason": "Missing parameter",
                "message": "name and uri cannot both be None",
            },
        )

    if after:
        where_clause = and_(where_clause, AssetEvent.timestamp >= after)
    if before:
        where_clause = and_(where_clause, AssetEvent.timestamp <= before)
    extra_dict = _parse_extra_params(extra)
    if extra_dict:
        where_clause = and_(where_clause, JsonContains(AssetEvent.extra, extra_dict))

    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.asset,
        where_clause=where_clause,
        session=session,
        ascending=ascending,
        limit=limit,
        filters=[partition_key, partition_key_regexp_pattern],
    )


@router.get("/by-asset-alias")
def get_asset_event_by_asset_alias(
    name: Annotated[str, Query(description="The name of the Asset Alias")],
    session: SessionDep,
    partition_key: QueryAssetEventPartitionKeyFilter,
    partition_key_regexp_pattern: QueryAssetEventPartitionKeyRegex,
    after: Annotated[UtcDateTime | None, Query(description="The start of the time range")] = None,
    before: Annotated[UtcDateTime | None, Query(description="The end of the time range")] = None,
    ascending: Annotated[bool, Query(description="Whether to sort results in ascending order")] = True,
    limit: Annotated[int | None, Query(description="The maximum number of results to return")] = None,
    extra: Annotated[
        list[str] | None,
        Query(
            description="Filter by extra JSON key-value pairs. Format: key=value. Repeat for AND logic.",
        ),
    ] = None,
) -> AssetEventsResponse:
    where_clause = AssetAliasModel.name == name
    if after:
        where_clause = and_(where_clause, AssetEvent.timestamp >= after)
    if before:
        where_clause = and_(where_clause, AssetEvent.timestamp <= before)
    extra_dict = _parse_extra_params(extra)
    if extra_dict:
        where_clause = and_(where_clause, JsonContains(AssetEvent.extra, extra_dict))

    return _get_asset_events_through_sql_clauses(
        join_clause=AssetEvent.source_aliases,
        where_clause=where_clause,
        session=session,
        ascending=ascending,
        limit=limit,
        filters=[partition_key, partition_key_regexp_pattern],
    )
