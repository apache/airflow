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
from collections import defaultdict
from typing import Annotated
from uuid import UUID

import attrs
from fastapi import Body, HTTPException, status
from pydantic import JsonValue
from sqlalchemy import tuple_, update
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from sqlalchemy.sql import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun,
    PrevSuccessfulDagRunResponse,
    TIDeferredStatePayload,
    TIEnterRunningPayload,
    TIHeartbeatInfo,
    TIRescheduleStatePayload,
    TIRunContext,
    TIStateUpdate,
    TISuccessStatePayload,
    TITerminalStatePayload,
)
from airflow.assets.manager import asset_manager
from airflow.exceptions import AirflowInactiveAssetAddedToAssetAliasException
from airflow.models.asset import AssetModel
from airflow.models.dagrun import DagRun as DR
from airflow.models.taskinstance import TaskInstance as TI, _update_rtif
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.trigger import Trigger
from airflow.models.xcom import XCom
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetUniqueKey
from airflow.utils import timezone
from airflow.utils.state import DagRunState, State, TerminalTIState

# TODO: Add dependency on JWT token
router = AirflowRouter()


log = logging.getLogger(__name__)


@router.patch(
    "/{task_instance_id}/run",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {"description": "The TI is already in the requested state"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid payload for the state transition"},
    },
)
def ti_run(
    task_instance_id: UUID, ti_run_payload: Annotated[TIEnterRunningPayload, Body()], session: SessionDep
) -> TIRunContext:
    """
    Run a TaskInstance.

    This endpoint is used to start a TaskInstance that is in the QUEUED state.
    """
    # We only use UUID above for validation purposes
    ti_id_str = str(task_instance_id)

    old = (
        select(TI.state, TI.dag_id, TI.run_id, TI.task_id, TI.map_index, TI.next_method, TI.max_tries)
        .where(TI.id == ti_id_str)
        .with_for_update()
    )
    try:
        (previous_state, dag_id, run_id, task_id, map_index, next_method, max_tries) = session.execute(
            old
        ).one()
    except NoResultFound:
        log.error("Task Instance %s not found", ti_id_str)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    # We exclude_unset to avoid updating fields that are not set in the payload
    data = ti_run_payload.model_dump(exclude_unset=True)

    query = update(TI).where(TI.id == ti_id_str).values(data)

    # TODO: We will need to change this for other states like:
    #   reschedule, retry, defer etc.
    if previous_state != State.QUEUED:
        log.warning(
            "Can not start Task Instance ('%s') in invalid state: %s",
            ti_id_str,
            previous_state,
        )

        # TODO: Pass a RFC 9457 compliant error message in "detail" field
        # https://datatracker.ietf.org/doc/html/rfc9457
        # to provide more information about the error
        # FastAPI will automatically convert this to a JSON response
        # This might be added in FastAPI in https://github.com/fastapi/fastapi/issues/10370
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "invalid_state",
                "message": "TI was not in a state where it could be marked as running",
                "previous_state": previous_state,
            },
        )
    log.info("Task with %s state started on %s ", previous_state, ti_run_payload.hostname)
    # Ensure there is no end date set.
    query = query.values(
        end_date=None,
        hostname=ti_run_payload.hostname,
        unixname=ti_run_payload.unixname,
        pid=ti_run_payload.pid,
        state=State.RUNNING,
    )

    try:
        result = session.execute(query)
        log.info("TI %s state updated: %s row(s) affected", ti_id_str, result.rowcount)

        dr = session.execute(
            select(
                DR.run_id,
                DR.dag_id,
                DR.data_interval_start,
                DR.data_interval_end,
                DR.start_date,
                DR.end_date,
                DR.run_type,
                DR.conf,
                DR.logical_date,
            ).filter_by(dag_id=dag_id, run_id=run_id)
        ).one_or_none()

        if not dr:
            raise ValueError(f"DagRun with dag_id={dag_id} and run_id={run_id} not found.")

        # Clear XCom data for the task instance since we are certain it is executing
        # However, do not clear it for deferral
        if not next_method:
            if map_index < 0:
                map_index = None
            log.info("Clearing xcom data for task id: %s", ti_id_str)
            XCom.clear(
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                map_index=map_index,
                session=session,
            )

        return TIRunContext(
            dag_run=DagRun.model_validate(dr, from_attributes=True),
            max_tries=max_tries,
            # TODO: Add variables and connections that are needed (and has perms) for the task
            variables=[],
            connections=[],
        )
    except SQLAlchemyError as e:
        log.error("Error marking Task Instance state as running: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error occurred"
        )


@router.patch(
    "/{task_instance_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    # TODO: Add description to the operation
    # TODO: Add Operation ID to control the function name in the OpenAPI spec
    # TODO: Do we need to use create_openapi_http_exception_doc here?
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {"description": "The TI is already in the requested state"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid payload for the state transition"},
    },
)
def ti_update_state(
    task_instance_id: UUID,
    ti_patch_payload: Annotated[TIStateUpdate, Body()],
    session: SessionDep,
):
    """
    Update the state of a TaskInstance.

    Not all state transitions are valid, and transitioning to some states requires extra information to be
    passed along. (Check out the datamodels for details, the rendered docs might not reflect this accurately)
    """
    updated_state: str = ""

    # We only use UUID above for validation purposes
    ti_id_str = str(task_instance_id)

    old = select(TI.state, TI.try_number, TI.max_tries).where(TI.id == ti_id_str).with_for_update()
    try:
        (
            previous_state,
            try_number,
            max_tries,
        ) = session.execute(old).one()
    except NoResultFound:
        log.error("Task Instance %s not found", ti_id_str)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    # We exclude_unset to avoid updating fields that are not set in the payload
    data = ti_patch_payload.model_dump(
        exclude={"task_outlets", "outlet_events", "asset_type"}, exclude_unset=True
    )

    query = update(TI).where(TI.id == ti_id_str).values(data)

    if isinstance(ti_patch_payload, TITerminalStatePayload):
        query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)
        updated_state = ti_patch_payload.state
        # if we get failed, we should attempt to retry, as it is a more
        # normal state. Tasks with retries are more frequent than without retries.
        if ti_patch_payload.state == TerminalTIState.FAIL_WITHOUT_RETRY:
            updated_state = State.FAILED
        elif ti_patch_payload.state == State.FAILED:
            if _is_eligible_to_retry(previous_state, try_number, max_tries):
                updated_state = State.UP_FOR_RETRY
            else:
                updated_state = State.FAILED
        query = query.values(state=updated_state)
    elif isinstance(ti_patch_payload, TISuccessStatePayload):
        print("Got payload" * 10, ti_patch_payload)
        query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)
        updated_state = ti_patch_payload.state
        task_instance = session.get(TI, ti_id_str)
        register_asset_changes(
            task_instance,
            ti_patch_payload.task_outlets,
            ti_patch_payload.outlet_events,
            ti_patch_payload.asset_type,
            session,
        )
        query = query.values(state=updated_state)
    elif isinstance(ti_patch_payload, TIDeferredStatePayload):
        # Calculate timeout if it was passed
        timeout = None
        if ti_patch_payload.trigger_timeout is not None:
            timeout = timezone.utcnow() + ti_patch_payload.trigger_timeout

        trigger_row = Trigger(
            classpath=ti_patch_payload.classpath,
            kwargs=ti_patch_payload.trigger_kwargs,
        )
        session.add(trigger_row)
        session.flush()

        # TODO: HANDLE execution timeout later as it requires a call to the DB
        # either get it from the serialised DAG or get it from the API

        query = update(TI).where(TI.id == ti_id_str)
        query = query.values(
            state=State.DEFERRED,
            trigger_id=trigger_row.id,
            next_method=ti_patch_payload.next_method,
            next_kwargs=ti_patch_payload.trigger_kwargs,
            trigger_timeout=timeout,
        )
        updated_state = State.DEFERRED
    elif isinstance(ti_patch_payload, TIRescheduleStatePayload):
        task_instance = session.get(TI, ti_id_str)
        actual_start_date = timezone.utcnow()
        session.add(
            TaskReschedule(
                task_instance.task_id,
                task_instance.dag_id,
                task_instance.run_id,
                task_instance.try_number,
                actual_start_date,
                ti_patch_payload.end_date,
                ti_patch_payload.reschedule_date,
                task_instance.map_index,
            )
        )

        query = update(TI).where(TI.id == ti_id_str)
        # calculate the duration for TI table too
        query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)
        # clear the next_method and next_kwargs so that none of the retries pick them up
        query = query.values(state=State.UP_FOR_RESCHEDULE, next_method=None, next_kwargs=None)
        updated_state = State.UP_FOR_RESCHEDULE
    # TODO: Replace this with FastAPI's Custom Exception handling:
    # https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers
    try:
        result = session.execute(query)
        log.info("TI %s state updated to %s: %s row(s) affected", ti_id_str, updated_state, result.rowcount)
    except SQLAlchemyError as e:
        log.error("Error updating Task Instance state: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error occurred"
        )


@router.put(
    "/{task_instance_id}/heartbeat",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {
            "description": "The TI attempting to heartbeat should be terminated for the given reason"
        },
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid payload for the state transition"},
    },
)
def ti_heartbeat(
    task_instance_id: UUID,
    ti_payload: TIHeartbeatInfo,
    session: SessionDep,
):
    """Update the heartbeat of a TaskInstance to mark it as alive & still running."""
    ti_id_str = str(task_instance_id)

    # Hot path: since heartbeating a task is a very common operation, we try to do minimize the number of queries
    # and DB round trips as much as possible.

    old = select(TI.state, TI.hostname, TI.pid).where(TI.id == ti_id_str).with_for_update()

    try:
        (previous_state, hostname, pid) = session.execute(old).one()
    except NoResultFound:
        log.error("Task Instance %s not found", ti_id_str)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    if hostname != ti_payload.hostname or pid != ti_payload.pid:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "running_elsewhere",
                "message": "TI is already running elsewhere",
                "current_hostname": hostname,
                "current_pid": pid,
            },
        )

    if previous_state != State.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "not_running",
                "message": "TI is no longer in the running state and task should terminate",
                "current_state": previous_state,
            },
        )

    # Update the last heartbeat time!
    session.execute(update(TI).where(TI.id == ti_id_str).values(last_heartbeat_at=timezone.utcnow()))
    log.debug("Task with %s state heartbeated", previous_state)


@router.put(
    "/{task_instance_id}/rtif",
    status_code=status.HTTP_201_CREATED,
    # TODO: Add description to the operation
    # TODO: Add Operation ID to control the function name in the OpenAPI spec
    # TODO: Do we need to use create_openapi_http_exception_doc here?
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Invalid payload for the setting rendered task instance fields"
        },
    },
)
def ti_put_rtif(
    task_instance_id: UUID,
    put_rtif_payload: Annotated[dict[str, JsonValue], Body()],
    session: SessionDep,
):
    """Add an RTIF entry for a task instance, sent by the worker."""
    ti_id_str = str(task_instance_id)
    task_instance = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not task_instance:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
        )
    _update_rtif(task_instance, put_rtif_payload, session)

    return {"message": "Rendered task instance fields successfully set"}


@router.get(
    "/{task_instance_id}/previous-successful-dagrun",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance or Dag Run not found"},
    },
)
def get_previous_successful_dagrun(
    task_instance_id: UUID, session: SessionDep
) -> PrevSuccessfulDagRunResponse:
    """
    Get the previous successful DagRun for a TaskInstance.

    The data from this endpoint is used to get values for Task Context.
    """
    ti_id_str = str(task_instance_id)
    task_instance = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not task_instance:
        return PrevSuccessfulDagRunResponse()

    dag_run = session.scalar(
        select(DR)
        .where(
            DR.dag_id == task_instance.dag_id,
            DR.logical_date < task_instance.logical_date,
            DR.state == DagRunState.SUCCESS,
        )
        .order_by(DR.logical_date.desc())
        .limit(1)
    )
    if not dag_run:
        return PrevSuccessfulDagRunResponse()

    return PrevSuccessfulDagRunResponse.model_validate(dag_run)


def _is_eligible_to_retry(state: str, try_number: int, max_tries: int) -> bool:
    """Is task instance is eligible for retry."""
    if state == State.RESTARTING:
        # If a task is cleared when running, it goes into RESTARTING state and is always
        # eligible for retry
        return True

    # max_tries is initialised with the retries defined at task level, we do not need to explicitly ask for
    # retries from the task SDK now, we can handle using max_tries
    return max_tries != 0 and try_number <= max_tries


def register_asset_changes(task_instance, task_outlets, outlet_events, asset_type, session):
    # One task only triggers one asset event for each asset with the same extra.
    # This tuple[asset uri, extra] to sets alias names mapping is used to find whether
    # there're assets with same uri but different extra that we need to emit more than one asset events.
    asset_alias_names: dict[tuple[AssetUniqueKey, frozenset], set[str]] = defaultdict(set)
    asset_name_refs: set[str] = set()
    asset_uri_refs: set[str] = set()

    for obj in task_outlets:
        # Lineage can have other types of objects besides assets
        if asset_type == "Asset":
            asset_manager.register_asset_change(
                task_instance=task_instance,
                asset=Asset(name=obj.name, uri=obj.uri),
                extra=outlet_events,
                session=session,
            )
        elif asset_type == "AssetNameRef":
            asset_name_refs.add(obj.name)
        elif asset_type == "AssetUriRef":
            asset_uri_refs.add(obj.uri)

    if asset_type == "AssetAlias":
        # deserialize to the expected type
        outlet_events = list(
            map(
                lambda event: {**event, "dest_asset_key": AssetUniqueKey(**event["dest_asset_key"])},
                outlet_events,
            )
        )

        for asset_alias_event in outlet_events:
            asset_alias_name = asset_alias_event["source_alias_name"]
            asset_unique_key = asset_alias_event["dest_asset_key"]
            frozen_extra = frozenset(asset_alias_event["extra"].items())
            asset_alias_names[(asset_unique_key, frozen_extra)].add(asset_alias_name)

    asset_unique_keys = {key for key, _ in asset_alias_names}
    existing_aliased_assets: set[AssetUniqueKey] = {
        AssetUniqueKey.from_asset(asset_obj)
        for asset_obj in session.scalars(
            select(AssetModel).where(
                tuple_(AssetModel.name, AssetModel.uri).in_(attrs.astuple(key) for key in asset_unique_keys)
            )
        )
    }

    inactive_asset_unique_keys = TI._get_inactive_asset_unique_keys(
        asset_unique_keys={key for key in asset_unique_keys if key in existing_aliased_assets},
        session=session,
    )

    if inactive_asset_unique_keys:
        raise AirflowInactiveAssetAddedToAssetAliasException(inactive_asset_unique_keys)

    if missing_assets := [
        asset_unique_key.to_asset()
        for asset_unique_key, _ in asset_alias_names
        if asset_unique_key not in existing_aliased_assets
    ]:
        asset_manager.create_assets(missing_assets, session=session)
        log.warning("Created new assets for alias reference: %s", missing_assets)
        session.flush()  # Needed because we need the id for fk.

    for (unique_key, extra_items), alias_names in asset_alias_names.items():
        log.info(
            'Creating event for %r through aliases "%s"',
            unique_key,
            ", ".join(alias_names),
        )
        asset_manager.register_asset_change(
            task_instance=task_instance,
            asset=unique_key,
            aliases=[AssetAlias(name=name) for name in alias_names],
            extra=dict(extra_items),
            session=session,
            source_alias_names=alias_names,
        )

    # Handle events derived from references.
    asset_stmt = select(AssetModel).where(AssetModel.name.in_(asset_name_refs), AssetModel.active.has())
    for asset_model in session.scalars(asset_stmt):
        log.info("Creating event through asset name reference %r", asset_model.name)
        asset_manager.register_asset_change(
            task_instance=task_instance,
            asset=asset_model,
            extra=outlet_events[asset_model].extra,
            session=session,
        )
    asset_stmt = select(AssetModel).where(AssetModel.uri.in_(asset_uri_refs), AssetModel.active.has())
    for asset_model in session.scalars(asset_stmt):
        log.info("Creating event for through asset URI reference %r", asset_model.uri)
        asset_manager.register_asset_change(
            task_instance=task_instance,
            asset=asset_model,
            extra=outlet_events[asset_model].extra,
            session=session,
        )
