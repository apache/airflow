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

import contextlib
import itertools
import json
from collections import defaultdict
from collections.abc import Iterator
from typing import TYPE_CHECKING, Annotated, Any, cast
from uuid import UUID

import attrs
import structlog
from cadwyn import VersionedAPIRouter
from fastapi import Body, HTTPException, Query, status
from pydantic import JsonValue
from sqlalchemy import func, or_, tuple_, update
from sqlalchemy.engine import CursorResult, Row
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import select
from structlog.contextvars import bind_contextvars

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    InactiveAssetsResponse,
    PrevSuccessfulDagRunResponse,
    TaskBreadcrumbsResponse,
    TaskStatesResponse,
    TIDeferredStatePayload,
    TIEnterRunningPayload,
    TIHeartbeatInfo,
    TIRescheduleStatePayload,
    TIRetryStatePayload,
    TIRunContext,
    TISkippedDownstreamTasksStatePayload,
    TIStateUpdate,
    TISuccessStatePayload,
    TITerminalStatePayload,
)
from airflow.api_fastapi.execution_api.deps import JWTBearerTIPathDep
from airflow.exceptions import TaskNotFound
from airflow.models.asset import AssetActive
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun as DR
from airflow.models.taskinstance import TaskInstance as TI, _stop_remaining_tasks
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.trigger import Trigger
from airflow.models.xcom import XComModel
from airflow.sdk.definitions._internal.expandinput import NotFullyPopulated
from airflow.serialization.definitions.assets import SerializedAsset, SerializedAssetUniqueKey
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.task.trigger_rule import TriggerRule
from airflow.utils.sqlalchemy import get_dialect_name
from airflow.utils.state import DagRunState, TaskInstanceState, TerminalTIState

if TYPE_CHECKING:
    from sqlalchemy.sql.dml import Update

    from airflow.models.expandinput import SchedulerExpandInput

router = VersionedAPIRouter()

ti_id_router = VersionedAPIRouter(
    dependencies=[
        # This checks that the UUID in the url matches the one in the token for us.
        JWTBearerTIPathDep
    ]
)


log = structlog.get_logger(__name__)


@ti_id_router.patch(
    "/{task_instance_id}/run",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {"description": "The TI is already in the requested state"},
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid payload for the state transition"},
    },
    response_model_exclude_unset=True,
)
def ti_run(
    task_instance_id: UUID,
    ti_run_payload: Annotated[TIEnterRunningPayload, Body()],
    session: SessionDep,
    dag_bag: DagBagDep,
) -> TIRunContext:
    """
    Run a TaskInstance.

    This endpoint is used to start a TaskInstance that is in the QUEUED state.
    """
    # We only use UUID above for validation purposes
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)
    log.debug(
        "Starting task instance run",
        hostname=ti_run_payload.hostname,
        unixname=ti_run_payload.unixname,
        pid=ti_run_payload.pid,
    )

    from sqlalchemy.sql import column
    from sqlalchemy.types import JSON

    old = (
        select(
            TI.state,
            TI.dag_id,
            TI.run_id,
            TI.task_id,
            TI.map_index,
            TI.try_number,
            TI.max_tries,
            TI.next_method,
            TI.hostname,
            TI.unixname,
            TI.pid,
            # This selects the raw JSON value, by-passing the deserialization -- we want that to happen on the
            # client
            column("next_kwargs", JSON),
        )
        .select_from(TI)
        .where(TI.id == ti_id_str)
        .with_for_update()
    )
    try:
        ti = session.execute(old).one()
        log.debug("Retrieved task instance details", state=ti.state, dag_id=ti.dag_id, task_id=ti.task_id)
    except NoResultFound:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    # We exclude_unset to avoid updating fields that are not set in the payload
    data = ti_run_payload.model_dump(exclude_unset=True)

    # don't update start date when resuming from deferral
    if ti.next_kwargs:
        data.pop("start_date")
        log.debug("Removed start_date from update as task is resuming from deferral")

    query = update(TI).where(TI.id == ti_id_str).values(data)

    previous_state = ti.state

    # If we are already running, but this is a duplicate request from the same client return the same OK
    # -- it's possible there was a network glitch and they never got the response
    if previous_state == TaskInstanceState.RUNNING and (ti.hostname, ti.unixname, ti.pid) == (
        ti_run_payload.hostname,
        ti_run_payload.unixname,
        ti_run_payload.pid,
    ):
        log.info("Duplicate start request received", hostname=ti_run_payload.hostname)
    elif previous_state not in (TaskInstanceState.QUEUED, TaskInstanceState.RESTARTING):
        log.warning(
            "Cannot start Task Instance in invalid state",
            previous_state=previous_state,
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
    else:
        log.info("Task started", previous_state=previous_state, hostname=ti_run_payload.hostname)
    # Ensure there is no end date set.
    query = query.values(
        end_date=None,
        hostname=ti_run_payload.hostname,
        unixname=ti_run_payload.unixname,
        pid=ti_run_payload.pid,
        state=TaskInstanceState.RUNNING,
        last_heartbeat_at=timezone.utcnow(),
    )

    try:
        result = session.execute(query)
        log.info("Task instance state updated", rows_affected=getattr(result, "rowcount", 0))

        dr = (
            session.scalars(
                select(DR)
                .filter_by(dag_id=ti.dag_id, run_id=ti.run_id)
                .options(joinedload(DR.consumed_asset_events))
            )
            .unique()
            .one_or_none()
        )

        if not dr:
            log.error("DagRun not found", dag_id=ti.dag_id, run_id=ti.run_id)
            raise ValueError(f"DagRun with dag_id={ti.dag_id} and run_id={ti.run_id} not found.")

        # Send the keys to the SDK so that the client requests to clear those XComs from the server.
        # The reason we cannot do this here in the server is because we need to issue a purge on custom XCom backends
        # too. With the current assumption, the workers ONLY have access to the custom XCom backends directly and they
        # can issue the purge.

        # However, do not clear it for deferral
        xcom_keys = []
        if not ti.next_method:
            map_index = None if ti.map_index < 0 else ti.map_index
            xcom_query = select(XComModel.key).where(
                XComModel.dag_id == ti.dag_id,
                XComModel.task_id == ti.task_id,
                XComModel.run_id == ti.run_id,
            )
            if map_index is not None:
                xcom_query = xcom_query.where(XComModel.map_index == map_index)

            xcom_keys = list(session.scalars(xcom_query))
        task_reschedule_count = (
            session.query(
                func.count(TaskReschedule.id)  # or any other primary key column
            )
            .filter(TaskReschedule.ti_id == ti_id_str)
            .scalar()
            or 0
        )

        if dag := dag_bag.get_dag_for_run(dag_run=dr, session=session):
            upstream_map_indexes = dict(
                _get_upstream_map_indexes(
                    serialized_dag=dag,
                    ti=ti,
                    session=session,
                )
            )
        else:
            upstream_map_indexes = None

        context = TIRunContext(
            dag_run=dr,
            task_reschedule_count=task_reschedule_count,
            max_tries=ti.max_tries,
            # TODO: Add variables and connections that are needed (and has perms) for the task
            variables=[],
            connections=[],
            xcom_keys_to_clear=xcom_keys,
            should_retry=_is_eligible_to_retry(previous_state, ti.try_number, ti.max_tries),
            upstream_map_indexes=upstream_map_indexes,
        )

        # Only set if they are non-null
        if ti.next_method:
            context.next_method = ti.next_method
            context.next_kwargs = ti.next_kwargs

        return context
    except SQLAlchemyError:
        log.exception("Error marking Task Instance state as running")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error occurred"
        )


def _get_upstream_map_indexes(
    *,
    serialized_dag: SerializedDAG,
    ti: TI | Row,
    session: SessionDep,
) -> Iterator[tuple[str, int | list[int] | None]]:
    task = serialized_dag.get_task(ti.task_id)
    for upstream_task in task.upstream_list:
        map_indexes: int | list[int] | None
        if (upstream_mapped_group := upstream_task.get_closest_mapped_task_group()) is None:
            # regular tasks or non-mapped task groups
            map_indexes = None
        elif task.get_closest_mapped_task_group() is upstream_mapped_group:
            # tasks in the same mapped task group hierarchy
            map_indexes = ti.map_index
        else:
            # tasks not in the same mapped task group
            # the upstream mapped task group should combine the return xcom as a list and return it
            mapped_ti_count: int | None = None

            try:
                # First try: without resolving XCom
                mapped_ti_count = upstream_mapped_group.get_parse_time_mapped_ti_count()
            except NotFullyPopulated:
                # Second try: resolve XCom for correct count
                try:
                    expand_input = cast("SchedulerExpandInput", upstream_mapped_group._expand_input)
                    mapped_ti_count = expand_input.get_total_map_length(ti.run_id, session=session)
                except NotFullyPopulated:
                    # For these trigger rules, unresolved map indexes are acceptable.
                    # The success of the upstream task is not the main reason for triggering the current task.
                    # Therefore, whether the upstream task is fully populated can be ignored.
                    if task.trigger_rule != TriggerRule.ALL_SUCCESS:
                        mapped_ti_count = None

            # Compute map indexes if we have a valid count
            map_indexes = list(range(mapped_ti_count)) if mapped_ti_count is not None else None

        yield upstream_task.task_id, map_indexes


@ti_id_router.patch(
    "/{task_instance_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {"description": "The TI is already in the requested state"},
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid payload for the state transition"},
    },
)
def ti_update_state(
    task_instance_id: UUID,
    ti_patch_payload: Annotated[TIStateUpdate, Body()],
    session: SessionDep,
    dag_bag: DagBagDep,
):
    """
    Update the state of a TaskInstance.

    Not all state transitions are valid, and transitioning to some states requires extra information to be
    passed along. (Check out the datamodels for details, the rendered docs might not reflect this accurately)
    """
    # We only use UUID above for validation purposes
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)
    log.debug("Updating task instance state", new_state=ti_patch_payload.state)

    old = select(TI.state, TI.try_number, TI.max_tries, TI.dag_id).where(TI.id == ti_id_str).with_for_update()
    try:
        (
            previous_state,
            try_number,
            max_tries,
            dag_id,
        ) = session.execute(old).one()
        log.debug(
            "Retrieved current task instance state",
            previous_state=previous_state,
            try_number=try_number,
            max_tries=max_tries,
        )
    except NoResultFound:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    if previous_state != TaskInstanceState.RUNNING:
        log.warning(
            "Cannot update Task Instance in invalid state",
            previous_state=previous_state,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "invalid_state",
                "message": "TI was not in the running state so it cannot be updated",
                "previous_state": previous_state,
            },
        )

    # We exclude_unset to avoid updating fields that are not set in the payload
    data = ti_patch_payload.model_dump(exclude={"task_outlets", "outlet_events"}, exclude_unset=True)
    query = update(TI).where(TI.id == ti_id_str).values(data)

    try:
        query, updated_state = _create_ti_state_update_query_and_update_state(
            ti_patch_payload=ti_patch_payload,
            ti_id_str=ti_id_str,
            session=session,
            query=query,
            dag_id=dag_id,
            dag_bag=dag_bag,
        )
    except Exception:
        # Set a task to failed in case any unexpected exception happened during task state update
        log.exception(
            "Error updating Task Instance state. Setting the task to failed.",
            payload=ti_patch_payload,
        )
        ti = session.get(TI, ti_id_str)
        if session.bind is not None:
            query = TI.duration_expression_update(timezone.utcnow(), query, session.bind)
        query = query.values(state=(updated_state := TaskInstanceState.FAILED))
        if ti is not None:
            _handle_fail_fast_for_dag(ti=ti, dag_id=dag_id, session=session, dag_bag=dag_bag)

    # TODO: Replace this with FastAPI's Custom Exception handling:
    # https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers
    try:
        result = session.execute(query)
        log.info(
            "Task instance state updated",
            new_state=updated_state,
            rows_affected=getattr(result, "rowcount", 0),
        )
    except SQLAlchemyError as e:
        log.error("Error updating Task Instance state", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error occurred"
        )


def _handle_fail_fast_for_dag(ti: TI, dag_id: str, session: SessionDep, dag_bag: DagBagDep) -> None:
    dr = ti.dag_run

    # Check fail_fast from DagModel (simple column lookup) - early exit if False
    # This avoids loading 5-50 MB SerializedDAG in 99% of cases
    fail_fast = session.scalar(select(DagModel.fail_fast).where(DagModel.dag_id == dag_id))
    if not fail_fast:
        return

    # Only load SerializedDAG when fail_fast=True (rare case ~1%)
    ser_dag = dag_bag.get_dag_for_run(dag_run=dr, session=session)
    if ser_dag:
        task_dict = getattr(ser_dag, "task_dict")
        task_teardown_map = {k: v.is_teardown for k, v in task_dict.items()}
        _stop_remaining_tasks(task_instance=ti, task_teardown_map=task_teardown_map, session=session)


def _create_ti_state_update_query_and_update_state(
    *,
    ti_patch_payload: TIStateUpdate,
    ti_id_str: str,
    query: Update,
    session: SessionDep,
    dag_bag: DagBagDep,
    dag_id: str,
) -> tuple[Update, TaskInstanceState]:
    if isinstance(ti_patch_payload, (TITerminalStatePayload, TIRetryStatePayload, TISuccessStatePayload)):
        ti = session.get(TI, ti_id_str)
        updated_state = TaskInstanceState(ti_patch_payload.state.value)
        if session.bind is not None:
            query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)
        query = query.values(state=updated_state, next_method=None, next_kwargs=None)

        if updated_state == TaskInstanceState.FAILED:
            # This is the only case needs extra handling for TITerminalStatePayload
            if ti is not None:
                _handle_fail_fast_for_dag(ti=ti, dag_id=dag_id, session=session, dag_bag=dag_bag)
        elif isinstance(ti_patch_payload, TIRetryStatePayload):
            if ti is not None:
                ti.prepare_db_for_next_try(session)
        elif isinstance(ti_patch_payload, TISuccessStatePayload):
            if ti is not None:
                TI.register_asset_changes_in_db(
                    ti,
                    ti_patch_payload.task_outlets,
                    ti_patch_payload.outlet_events,
                    session,
                )
    elif isinstance(ti_patch_payload, TIDeferredStatePayload):
        # Calculate timeout if it was passed
        timeout = None
        if ti_patch_payload.trigger_timeout is not None:
            timeout = timezone.utcnow() + ti_patch_payload.trigger_timeout

        trigger_kwargs = ti_patch_payload.trigger_kwargs
        if not isinstance(trigger_kwargs, str):
            # If it's passed as a string, assume the client encrypted it, otherwise assume it doesn't need to
            # be. Just JSON serialize it
            trigger_kwargs = json.dumps(trigger_kwargs)

        trigger_row = Trigger(
            classpath=ti_patch_payload.classpath,
            kwargs={},
        )
        trigger_row.encrypted_kwargs = trigger_kwargs
        session.add(trigger_row)
        session.flush()

        # TODO: HANDLE execution timeout later as it requires a call to the DB
        # either get it from the serialised DAG or get it from the API

        query = update(TI).where(TI.id == ti_id_str)

        # This is slightly inefficient as we deserialize it to then right again serialize it in the sqla
        # TypeAdapter.
        next_kwargs = None
        if ti_patch_payload.next_kwargs:
            from airflow.serialization.serialized_objects import BaseSerialization

            next_kwargs = BaseSerialization.deserialize(ti_patch_payload.next_kwargs)

        query = query.values(
            state=TaskInstanceState.DEFERRED,
            trigger_id=trigger_row.id,
            next_method=ti_patch_payload.next_method,
            next_kwargs=next_kwargs,
            trigger_timeout=timeout,
        )
        updated_state = TaskInstanceState.DEFERRED
    elif isinstance(ti_patch_payload, TIRescheduleStatePayload):
        # Quick check for poke_interval isn't immediately over MySQL's TIMESTAMP limit.
        # This check is only rudimentary to catch trivial user errors, e.g. mistakenly
        # set the value to milliseconds instead of seconds. There's another check when
        # we actually try to reschedule to ensure database coherence.
        if get_dialect_name(session) == "mysql":
            # As documented in https://dev.mysql.com/doc/refman/5.7/en/datetime.html.
            _MYSQL_TIMESTAMP_MAX = timezone.datetime(2038, 1, 19, 3, 14, 7)
            if ti_patch_payload.reschedule_date > _MYSQL_TIMESTAMP_MAX:
                # Set a task to failed in case any unexpected exception happened during task state update
                log.error(
                    "Cannot reschedule task past MySQL limit. Setting the task to failed.",
                    payload=ti_patch_payload,
                    mysql_timestamp_max=_MYSQL_TIMESTAMP_MAX,
                )
                data = ti_patch_payload.model_dump(exclude={"reschedule_date"}, exclude_unset=True)
                query = update(TI).where(TI.id == ti_id_str).values(data)
                if session.bind is not None:
                    query = TI.duration_expression_update(timezone.utcnow(), query, session.bind)
                query = query.values(state=TaskInstanceState.FAILED)
                ti = session.get(TI, ti_id_str)
                if ti is not None:
                    _handle_fail_fast_for_dag(ti=ti, dag_id=dag_id, session=session, dag_bag=dag_bag)
                return query, TaskInstanceState.FAILED

        task_instance = session.get(TI, ti_id_str)
        actual_start_date = timezone.utcnow()
        if task_instance is not None and task_instance.id is not None:
            session.add(
                TaskReschedule(
                    UUID(str(task_instance.id)),
                    actual_start_date,
                    ti_patch_payload.end_date,
                    ti_patch_payload.reschedule_date,
                )
            )

        query = update(TI).where(TI.id == ti_id_str)
        # calculate the duration for TI table too
        if session.bind is not None:
            query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)
        # clear the next_method and next_kwargs so that none of the retries pick them up
        updated_state = TaskInstanceState.UP_FOR_RESCHEDULE
        query = query.values(state=updated_state, next_method=None, next_kwargs=None)
    else:
        raise ValueError(f"Unexpected Payload Type {type(ti_patch_payload)}")

    return query, updated_state


@ti_id_router.patch(
    "/{task_instance_id}/skip-downstream",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid payload for the state transition"},
    },
)
def ti_skip_downstream(
    task_instance_id: UUID,
    ti_patch_payload: TISkippedDownstreamTasksStatePayload,
    session: SessionDep,
):
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)
    log.info("Skipping downstream tasks", task_count=len(ti_patch_payload.tasks))

    now = timezone.utcnow()
    tasks = ti_patch_payload.tasks

    query_result = session.execute(select(TI.dag_id, TI.run_id).where(TI.id == ti_id_str))
    row_result = query_result.fetchone()
    if row_result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": "Task Instance not found"},
        )
    dag_id, run_id = row_result
    log.debug("Retrieved DAG and run info", dag_id=dag_id, run_id=run_id)

    task_ids = [task if isinstance(task, tuple) else (task, -1) for task in tasks]
    log.debug("Prepared task IDs for skipping", task_ids=task_ids)

    query = (
        update(TI)
        .where(TI.dag_id == dag_id, TI.run_id == run_id, tuple_(TI.task_id, TI.map_index).in_(task_ids))
        .values(state=TaskInstanceState.SKIPPED, start_date=now, end_date=now)
        .execution_options(synchronize_session=False)
    )

    result = session.execute(query)
    log.info("Downstream tasks skipped", tasks_skipped=getattr(result, "rowcount", 0))


@ti_id_router.put(
    "/{task_instance_id}/heartbeat",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {
            "description": "The TI attempting to heartbeat should be terminated for the given reason"
        },
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid payload for the state transition"},
    },
)
def ti_heartbeat(
    task_instance_id: UUID,
    ti_payload: TIHeartbeatInfo,
    session: SessionDep,
):
    """Update the heartbeat of a TaskInstance to mark it as alive & still running."""
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)
    log.debug("Processing heartbeat", hostname=ti_payload.hostname, pid=ti_payload.pid)

    # Hot path: since heartbeating a task is a very common operation, we try to do minimize the number of queries
    # and DB round trips as much as possible.

    old = select(TI.state, TI.hostname, TI.pid).where(TI.id == ti_id_str).with_for_update()

    try:
        (previous_state, hostname, pid) = session.execute(old).one()
        log.debug(
            "Retrieved current task state", state=previous_state, current_hostname=hostname, current_pid=pid
        )
    except NoResultFound:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    if hostname != ti_payload.hostname or pid != ti_payload.pid:
        log.warning(
            "Task running elsewhere",
            current_hostname=hostname,
            current_pid=pid,
            requested_hostname=ti_payload.hostname,
            requested_pid=ti_payload.pid,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "reason": "running_elsewhere",
                "message": "TI is already running elsewhere",
                "current_hostname": hostname,
                "current_pid": pid,
            },
        )

    if previous_state != TaskInstanceState.RUNNING:
        log.warning("Task not in running state", current_state=previous_state)
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
    log.debug("Heartbeat updated", state=previous_state)


@ti_id_router.put(
    "/{task_instance_id}/rtif",
    status_code=status.HTTP_201_CREATED,
    # TODO: Add description to the operation
    # TODO: Add Operation ID to control the function name in the OpenAPI spec
    # TODO: Do we need to use create_openapi_http_exception_doc here?
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        HTTP_422_UNPROCESSABLE_CONTENT: {
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
    bind_contextvars(ti_id=ti_id_str)
    log.info("Updating RenderedTaskInstanceFields", field_count=len(put_rtif_payload))

    task_instance = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not task_instance:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
        )
    task_instance.update_rtif(put_rtif_payload, session)
    log.debug("RenderedTaskInstanceFields updated successfully")

    return {"message": "Rendered task instance fields successfully set"}


@ti_id_router.patch(
    "/{task_instance_id}/rendered-map-index",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid rendered_map_index value"},
    },
)
def ti_patch_rendered_map_index(
    task_instance_id: UUID,
    rendered_map_index: Annotated[str, Body()],
    session: SessionDep,
):
    """Update rendered_map_index for a task instance, sent by the worker during task execution."""
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)

    if not rendered_map_index:
        log.error("rendered_map_index cannot be empty")
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            detail="rendered_map_index cannot be empty",
        )

    log.debug("Updating rendered_map_index", length=len(rendered_map_index))

    query = update(TI).where(TI.id == ti_id_str).values(rendered_map_index=rendered_map_index)
    result = session.execute(query)

    result = cast("CursorResult[Any]", result)
    if result.rowcount == 0:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task Instance not found",
        )


@ti_id_router.get(
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
    bind_contextvars(ti_id=ti_id_str)
    log.debug("Retrieving previous successful DAG run")

    task_instance = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not task_instance or not task_instance.logical_date:
        log.debug("No task instance or logical date found")
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
        log.debug("No previous successful DAG run found")
        return PrevSuccessfulDagRunResponse()

    log.debug(
        "Found previous successful DAG run",
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        logical_date=dag_run.logical_date,
    )
    return PrevSuccessfulDagRunResponse.model_validate(dag_run)


@router.get("/count", status_code=status.HTTP_200_OK)
def get_task_instance_count(
    dag_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    map_index: Annotated[int | None, Query()] = None,
    task_ids: Annotated[list[str] | None, Query()] = None,
    task_group_id: Annotated[str | None, Query()] = None,
    logical_dates: Annotated[list[UtcDateTime] | None, Query()] = None,
    run_ids: Annotated[list[str] | None, Query()] = None,
    states: Annotated[list[str] | None, Query()] = None,
) -> int:
    """Get the count of task instances matching the given criteria."""
    query = select(func.count()).select_from(TI).where(TI.dag_id == dag_id)

    if task_ids:
        query = query.where(TI.task_id.in_(task_ids))

    if map_index is not None:
        query = query.where(TI.map_index == map_index)

    if logical_dates:
        query = query.where(TI.logical_date.in_(logical_dates))

    if run_ids:
        query = query.where(TI.run_id.in_(run_ids))

    if task_group_id:
        group_tasks = _get_group_tasks(dag_id, task_group_id, session, dag_bag, logical_dates, run_ids)

        # Get unique (task_id, map_index) pairs

        task_map_pairs = [(ti.task_id, ti.map_index) for ti in group_tasks]

        if map_index is not None:
            task_map_pairs = [(ti.task_id, ti.map_index) for ti in group_tasks if ti.map_index == map_index]

        if not task_map_pairs:
            # If no task group tasks found, default to checking the task group ID itself
            # This matches the behavior in _get_external_task_group_task_ids
            task_map_pairs = [(task_group_id, -1)]

        # Update query to use task_id, map_index pairs
        query = query.where(tuple_(TI.task_id, TI.map_index).in_(task_map_pairs))

    if states:
        if "null" in states:
            not_none_states = [s for s in states if s != "null"]
            if not_none_states:
                query = query.where(or_(TI.state.is_(None), TI.state.in_(not_none_states)))
            else:
                query = query.where(TI.state.is_(None))
        else:
            query = query.where(TI.state.in_(states))

    count = session.scalar(query)
    return count or 0


@router.get("/states", status_code=status.HTTP_200_OK)
def get_task_instance_states(
    dag_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    map_index: Annotated[int | None, Query()] = None,
    task_ids: Annotated[list[str] | None, Query()] = None,
    task_group_id: Annotated[str | None, Query()] = None,
    logical_dates: Annotated[list[UtcDateTime] | None, Query()] = None,
    run_ids: Annotated[list[str] | None, Query()] = None,
) -> TaskStatesResponse:
    """Get the states for Task Instances with the given criteria."""
    run_id_task_state_map: dict[str, dict[str, Any]] = defaultdict(dict)

    query = select(TI).where(TI.dag_id == dag_id)

    if task_ids:
        query = query.where(TI.task_id.in_(task_ids))

    if logical_dates:
        query = query.where(TI.logical_date.in_(logical_dates))

    if run_ids:
        query = query.where(TI.run_id.in_(run_ids))

    results = session.scalars(query).all()

    if task_group_id:
        group_tasks = _get_group_tasks(dag_id, task_group_id, session, dag_bag, logical_dates, run_ids)

        results = results + group_tasks if task_ids else group_tasks

    if map_index is not None:
        results = [task for task in results if task.map_index == map_index]
    [
        run_id_task_state_map[task.run_id].update(
            {task.task_id: task.state}
            if task.map_index < 0
            else {f"{task.task_id}_{task.map_index}": task.state}
        )
        for task in results
    ]

    return TaskStatesResponse(task_states=run_id_task_state_map)


@router.get("/breadcrumbs", status_code=status.HTTP_200_OK)
def get_task_instance_breadcrumbs(dag_id: str, run_id: str, session: SessionDep) -> TaskBreadcrumbsResponse:
    result = session.execute(
        select(TI.task_id, TI.map_index, TI.state, TI.operator, TI.duration)
        .where(TI.dag_id == dag_id, TI.run_id == run_id, TI.state.in_(TerminalTIState))
        .order_by(TI.task_id, TI.map_index)
    ).mappings()

    def _iter_breadcrumbs() -> Iterator[dict[str, Any]]:
        for row in result:
            yield {str(k): v for k, v in row.items()}

    return TaskBreadcrumbsResponse(breadcrumbs=_iter_breadcrumbs())


def _is_eligible_to_retry(state: str, try_number: int, max_tries: int) -> bool:
    """Is task instance is eligible for retry."""
    if state == TaskInstanceState.RESTARTING:
        # If a task is cleared when running, it goes into RESTARTING state and is always
        # eligible for retry
        return True

    # max_tries is initialised with the retries defined at task level, we do not need to explicitly ask for
    # retries from the task SDK now, we can handle using max_tries
    return max_tries != 0 and try_number <= max_tries


def _get_group_tasks(
    dag_id: str, task_group_id: str, session: SessionDep, dag_bag: DagBagDep, logical_dates=None, run_ids=None
):
    # Get all tasks in the task group
    dag = get_latest_version_of_dag(dag_bag, dag_id, session, include_reason=True)
    task_group = dag.task_group_dict.get(task_group_id)
    if not task_group:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": f"Task group {task_group_id} not found in DAG {dag_id}",
            },
        )

    # First get all task instances to get the task_id, map_index pairs
    group_tasks = session.scalars(
        select(TI).where(
            TI.dag_id == dag_id,
            TI.task_id.in_(task.task_id for task in task_group.iter_tasks()),
            *([TI.logical_date.in_(logical_dates)] if logical_dates else []),
            *([TI.run_id.in_(run_ids)] if run_ids else []),
        )
    ).all()

    return group_tasks


@ti_id_router.get(
    "/{task_instance_id}/validate-inlets-and-outlets",
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
    },
)
def validate_inlets_and_outlets(
    task_instance_id: UUID,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> InactiveAssetsResponse:
    """Validate whether there're inactive assets in inlets and outlets of a given task instance."""
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)

    ti = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not ti:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    if not ti.task:
        dr = ti.dag_run
        dag = dag_bag.get_dag_for_run(dag_run=dr, session=session)
        if dag:
            with contextlib.suppress(TaskNotFound):
                ti.task = dag.get_task(ti.task_id)

    inlets = (
        [asset.asprofile() for asset in ti.task.inlets if isinstance(asset, SerializedAsset)]
        if ti.task
        else []
    )
    outlets = (
        [asset.asprofile() for asset in ti.task.outlets if isinstance(asset, SerializedAsset)]
        if ti.task
        else []
    )
    if not (inlets or outlets):
        return InactiveAssetsResponse(inactive_assets=[])

    all_asset_unique_keys: set[SerializedAssetUniqueKey] = {
        SerializedAssetUniqueKey.from_asset(inlet_or_outlet)  # type: ignore
        for inlet_or_outlet in itertools.chain(inlets, outlets)
    }
    active_asset_unique_keys = {
        SerializedAssetUniqueKey(name, uri)
        for name, uri in session.execute(
            select(AssetActive.name, AssetActive.uri).where(
                tuple_(AssetActive.name, AssetActive.uri).in_(
                    attrs.astuple(key) for key in all_asset_unique_keys
                )
            )
        )
    }
    different = all_asset_unique_keys - active_asset_unique_keys

    return InactiveAssetsResponse(
        inactive_assets=[asset_unique_key.asprofile() for asset_unique_key in different],
    )


# This line should be at the end of the file to ensure all routes are registered
router.include_router(ti_id_router)
