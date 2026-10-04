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

# Cadwyn needs evaluated endpoint annotations to generate versioned request models.
# See https://github.com/zmievsa/cadwyn/pull/413
# ruff: noqa: I002

import contextlib
import itertools
import json
from collections import defaultdict
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import Annotated, Any, NoReturn, cast
from uuid import UUID

import attrs
import structlog
from cadwyn import VersionedAPIRouter
from fastapi import Body, HTTPException, Query, Response, Security, status
from opentelemetry import trace
from opentelemetry.trace import StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import JsonValue, ValidationError
from sqlalchemy import and_, exists, func, or_, tuple_, union, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import DataError, NoResultFound, SQLAlchemyError
from sqlalchemy.orm import Session, contains_eager, joinedload
from sqlalchemy.sql import select
from sqlalchemy.sql.dml import Update
from structlog.contextvars import bind_contextvars

from airflow._shared.observability.traces import override_ids
from airflow._shared.state import TaskScope
from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import AsyncSessionDep, SessionDep
from airflow.api_fastapi.common.db.dags import eager_load_teams
from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.services.public.dag_run import patch_dag_run_note
from airflow.api_fastapi.execution_api.datamodels.task_arg_binding import get_arg_bindings_adapter
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRunNoteUpdatePayload,
    InactiveAssetsResponse,
    LoopContext,
    PreviousTIResponse,
    PrevSuccessfulDagRunResponse,
    TaskBreadcrumbsResponse,
    TaskStatesResponse,
    TerminalStateNonSuccess,
    TIAwaitingInputStatePayload,
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
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.security import (
    CurrentTIToken,
    ExecutionAPIRoute,
    get_team_name_for_ti,
    issue_execution_token,
    require_auth,
    skip_auto_ti_attempt_live,
)
from airflow.api_fastapi.execution_api.services.task_instances import (
    client_supports_arg_bindings,
    get_arg_bindings,
)
from airflow.api_fastapi.execution_api.versions.v2026_10_30 import IdentifyArchivedTaskStateUpdates
from airflow.configuration import conf
from airflow.exceptions import InvalidPartitionKeyError, TaskNotFound
from airflow.models.asset import AssetActive
from airflow.models.base import ID_LEN
from airflow.models.dag import DagModel
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun as DR, InvalidLoopDecision
from airflow.models.dynamic_region import SENTINEL_REGION_ID, AmbiguousProducerError
from airflow.models.hitl import HITLDetail
from airflow.models.log import Log
from airflow.models.task_coordinates import (
    LOOP_GATE_OPERATOR,
    TaskCoordinateResolver,
    build_coordinate_filters,
    get_public_region,
    public_map_index_expression,
)
from airflow.models.taskinstance import TaskInstance as TI, _stop_remaining_tasks
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.trigger import Trigger, handle_event_submit
from airflow.models.xcom import build_xcom_read_query, xcom_entity
from airflow.serialization.definitions.assets import SerializedAsset, SerializedAssetUniqueKey
from airflow.state import get_state_backend
from airflow.triggers.base import TriggerEvent
from airflow.utils.sqlalchemy import get_dialect_name
from airflow.utils.state import DagRunState, IntermediateTIState, TaskInstanceState, TerminalTIState

router = VersionedAPIRouter()

ti_id_router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    dependencies=[
        Security(require_auth, scopes=["ti:self"]),
    ],
)


log = structlog.get_logger(__name__)
tracer = trace.get_tracer(__name__)


@ti_id_router.patch(
    "/{task_instance_id}/run",
    status_code=status.HTTP_200_OK,
    dependencies=[Security(require_auth, scopes=["token:execution", "token:workload"])],
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task Instance not found"),
            (status.HTTP_409_CONFLICT, "The TI is already in the requested state"),
            (HTTP_422_UNPROCESSABLE_CONTENT, "Invalid payload for the state transition"),
            (
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "The serialized TaskFlow arg spec for this stub task is not valid",
            ),
        ]
    ),
    response_model_exclude_unset=True,
)
def ti_run(
    task_instance_id: UUID,
    ti_run_payload: Annotated[TIEnterRunningPayload, Body()],
    response: Response,
    session: SessionDep,
    dag_bag: DagBagDep,
    services=DepContainer,
    token: TIToken = CurrentTIToken,
) -> TIRunContext:
    """
    Run a TaskInstance.

    This endpoint is used to start a TaskInstance that is in the QUEUED state.
    """
    bind_contextvars(ti_id=str(task_instance_id))
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
            TI.region_id,
            TI.region_index,
            TI.try_number,
            TI.max_tries,
            TI.start_date,
            TI.next_method,
            TI.hostname,
            TI.unixname,
            TI.pid,
            TI.dag_version_id,
            # This selects the raw JSON value, bypassing the deserialization -- we want that to happen on the
            # client
            column("next_kwargs", JSON),
            DR.logical_date,
            DagModel.owners,
        )
        .select_from(TI)
        .join(DR, and_(TI.dag_id == DR.dag_id, TI.run_id == DR.run_id))
        .join(DagModel, TI.dag_id == DagModel.dag_id)
        .where(TI.id == task_instance_id)
        .with_for_update(of=TI)
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

    query = update(TI).where(TI.id == task_instance_id).values(data)

    previous_state = ti.state

    if previous_state == TaskInstanceState.RESTARTING and (ti.hostname, ti.unixname, ti.pid) != (
        ti_run_payload.hostname,
        ti_run_payload.unixname,
        ti_run_payload.pid,
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"reason": "running_elsewhere", "previous_state": previous_state},
        )

    # If we are already running, but this is a duplicate request from the same client return the same OK
    # -- it's possible there was a network glitch and they never got the response
    if previous_state == TaskInstanceState.RUNNING and (ti.hostname, ti.unixname, ti.pid) == (
        ti_run_payload.hostname,
        ti_run_payload.unixname,
        ti_run_payload.pid,
    ):
        log.info("Duplicate start request received", hostname=ti_run_payload.hostname)
    elif previous_state != TaskInstanceState.QUEUED:
        log.warning(
            "Cannot start Task Instance in invalid state",
            previous_state=previous_state,
        )

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
        session.add(
            Log(
                event=TaskInstanceState.RUNNING.value,
                task_instance_id=task_instance_id,
                task_id=ti.task_id,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                map_index=ti.region_index,
                try_number=ti.try_number,
                logical_date=ti.logical_date,
                owner=ti.owners,
                extra=json.dumps({"host_name": ti_run_payload.hostname}) if ti_run_payload.hostname else None,
            )
        )
    # Ensure there is no end date set and clear retry policy overrides from the previous attempt.
    query = query.values(
        end_date=None,
        hostname=ti_run_payload.hostname,
        unixname=ti_run_payload.unixname,
        pid=ti_run_payload.pid,
        state=TaskInstanceState.RUNNING,
        last_heartbeat_at=timezone.utcnow(),
        retry_delay_override=None,
        retry_reason=None,
    )

    try:
        result = session.execute(query)
        log.info("Task instance state updated", rows_affected=getattr(result, "rowcount", 0))

        dr = (
            session.scalars(
                select(DR)
                .filter_by(dag_id=ti.dag_id, run_id=ti.run_id)
                .options(joinedload(DR.consumed_asset_events), *eager_load_teams(DR.dag_model))
            )
            .unique()
            .one_or_none()
        )

        if not dr:
            log.error("DagRun not found", dag_id=ti.dag_id, run_id=ti.run_id)
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "reason": "not_found",
                    "message": f"DagRun with dag_id={ti.dag_id} and run_id={ti.run_id} not found",
                },
            )

        # Send the keys to the SDK so that the client requests to clear those XComs from the server.
        # The reason we cannot do this here in the server is because we need to issue a purge on custom XCom backends
        # too. With the current assumption, the workers ONLY have access to the custom XCom backends directly and they
        # can issue the purge.

        # However, do not clear it for deferral
        xcom_keys = []
        if not ti.next_method:
            read = build_xcom_read_query(producer_ids=select(TI.id).where(TI.id == task_instance_id))
            entity = xcom_entity(read)
            xcom_keys = list(session.scalars(read.with_only_columns(entity.key)))
        task_reschedule_count = (
            session.scalar(
                select(func.count(TaskReschedule.id)).where(TaskReschedule.ti_id == task_instance_id)
            )
            or 0
        )

        context = TIRunContext(
            dag_run=dr,
            task_reschedule_count=task_reschedule_count,
            max_tries=ti.max_tries,
            # TODO: Add variables and connections that are needed (and has perms) for the task
            variables=[],
            connections=[],
            xcom_keys_to_clear=xcom_keys,
            should_retry=_is_eligible_to_retry(previous_state, ti.try_number, ti.max_tries),
            multi_team=conf.getboolean("core", "multi_team"),
        )
        resolver = TaskCoordinateResolver(dag_bag, session)
        if loop_context := resolver.loop_context(ti):
            group, index = loop_context
            terminal = resolver.get_task(
                ti.dag_id, ti.run_id, group.terminal_task_id, dag_version_id=ti.dag_version_id
            )
            context.loop = LoopContext(
                node_id=group.node_id,
                index=index,
                max_iterations=group.max_iterations,
                terminal_task_id=group.terminal_task_id,
                terminal_is_mapped=terminal.get_needs_expansion(),
            )

        # Only set for lang-SDK (foreign-runtime) tasks with a captured TaskFlow arg
        # spec; the route excludes unset fields, keeping regular responses lean.
        if client_supports_arg_bindings() and (
            arg_bindings := get_arg_bindings(dag_bag, ti, session=session)
        ):
            try:
                context.arg_bindings = get_arg_bindings_adapter().validate_python(arg_bindings)
            except ValidationError:
                log.exception(
                    "Serialized arg_bindings spec failed validation",
                    dag_id=ti.dag_id,
                    task_id=ti.task_id,
                    dag_version_id=ti.dag_version_id,
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={
                        "reason": "invalid_arg_bindings",
                        "message": "The serialized TaskFlow arg spec for this stub task is not valid.",
                    },
                )

        # Only set if they are non-null
        if ti.next_method:
            context.next_method = ti.next_method
            context.next_kwargs = ti.next_kwargs
            context.start_date = ti.start_date
    except DataError:
        # Let the app-level DataErrorHandler return a 422 (not the opaque 500 below).
        raise
    except SQLAlchemyError:
        # Defer to app-level SQLAlchemyError handler (returns HTTP 500).
        raise

    # JWTReissueMiddleware also writes Refreshed-API-Token but skips workload tokens, so we set it here for the workload→execution swap.
    if token.claims.scope == "workload":
        issue_execution_token(services, response, sub=str(task_instance_id))

    return context


@ti_id_router.patch(
    "/{task_instance_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_200_OK: {"description": "The TI was already in the requested state"},
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {"description": "The TI is not in a valid state for this transition"},
        status.HTTP_410_GONE: {"description": "The task attempt has been archived"},
        HTTP_422_UNPROCESSABLE_CONTENT: {"description": "Invalid payload for the state transition"},
    },
)
@skip_auto_ti_attempt_live
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
    bind_contextvars(ti_id=str(task_instance_id))
    log.debug("Updating task instance state", new_state=ti_patch_payload.state)

    if isinstance(ti_patch_payload, TITerminalStatePayload) and (
        ti_patch_payload.state == TerminalStateNonSuccess.SERVER_TERMINATED
    ):
        in_region = session.scalar(
            select(or_(TI.region_id != SENTINEL_REGION_ID, TI.region_index >= 0)).where(
                TI.id == task_instance_id
            )
        )
        if in_region:
            session.execute(
                select(DR.id)
                .where(
                    exists().where(
                        TI.id == task_instance_id,
                        TI.dag_id == DR.dag_id,
                        TI.run_id == DR.run_id,
                    )
                )
                .with_for_update()
            ).all()
        ti = session.scalar(
            select(TI)
            .where(TI.id == task_instance_id)
            .with_for_update(of=TI)
            .execution_options(populate_existing=True, include_all_attempts=True)
        )
        if ti is None:
            raise HTTPException(status_code=404, detail={"reason": "not_found"})
        if ti.working_set is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        if (ti.hostname, ti.pid) != (ti_patch_payload.hostname, ti_patch_payload.pid) or (
            ti_patch_payload.hostname is None or ti_patch_payload.pid is None
        ):
            raise HTTPException(status_code=409, detail={"reason": "running_elsewhere"})
        if ti.state == TaskInstanceState.RESTARTING:
            dag = dag_bag.get_dag_for_run(dag_run=ti.dag_run, session=session)
            ti.task = None
            if dag is not None:
                with contextlib.suppress(TaskNotFound):
                    ti.task = dag.get_task(ti.task_id)
            ti.end_date = ti_patch_payload.end_date
            ti.set_duration()
            ti.complete_restart(session=session)
        elif ti.state not in set(TerminalTIState):
            raise HTTPException(status_code=409, detail={"reason": "invalid_state"})
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    loop_group = None
    loop_gate = None
    if isinstance(ti_patch_payload, (TISuccessStatePayload, TITerminalStatePayload)):
        gate_run = session.execute(
            select(TI.dag_id, TI.run_id).where(
                TI.id == task_instance_id,
                TI.working_set.is_(True),
                TI.operator == LOOP_GATE_OPERATOR,
            )
        ).one_or_none()
        if gate_run is not None:
            session.execute(
                select(DR).where(DR.dag_id == gate_run.dag_id, DR.run_id == gate_run.run_id).with_for_update()
            ).scalar_one()
            loop_gate = session.scalar(
                select(TI)
                .where(TI.id == task_instance_id, TI.working_set.is_(True))
                .with_for_update(of=TI)
                .execution_options(populate_existing=True)
            )
            if loop_gate is not None:
                loop_context = TaskCoordinateResolver(dag_bag, session).loop_context(loop_gate)
                if loop_context is None:
                    loop_gate = None
                elif loop_context[0].gate_task_id != loop_gate.task_id:
                    raise HTTPException(status_code=409, detail={"reason": "invalid_loop_gate"})
                else:
                    loop_group = loop_context[0]

    old = (
        select(
            TI.state,
            TI.try_number,
            TI.max_tries,
            TI.dag_id,
            TI.task_id,
            TI.run_id,
            TI.region_index,
            TI.region_id,
            TI.hostname,
            DR.logical_date,
            DagModel.owners,
            TI.working_set,
        )
        .select_from(TI)
        .join(DR, and_(TI.dag_id == DR.dag_id, TI.run_id == DR.run_id))
        .join(DagModel, TI.dag_id == DagModel.dag_id)
        .where(TI.id == task_instance_id)
        .with_for_update(of=TI)
        .execution_options(include_all_attempts=True)
    )
    try:
        (
            previous_state,
            try_number,
            max_tries,
            dag_id,
            task_id,
            run_id,
            region_index,
            region_id,
            hostname,
            logical_date,
            owners,
            working_set,
        ) = session.execute(old).one()
        log.debug(
            "Retrieved current task instance state",
            previous_state=previous_state,
            try_number=try_number,
            max_tries=max_tries,
        )
    except NoResultFound:
        _raise_ti_not_in_live_table(task_instance_id, archived_in_history=False)
    if working_set is None:
        _raise_ti_not_in_live_table(
            task_instance_id, archived_in_history=IdentifyArchivedTaskStateUpdates.is_applied
        )

    # TIStateUpdate can include terminal and intermediate states. This idempotency check handles
    # duplicate updates when the requested state is already persisted (for example SUCCESS ->
    # SUCCESS or DEFERRED -> DEFERRED), including duplicates that would not pass the RUNNING
    # transition check below.
    if ti_patch_payload.state.value == previous_state:
        log.info(
            "Duplicate state update request received; state already set",
            requested_state=ti_patch_payload.state.value,
            previous_state=previous_state,
        )
        return Response(status_code=status.HTTP_200_OK)

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

    # Validate outlet event partition keys early, before entering the catch-all
    # except block that would otherwise swallow the HTTPException and mark the TI failed.
    if isinstance(ti_patch_payload, TISuccessStatePayload):
        try:
            _validate_outlet_event_partition_keys(ti_patch_payload.outlet_events)
        except InvalidPartitionKeyError as e:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_CONTENT,
                detail={"reason": "invalid_partition_key", "message": str(e)},
            ) from e

    gate_completed = False
    if (
        loop_gate is not None
        and loop_group is not None
        and isinstance(ti_patch_payload, TISuccessStatePayload)
    ):
        try:
            loop_gate.dag_run.complete_loop_gate(
                loop_gate, loop_group, TaskInstanceState.SUCCESS, session=session
            )
            gate_completed = True
        except InvalidLoopDecision as error:
            log.warning("Loop gate success rejected", error=str(error))
            ti_patch_payload = _build_rejected_gate_payload(
                ti_patch_payload,
                reason=f"Loop gate success rejected: {error}",
                retry=_is_eligible_to_retry(previous_state, try_number, max_tries),
            )

    # We exclude_unset to avoid updating fields that are not set in the payload
    data = ti_patch_payload.model_dump(
        exclude={"task_outlets", "outlet_events", "retry_delay_seconds", "retry_reason"},
        exclude_unset=True,
    )
    if "rendered_map_index" in data:
        data["_rendered_map_index"] = data.pop("rendered_map_index")
    query = update(TI).where(TI.id == task_instance_id).values(data)

    asset_callbacks: Sequence[Callable[[], None]] = ()
    try:
        query, updated_state, asset_callbacks = _create_ti_state_update_query_and_update_state(
            ti_patch_payload=ti_patch_payload,
            task_instance_id=task_instance_id,
            session=session,
            query=query,
            dag_id=dag_id,
            dag_bag=dag_bag,
        )
    except DataError:
        # Let DataErrorHandler return a 422 instead of silently marking the TI FAILED below.
        raise
    except Exception:
        if loop_gate is not None:
            raise
        # Set a task to failed in case any unexpected exception happened during task state update
        log.exception(
            "Error updating Task Instance state. Setting the task to failed.",
            payload=ti_patch_payload,
        )
        session.rollback()
        ti = session.scalar(select(TI).where(TI.id == task_instance_id).with_for_update(of=TI))
        if session.bind is not None:
            query = TI.duration_expression_update(timezone.utcnow(), query, session.bind)
        query = query.values(state=(updated_state := TaskInstanceState.FAILED))
        if ti is not None:
            _handle_fail_fast_for_dag(ti=ti, dag_id=dag_id, session=session, dag_bag=dag_bag)

    # TODO: Replace this with FastAPI's Custom Exception handling:
    # https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers
    try:
        result = session.execute(query)
        if (
            isinstance(ti_patch_payload, TIRetryStatePayload)
            and updated_state == TaskInstanceState.UP_FOR_RETRY
        ):
            ti = session.scalar(select(TI).where(TI.id == task_instance_id))
            if ti is not None:
                ti = ti.prepare_db_for_next_try(session)
        log.info(
            "Task instance state updated",
            new_state=updated_state,
            rows_affected=getattr(result, "rowcount", 0),
        )
        session.add(
            Log(
                event=updated_state.value,
                task_instance_id=task_instance_id,
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=region_index,
                try_number=try_number,
                logical_date=logical_date,
                owner=owners,
                extra=json.dumps({"host_name": hostname}) if hostname else None,
            )
        )
    except DataError:
        # Let DataErrorHandler return a 422 (not the opaque 500 below).
        raise
    except SQLAlchemyError:
        # Defer to app-level SQLAlchemyError handler (returns HTTP 500).
        raise

    if loop_gate is not None and loop_group is not None and not gate_completed:
        loop_gate.dag_run.complete_loop_gate(loop_gate, loop_group, updated_state, session=session)

    if updated_state == TaskInstanceState.SUCCESS:
        if conf.getboolean("state_store", "clear_on_success"):
            scope = TaskScope(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                region_index=region_index if region_index is not None else -1,
                region_id=region_id,
            )
            try:
                get_state_backend().clear(scope, session=session)
                log.info(
                    "Cleared task state on success",
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                    region_index=region_index,
                )
            except Exception:
                log.warning(
                    "Failed to clear task state on success",
                    dag_id=dag_id,
                    run_id=run_id,
                    task_id=task_id,
                )

    # Release the task_instance row lock before running listener callbacks.
    session.commit()

    for callback in asset_callbacks:
        callback()


def _build_rejected_gate_payload(
    payload: TISuccessStatePayload, *, reason: str, retry: bool
) -> TIRetryStatePayload | TITerminalStatePayload:
    carried = payload.model_dump(include={"end_date", "rendered_map_index"}, exclude_unset=True)
    if retry:
        return TIRetryStatePayload(state=IntermediateTIState.UP_FOR_RETRY, retry_reason=reason, **carried)
    return TITerminalStatePayload(state=TerminalStateNonSuccess.FAILED, retry_reason=reason, **carried)


def _emit_task_span(ti, state, *, resolver: TaskCoordinateResolver):
    # just to be safe
    if not ti.dag_run:
        return
    if not isinstance(ti.dag_run.context_carrier, dict):
        return
    if not isinstance(ti.context_carrier, dict):
        return
    dr_ctx = TraceContextTextMapPropagator().extract(ti.dag_run.context_carrier)

    # Skip if the run was head-sampled out, so every span in the run agrees with the
    # carrier's decision. A parent-based sampler would already drop this child span,
    # but the explicit check also covers non-parent-based samplers (which ignore the
    # parent and would re-sample it in) and short-circuits before building the span.
    # An invalid/empty carrier (legacy/NULL) recorded no decision, so it falls through
    # and still emits — preserving prior behavior.
    dr_span_context = trace.get_current_span(context=dr_ctx).get_span_context()
    if dr_span_context.is_valid and not dr_span_context.trace_flags.sampled:
        return

    ti_ctx = TraceContextTextMapPropagator().extract(ti.context_carrier)
    ti_span = trace.get_current_span(context=ti_ctx)
    span_context = ti_span.get_span_context()
    start_time_candidates = (x for x in (ti.queued_dttm, ti.start_date, timezone.utcnow()) if x)
    map_index = resolver.public_map_index(ti)
    name = f"task_run.{ti.task_id}"
    if map_index >= 0:
        name += f"[{map_index}]"
    with override_ids(span_context.trace_id, span_context.span_id):
        span = tracer.start_span(
            name=name,
            start_time=int(min(start_time_candidates).timestamp() * 1e9),
            context=dr_ctx,
        )

        attributes: dict[str, str | int] = {
            "airflow.dag_id": ti.dag_id,
            "airflow.task_id": ti.task_id,
            "airflow.dag_run.run_id": ti.run_id,
            "airflow.task_instance.try_number": ti.try_number,
            "airflow.task_instance.map_index": map_index,
            "airflow.task_instance.state": state,
            "airflow.task_instance.id": str(ti.id),
        }
        region_id, region_index = get_public_region(ti.region_id, ti.region_index)
        if region_id is not None and region_index is not None:
            attributes["airflow.task_instance.region_id"] = str(region_id)
            attributes["airflow.task_instance.region_index"] = region_index
        span.set_attributes(attributes)
        status_code = StatusCode.OK if state == TaskInstanceState.SUCCESS else StatusCode.ERROR
        span.set_status(status_code)
        span.end()


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


def _validate_outlet_event_partition_keys(outlet_events: list[dict[str, Any]]) -> None:
    """
    Validate partition_key values embedded in outlet events.

    Raises ``InvalidPartitionKeyError`` (which the caller translates to HTTP 422)
    if any per-emission partition key is empty/whitespace-only or exceeds the
    ``ID_LEN`` column width used in the metadata database.
    """
    for event in outlet_events:
        if (pk := event.get("partition_key")) is None:
            continue
        if not pk.strip():
            raise InvalidPartitionKeyError(
                f"partition_key in outlet event must not be empty or whitespace-only; got {pk!r}."
            )
        if len(pk) > ID_LEN:
            raise InvalidPartitionKeyError(
                f"partition_key in outlet event must be at most {ID_LEN} characters; got {len(pk)}."
            )


def _create_ti_state_update_query_and_update_state(
    *,
    ti_patch_payload: TIStateUpdate,
    task_instance_id: UUID,
    query: Update,
    session: SessionDep,
    dag_bag: DagBagDep,
    dag_id: str,
) -> tuple[Update, TaskInstanceState, Sequence[Callable[[], None]]]:
    asset_callbacks: Sequence[Callable[[], None]] = ()
    if isinstance(ti_patch_payload, (TITerminalStatePayload, TIRetryStatePayload, TISuccessStatePayload)):
        ti = session.scalar(select(TI).where(TI.id == task_instance_id).with_for_update(of=TI))
        updated_state = TaskInstanceState(ti_patch_payload.state.value)
        if session.bind is not None:
            query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)
        query = query.values(state=updated_state, next_method=None, next_kwargs=None)

        if updated_state == TaskInstanceState.FAILED:
            # This is the only case needs extra handling for TITerminalStatePayload
            if isinstance(ti_patch_payload, TITerminalStatePayload) and ti_patch_payload.retry_reason:
                query = query.values(retry_reason=ti_patch_payload.retry_reason[:500])
            if ti is not None:
                _handle_fail_fast_for_dag(ti=ti, dag_id=dag_id, session=session, dag_bag=dag_bag)
        elif isinstance(ti_patch_payload, TIRetryStatePayload):
            retry_delay_override = ti_patch_payload.retry_delay_seconds
            retry_reason = ti_patch_payload.retry_reason[:500] if ti_patch_payload.retry_reason else None
            if ti is not None:
                ti.retry_delay_override = retry_delay_override
                ti.retry_reason = retry_reason
                ti.end_date = ti_patch_payload.end_date
                ti.set_duration()
                if "rendered_map_index" in ti_patch_payload.model_fields_set:
                    ti._rendered_map_index = ti_patch_payload.rendered_map_index
            # Store retry policy overrides so next_retry_datetime() can read them.
            # These are cleared when the task enters RUNNING (ti_run).
            query = query.values(retry_delay_override=retry_delay_override, retry_reason=retry_reason)
        elif isinstance(ti_patch_payload, TISuccessStatePayload):
            if ti is not None:
                asset_callbacks = TI.register_asset_changes_in_db(
                    ti,
                    ti_patch_payload.task_outlets,
                    ti_patch_payload.outlet_events,
                    session=session,
                )
        try:
            _emit_task_span(ti, state=updated_state, resolver=TaskCoordinateResolver(dag_bag, session))
        except Exception:
            log.warning("Failed to emit task span", exc_info=True)
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
            queue=ti_patch_payload.queue,
            team_name=get_team_name_for_ti(task_instance_id, session),
        )
        trigger_row.encrypted_kwargs = trigger_kwargs
        session.add(trigger_row)
        session.flush()

        # TODO: HANDLE execution timeout later as it requires a call to the DB
        # either get it from the serialised DAG or get it from the API

        query = update(TI).where(TI.id == task_instance_id)

        # Store next_kwargs directly (already serialized by worker)
        query = query.values(
            state=TaskInstanceState.DEFERRED,
            trigger_id=trigger_row.id,
            next_method=ti_patch_payload.next_method,
            next_kwargs=ti_patch_payload.next_kwargs,
            trigger_timeout=timeout,
        )
        updated_state = TaskInstanceState.DEFERRED
    elif isinstance(ti_patch_payload, TIAwaitingInputStatePayload):
        # Park the task waiting for human input (Human-in-the-loop). No trigger / triggerer is
        # created: the task is resumed by the Core API response handler or the scheduler timeout
        # sweep. The optional response deadline is stored on the existing trigger_timeout column.
        #
        # Fixed lock order (TaskInstance -> HITLDetail), matching the Core API response path, so a
        # human response racing this park transition cannot deadlock.
        ti = session.scalar(select(TI).where(TI.id == task_instance_id).with_for_update(of=TI))
        # Lock only the hitl_detail row (of=...): HITLDetail eager-joins task_instance (lazy="joined"),
        # and Postgres rejects FOR UPDATE against the nullable side of that outer join.
        hitl_detail = session.scalar(
            select(HITLDetail).where(HITLDetail.ti_id == task_instance_id).with_for_update(of=HITLDetail)
        )
        if ti is not None and hitl_detail is not None and hitl_detail.response_received:
            # The human responded in the window between the operator writing the HITL request and
            # the worker parking the task. Resume straight to execute_complete instead of parking,
            # which would otherwise strand an already-responded task (no trigger/sweep would fire).
            # Carry next_method/next_kwargs onto the TI first so the resume dispatches correctly;
            # handle_event_submit then injects the response event into next_kwargs.
            ti.next_method = ti_patch_payload.next_method
            ti.next_kwargs = ti_patch_payload.next_kwargs
            handle_event_submit(
                TriggerEvent(hitl_detail.as_resume_event_payload()),
                task_instance=ti,
                session=session,
            )
            query = update(TI).where(TI.id == task_instance_id).values(state=TaskInstanceState.SCHEDULED)
            updated_state = TaskInstanceState.SCHEDULED
        else:
            timeout = None
            if ti_patch_payload.timeout is not None:
                timeout = timezone.utcnow() + ti_patch_payload.timeout

            query = update(TI).where(TI.id == task_instance_id)
            query = query.values(
                state=TaskInstanceState.AWAITING_INPUT,
                trigger_id=None,
                next_method=ti_patch_payload.next_method,
                next_kwargs=ti_patch_payload.next_kwargs,
                trigger_timeout=timeout,
            )
            updated_state = TaskInstanceState.AWAITING_INPUT
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
                query = update(TI).where(TI.id == task_instance_id).values(data)
                if session.bind is not None:
                    query = TI.duration_expression_update(timezone.utcnow(), query, session.bind)
                query = query.values(state=TaskInstanceState.FAILED)
                ti = session.scalar(select(TI).where(TI.id == task_instance_id).with_for_update(of=TI))
                if ti is not None:
                    _handle_fail_fast_for_dag(ti=ti, dag_id=dag_id, session=session, dag_bag=dag_bag)
                return query, TaskInstanceState.FAILED, ()

        actual_start_date = timezone.utcnow()
        session.add(
            TaskReschedule(
                task_instance_id,
                actual_start_date,
                ti_patch_payload.end_date,
                ti_patch_payload.reschedule_date,
            )
        )

        query = update(TI).where(TI.id == task_instance_id)
        # calculate the duration for TI table too
        if session.bind is not None:
            query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)
        # clear the next_method and next_kwargs so that none of the retries pick them up
        updated_state = TaskInstanceState.UP_FOR_RESCHEDULE
        query = query.values(state=updated_state, next_method=None, next_kwargs=None)
    else:
        raise ValueError(f"Unexpected Payload Type {type(ti_patch_payload)}")

    return query, updated_state, asset_callbacks


@ti_id_router.patch(
    "/{task_instance_id}/skip-downstream",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_400_BAD_REQUEST, "Invalid task coordinates"),
            (status.HTTP_404_NOT_FOUND, "Task Instance not found"),
            (status.HTTP_409_CONFLICT, "Multiple live task instances match a task coordinate"),
            (HTTP_422_UNPROCESSABLE_CONTENT, "Invalid payload for the state transition"),
        ]
    ),
)
def ti_skip_downstream(
    task_instance_id: UUID,
    ti_patch_payload: TISkippedDownstreamTasksStatePayload,
    session: SessionDep,
    dag_bag: DagBagDep,
):
    bind_contextvars(ti_id=str(task_instance_id))
    log.info("Skipping downstream tasks", task_count=len(ti_patch_payload.tasks))

    now = timezone.utcnow()
    tasks = ti_patch_payload.tasks

    caller = session.get(TI, task_instance_id)
    if caller is None or caller.working_set is not True:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": "Task Instance not found"},
        )
    dag_id, run_id = caller.dag_id, caller.run_id
    log.debug("Retrieved DAG and run info", dag_id=dag_id, run_id=run_id)

    log.debug("Prepared task IDs for skipping", tasks=tasks)
    resolver = TaskCoordinateResolver(dag_bag, session)
    resolver.prefetch_regional_tasks(
        dag_id, run_id, {task if isinstance(task, str) else task[0] for task in tasks}
    )
    try:
        # A bare task_id skips every TI of that task, so an already expanded mapped task
        # (e.g. one mapped over a literal list) is skipped too, not only map_index -1.
        targets = [(task, None) if isinstance(task, str) else (task[0], task[1]) for task in tasks]
        plain = [
            (task_id, index)
            for task_id, index in targets
            if not resolver.has_regions(dag_id, run_id, task_id)
        ]
        selects = [
            resolver.select_skip_target_ids(caller=caller, task_id=task_id, map_indexes=index)
            for task_id, index in targets
            if (task_id, index) not in plain
        ]
        if plain:
            selects.append(
                resolver.select_legacy_task_ids(
                    dag_id=dag_id,
                    run_id=run_id,
                    task_ids=[task_id for task_id, index in plain if index is None],
                    slots=[(task_id, index) for task_id, index in plain if index is not None],
                )
            )
    except AmbiguousProducerError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
    except ValueError as error:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(error)) from error
    if not selects:
        return
    selected_ids = set(session.scalars(union(*selects)))

    # Don't overwrite tasks that are already executing or finished.
    # See: https://github.com/apache/airflow/issues/59378
    # Note: SQL NULL NOT IN (...) is falsy, so we need an explicit IS NULL check.
    skippable_state_clause = or_(
        TI.state.is_(None),
        TI.state.not_in(
            [
                TaskInstanceState.RUNNING,
                TaskInstanceState.SUCCESS,
                TaskInstanceState.FAILED,
            ]
        ),
    )
    query = (
        update(TI)
        .where(
            TI.working_set.is_(True),
            TI.id.in_(selected_ids),
            skippable_state_clause,
        )
        .values(state=TaskInstanceState.SKIPPED, start_date=now, end_date=now)
        .execution_options(synchronize_session=False)
    )

    result = session.execute(query)
    log.info("Downstream tasks skipped", tasks_skipped=getattr(result, "rowcount", 0))


def _raise_ti_not_in_live_table(task_instance_id: UUID, *, archived_in_history: bool) -> NoReturn:
    """Raise 410 Gone if the missing TI id was archived to history, else 404 Not Found."""
    if archived_in_history:
        log.error("TaskInstance not in live table but archived in history", ti_id=str(task_instance_id))
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found, it may have been moved to the Task Instance History table",
            },
        )
    log.error("Task Instance not found", ti_id=str(task_instance_id))
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={
            "reason": "not_found",
            "message": "Task Instance not found",
        },
    )


@ti_id_router.patch(
    "/{task_instance_id}/dag-run-note",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task Instance not found"),
            (HTTP_422_UNPROCESSABLE_CONTENT, "Invalid payload for the DagRun note update"),
        ]
    ),
)
def update_dag_run_note(
    task_instance_id: UUID,
    body: DagRunNoteUpdatePayload,
    session: SessionDep,
) -> None:
    """
    Update the note for the DagRun associated with this task instance.

    An empty note removes the existing note, matching the public API. A null note is a
    no-op so runtime callers can leave a user-authored note untouched.
    """
    bind_contextvars(ti_id=str(task_instance_id))

    dag_run = session.scalar(
        select(DR)
        .join(TI, and_(TI.dag_id == DR.dag_id, TI.run_id == DR.run_id))
        .options(joinedload(DR.dag_run_note))
        .where(TI.id == task_instance_id)
    )
    if dag_run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": "Task Instance not found"},
        )

    if body.note is None:
        return

    # Runtime notes have no acting user, so they are stored unattributed. Carrying over the
    # previous author would credit them with content they did not write, so log the drop
    # instead of keeping it.
    if dag_run.dag_run_note is not None and dag_run.dag_run_note.user_id is not None:
        log.info(
            "Replacing an attributed DagRun note from task runtime; the note becomes unattributed",
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            previous_user_id=dag_run.dag_run_note.user_id,
        )

    # Reuse the public API note logic so both editing paths stay consistent.
    patch_dag_run_note(dag_run=dag_run, note=body.note, user_id=None)


@ti_id_router.put(
    "/{task_instance_id}/heartbeat",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task Instance not found"),
            (
                status.HTTP_409_CONFLICT,
                "The TI attempting to heartbeat should be terminated for the given reason",
            ),
            (
                status.HTTP_410_GONE,
                "Task Instance not found in the TI table but exists in the Task Instance History table",
            ),
            (HTTP_422_UNPROCESSABLE_CONTENT, "Invalid payload for the state transition"),
        ]
    ),
)
@skip_auto_ti_attempt_live
async def ti_heartbeat(
    task_instance_id: UUID,
    ti_payload: TIHeartbeatInfo,
    session: AsyncSessionDep,
):
    """Update the heartbeat of a TaskInstance to mark it as alive & still running."""
    bind_contextvars(ti_id=str(task_instance_id))
    log.debug("Processing heartbeat", hostname=ti_payload.hostname, pid=ti_payload.pid)

    # Hot path: in the common case the TI is still running on the same host and pid,
    # so we can update last_heartbeat_at directly without first taking a row lock.
    fast_path_result = cast(
        "CursorResult[Any]",
        await session.execute(
            update(TI)
            .where(
                TI.id == task_instance_id,
                TI.state == TaskInstanceState.RUNNING,
                TI.hostname == ti_payload.hostname,
                TI.pid == ti_payload.pid,
            )
            .values(last_heartbeat_at=timezone.utcnow())
            .execution_options(synchronize_session=False)
        ),
    )
    if fast_path_result.rowcount is not None and fast_path_result.rowcount > 0:
        log.debug("Heartbeat updated via fast path")
        return

    log.debug("Heartbeat fast path missed; falling back to diagnostic checks")

    old = (
        select(TI.state, TI.hostname, TI.pid, TI.working_set)
        .where(TI.id == task_instance_id)
        .with_for_update()
        .execution_options(include_all_attempts=True)
    )

    try:
        (previous_state, hostname, pid, working_set) = (await session.execute(old)).one()
        log.debug(
            "Retrieved current task state", state=previous_state, current_hostname=hostname, current_pid=pid
        )
    except NoResultFound:
        _raise_ti_not_in_live_table(task_instance_id, archived_in_history=False)
    if working_set is None:
        # An archived attempt was likely cleared while running, so return 410 Gone
        # instead of 404 Not Found to give the client a more specific signal.
        _raise_ti_not_in_live_table(task_instance_id, archived_in_history=True)

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
    await session.execute(
        update(TI).where(TI.id == task_instance_id).values(last_heartbeat_at=timezone.utcnow())
    )
    log.debug("Heartbeat updated", state=previous_state)


@ti_id_router.put(
    "/{task_instance_id}/rtif",
    status_code=status.HTTP_201_CREATED,
    operation_id="put_rtif",
    summary="Set Rendered Task Instance Fields",
    description="Store the rendered task instance fields (RTIF) for a task instance. "
    "These are the template fields after Jinja rendering has been applied. "
    "Called by the worker after task execution begins.",
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task Instance not found"),
            (
                status.HTTP_410_GONE,
                "Task Instance not found in the TI table but exists in the Task Instance History table",
            ),
            (
                HTTP_422_UNPROCESSABLE_CONTENT,
                "Invalid payload for the setting rendered task instance fields",
            ),
        ]
    ),
)
def ti_put_rtif(
    task_instance_id: UUID,
    put_rtif_payload: Annotated[dict[str, JsonValue], Body()],
    session: SessionDep,
):
    """Add an RTIF entry for a task instance, sent by the worker."""
    bind_contextvars(ti_id=str(task_instance_id))
    log.info("Updating RenderedTaskInstanceFields", field_count=len(put_rtif_payload))

    task_instance = session.scalar(
        select(TI).where(TI.id == task_instance_id).execution_options(include_all_attempts=True)
    )
    if task_instance is None or task_instance.working_set is None:
        # On retry/clear, the server regenerates the TI id. Return 410 for the stale id.
        _raise_ti_not_in_live_table(task_instance_id, archived_in_history=task_instance is not None)
    task_instance.update_rtif(put_rtif_payload, session=session)
    log.debug("RenderedTaskInstanceFields updated successfully")

    return {"message": "Rendered task instance fields successfully set"}


@ti_id_router.patch(
    "/{task_instance_id}/rendered-map-index",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task Instance not found"),
            (
                HTTP_422_UNPROCESSABLE_CONTENT,
                "Invalid rendered_map_index value",
            ),
        ]
    ),
)
def ti_patch_rendered_map_index(
    task_instance_id: UUID,
    rendered_map_index: Annotated[str, Body()],
    session: SessionDep,
):
    """Update rendered_map_index for a task instance, sent by the worker during task execution."""
    bind_contextvars(ti_id=str(task_instance_id))

    if not rendered_map_index:
        log.error("rendered_map_index cannot be empty")
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_CONTENT,
            detail="rendered_map_index cannot be empty",
        )

    log.debug("Updating rendered_map_index", length=len(rendered_map_index))

    query = update(TI).where(TI.id == task_instance_id).values(_rendered_map_index=rendered_map_index)
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
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task Instance or Dag Run not found"),
        ]
    ),
)
async def get_previous_successful_dagrun(
    task_instance_id: UUID, session: AsyncSessionDep
) -> PrevSuccessfulDagRunResponse:
    """
    Get the previous successful DagRun for a TaskInstance.

    The data from this endpoint is used to get values for Task Context.
    """
    bind_contextvars(ti_id=str(task_instance_id))
    log.debug("Retrieving previous successful DAG run")

    task_instance = await session.scalar(select(TI).where(TI.id == task_instance_id))
    if not task_instance or not task_instance.logical_date:
        log.debug("No task instance or logical date found")
        return PrevSuccessfulDagRunResponse()

    dag_run = await session.scalar(
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


def _find_superseded_ids(
    session: Session, dag_id: str, tasks: Collection[tuple[str, str]], dag_bag: DBDagBag
) -> set[UUID]:
    """
    Find the live rows of ``tasks`` (``(run_id, task_id)`` pairs) that a later loop pass supersedes.

    A task inside a loop keeps every pass live, so callers that name a task without a pass get its latest
    one, whatever map index, state or run they filter on: the latest pass is chosen among every live row of
    the task in the run. Two live rows of that pass in one slot leave it ambiguous.
    """
    if not tasks:
        return set()
    live = session.execute(
        select(
            TI.id,
            TI.dag_id,
            TI.run_id,
            TI.task_id,
            TI.region_id,
            TI.region_index,
            TI.dag_version_id,
            public_map_index_expression(TI).label("slot"),
        ).where(
            TI.working_set.is_(True),
            TI.dag_id == dag_id,
            tuple_(TI.run_id, TI.task_id).in_(list(tasks)),
        )
    ).all()
    try:
        passes = TaskCoordinateResolver(dag_bag, session).get_loop_passes(live)
    except ValueError as error:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Multiple live task instances share a task slot"
        ) from error
    rows_by_task: dict[tuple[str, str], list[tuple[Any, int | None]]] = defaultdict(list)
    for row, loop_pass in zip(live, passes):
        rows_by_task[row.run_id, row.task_id].append((row, loop_pass))
    superseded: set[UUID] = set()
    for rows in rows_by_task.values():
        latest = max((loop_pass for _, loop_pass in rows), key=lambda value: -1 if value is None else value)
        latest_slots: set[int] = set()
        for row, loop_pass in rows:
            if loop_pass != latest:
                superseded.add(row.id)
            elif row.slot in latest_slots:
                raise HTTPException(
                    status.HTTP_409_CONFLICT, "Multiple live task instances share a task slot"
                )
            else:
                latest_slots.add(row.slot)
    return superseded


@router.get(
    "/count",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task group not found"),
            (status.HTTP_409_CONFLICT, "Multiple live task instances share a task slot"),
        ]
    ),
)
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
    """Get the count of task instances matching the given criteria, counting a looped task's latest pass."""
    conditions = [TI.dag_id == dag_id, *build_coordinate_filters(TI, map_index=map_index)]

    if task_ids:
        conditions.append(TI.task_id.in_(task_ids))

    if logical_dates:
        conditions.append(TI.logical_date.in_(logical_dates))

    if run_ids:
        conditions.append(TI.run_id.in_(run_ids))

    if task_group_id:
        group_tasks = _get_group_tasks(
            dag_id, task_group_id, session, dag_bag, logical_dates, run_ids, map_index
        )

        if not group_tasks:
            # If no task group tasks found, default to checking the task group ID itself
            # This matches the behavior in _get_external_task_group_task_ids
            conditions.extend([TI.task_id == task_group_id, public_map_index_expression(TI) == -1])
        else:
            conditions.append(TI.id.in_(ti.id for ti in group_tasks))

    regional_tasks = session.execute(
        select(TI.run_id, TI.task_id).where(*conditions, TI.region_id != SENTINEL_REGION_ID).distinct()
    ).tuples()
    if superseded := _find_superseded_ids(session, dag_id, set(regional_tasks), dag_bag):
        conditions.append(TI.id.not_in(superseded))

    if states:
        if "null" in states:
            not_none_states = [s for s in states if s != "null"]
            if not_none_states:
                conditions.append(or_(TI.state.is_(None), TI.state.in_(not_none_states)))
            else:
                conditions.append(TI.state.is_(None))
        else:
            conditions.append(TI.state.in_(states))

    return session.scalar(select(func.count()).select_from(TI).where(*conditions)) or 0


@router.get(
    "/previous/{dag_id}/{task_id}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [(status.HTTP_409_CONFLICT, "Multiple live task instances share a task slot")]
    ),
)
async def get_previous_task_instance(
    dag_id: str,
    task_id: str,
    session: AsyncSessionDep,
    dag_bag: DagBagDep,
    logical_date: Annotated[UtcDateTime | None, Query()] = None,
    map_index: Annotated[int, Query()] = -1,
    state: Annotated[TaskInstanceState | None, Query()] = None,
) -> PreviousTIResponse | None:
    """
    Get the previous task instance matching the given criteria, preferring a looped task's latest pass.

    A run whose latest loop pass of the task holds no row matching the criteria is skipped, as the count
    and states endpoints would not report such a row either.

    :param dag_id: DAG ID (from path)
    :param task_id: Task ID (from path)
    :param logical_date: If provided, finds TI with logical_date < this value (before filter)
    :param map_index: Map index to filter by (defaults to -1 for non-mapped tasks)
    :param state: If provided, filters by TaskInstance state
    """
    query = (
        select(TI, public_map_index_expression(TI))
        .where(TI.working_set.is_(True))
        .join(DR, (TI.dag_id == DR.dag_id) & (TI.run_id == DR.run_id))
        .options(contains_eager(TI.dag_run).load_only(DR.logical_date))
        .where(TI.dag_id == dag_id, TI.task_id == task_id, *build_coordinate_filters(TI, map_index=map_index))
    )

    if state:
        query = query.where(TI.state == state)

    before = logical_date
    while True:
        candidates = query if before is None else query.where(DR.logical_date < before)
        row = (
            await session.execute(
                candidates.order_by(DR.logical_date.desc(), TI.region_index.desc()).limit(1)
            )
        ).first()
        if row is None:
            return None
        ti, public_index = row
        if ti.region_id == SENTINEL_REGION_ID:
            break

        superseded = await session.run_sync(_find_superseded_ids, dag_id, {(ti.run_id, task_id)}, dag_bag)
        if ti.id not in superseded:
            break
        run_rows = (
            await session.execute(query.where(DR.run_id == ti.run_id).order_by(TI.region_index.desc()))
        ).all()
        current = next(((other, index) for other, index in run_rows if other.id not in superseded), None)
        if current is not None:
            ti, public_index = current
            break
        before = ti.dag_run.logical_date
        if before is None:
            return None

    region_id, region_index = get_public_region(ti.region_id, ti.region_index)

    return PreviousTIResponse(
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        logical_date=ti.dag_run.logical_date,
        start_date=ti.start_date,
        end_date=ti.end_date,
        state=ti.state,
        try_number=ti.try_number,
        map_index=public_index,
        region_id=region_id,
        region_index=region_index,
        duration=ti.duration,
    )


@router.get(
    "/states",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task group not found"),
            (status.HTTP_409_CONFLICT, "Multiple live task instances share a task slot"),
        ]
    ),
)
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
    """Get the states for Task Instances with the given criteria, reporting a looped task's latest pass."""
    run_id_task_state_map: dict[str, dict[str, Any]] = defaultdict(dict)

    coordinates = build_coordinate_filters(TI, map_index=map_index)
    query = select(TI, public_map_index_expression(TI)).where(TI.dag_id == dag_id, *coordinates)

    if task_ids:
        query = query.where(TI.task_id.in_(task_ids))

    if logical_dates:
        query = query.where(TI.logical_date.in_(logical_dates))

    if run_ids:
        query = query.where(TI.run_id.in_(run_ids))

    results = session.execute(query).tuples().all()

    if task_group_id:
        group_tasks = _get_group_tasks(
            dag_id, task_group_id, session, dag_bag, logical_dates, run_ids, map_index
        )

        group_query = select(TI, public_map_index_expression(TI)).where(
            TI.id.in_(ti.id for ti in group_tasks), *coordinates
        )
        group_results = session.execute(group_query).tuples().all()
        results = [*results, *group_results] if task_ids else group_results

    superseded = _find_superseded_ids(
        session,
        dag_id,
        {(task.run_id, task.task_id) for task, _ in results if task.region_id != SENTINEL_REGION_ID},
        dag_bag,
    )
    for task, public_index in results:
        if task.id in superseded:
            continue
        key = task.task_id if public_index < 0 else f"{task.task_id}_{public_index}"
        run_id_task_state_map[task.run_id][key] = task.state

    return TaskStatesResponse(task_states=run_id_task_state_map)


@router.get("/breadcrumbs", status_code=status.HTTP_200_OK)
async def get_task_instance_breadcrumbs(
    dag_id: str,
    run_id: str,
    session: AsyncSessionDep,
) -> TaskBreadcrumbsResponse:
    query = (
        select(
            TI.task_id,
            public_map_index_expression(TI).label("map_index"),
            TI.state,
            TI.operator,
            TI.duration,
            TI.region_id,
            TI.region_index,
        )
        .where(TI.working_set.is_(True))
        .where(TI.dag_id == dag_id, TI.run_id == run_id, TI.state.in_(TerminalTIState))
        .order_by(TI.task_id, public_map_index_expression(TI), TI.region_id, TI.region_index)
    )
    result = (await session.execute(query)).mappings()

    def _iter_breadcrumbs() -> Iterator[dict[str, Any]]:
        for row in result:
            breadcrumb = {str(k): v for k, v in row.items() if k not in {"region_id", "region_index"}}
            region_id, region_index = get_public_region(row.region_id, row.region_index)
            if region_id is not None:
                breadcrumb.update(region_id=region_id, region_index=region_index)
            yield breadcrumb

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
    dag_id: str,
    task_group_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    logical_dates=None,
    run_ids=None,
    map_index: int | None = None,
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
            *([public_map_index_expression(TI) == map_index] if map_index is not None else []),
        )
    ).all()

    return group_tasks


@ti_id_router.get(
    "/{task_instance_id}/validate-inlets-and-outlets",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_404_NOT_FOUND, "Task Instance not found"),
        ]
    ),
)
def validate_inlets_and_outlets(
    task_instance_id: UUID,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> InactiveAssetsResponse:
    """Validate whether there're inactive assets in inlets and outlets of a given task instance."""
    bind_contextvars(ti_id=str(task_instance_id))

    ti = session.scalar(select(TI).where(TI.id == task_instance_id))
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
