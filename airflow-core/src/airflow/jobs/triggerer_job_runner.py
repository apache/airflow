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

import asyncio
import functools
import logging
import os
import selectors
import signal
import sys
import time
from collections import deque
from collections.abc import Generator, Iterable
from contextlib import suppress
from datetime import datetime
from socket import socket
from traceback import format_exception
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, ClassVar, Literal, TextIO, TypedDict

import anyio
import attrs
import structlog
from pydantic import BaseModel, Field, TypeAdapter
from sqlalchemy import func, select
from structlog.contextvars import bind_contextvars as bind_log_contextvars

from airflow._shared.module_loading import import_string
from airflow._shared.observability.metrics.dual_stats_manager import DualStatsManager
from airflow._shared.observability.metrics.stats import Stats
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.executors import workloads
from airflow.executors.workloads.task import TaskInstanceDTO
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import perform_heartbeat
from airflow.models.trigger import Trigger
from airflow.observability.metrics import stats_utils
from airflow.sdk.api.datamodels._generated import HITLDetailResponse
from airflow.sdk.execution_time.comms import (
    CommsDecoder,
    ConnectionResult,
    DagRunStateResult,
    DeleteVariable,
    DeleteXCom,
    DRCount,
    ErrorResponse,
    GetConnection,
    GetDagRunState,
    GetDRCount,
    GetHITLDetailResponse,
    GetPreviousTI,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetXCom,
    MaskSecret,
    OKResponse,
    PutVariable,
    SetXCom,
    TaskStatesResult,
    TICount,
    UpdateHITLDetail,
    VariableResult,
    XComResult,
    _new_encoder,
    _RequestFrame,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess, make_buffered_socket_reader
from airflow.triggers.base import BaseEventTrigger, BaseTrigger, DiscrimatedTriggerEvent, TriggerEvent
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from structlog.typing import FilteringBoundLogger, WrappedLogger

    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
    from airflow.jobs.job import Job
    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels.triggerer import NextTriggersResponse
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI

logger = logging.getLogger(__name__)

__all__ = [
    "TriggerRunner",
    "TriggerRunnerSupervisor",
    "TriggererJobRunner",
]


class TriggererJobRunner(BaseJobRunner, LoggingMixin):
    """
    Run active triggers in asyncio and update their dependent tests/DAGs once their events have fired.

    It runs as two threads:
     - The main thread does DB calls/checkins
     - A subthread runs all the async code
    """

    job_type = "TriggererJob"

    def __init__(
        self,
        job: Job,
        capacity=None,
        queues: set[str] | None = None,
    ):
        super().__init__(job)
        if capacity is None:
            self.capacity = conf.getint("triggerer", "capacity")
        elif isinstance(capacity, int) and capacity > 0:
            self.capacity = capacity
        else:
            raise ValueError(f"Capacity number {capacity!r} is invalid")
        self.queues = queues

    def register_signals(self) -> None:
        """Register signals that stop child processes."""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    @classmethod
    @provide_session
    def is_needed(cls, session) -> bool:
        """
        Test if the triggerer job needs to be run (i.e., if there are triggers in the trigger table).

        This is used for the warning boxes in the UI.
        """
        return session.execute(select(func.count(Trigger.id))).scalar_one() > 0

    def on_kill(self):
        """
        Stop the trigger runner.

        Called when there is an external kill command (via the heartbeat mechanism, for example).
        """
        # TODO: signal instead.
        self.trigger_runner.stop = True

    def _exit_gracefully(self, signum, frame) -> None:
        # The first time, try to exit nicely
        if self.trigger_runner and not self.trigger_runner.stop:
            self.log.info("Exiting gracefully upon receiving signal %s", signum)
            self.trigger_runner.stop = True
        else:
            self.log.warning("Forcing exit due to second exit signal %s", signum)
            if self.trigger_runner:
                self.trigger_runner.kill(signal.SIGKILL)
            sys.exit(os.EX_SOFTWARE)

    def _execute(self) -> int | None:
        self.log.info("Starting the triggerer")
        self.register_signals()
        stats_factory = stats_utils.get_stats_factory(Stats)
        Stats.initialize(factory=stats_factory)
        try:
            # Kick off runner sub-process without DB access
            self.trigger_runner = TriggerRunnerSupervisor.start(
                job=self.job,
                capacity=self.capacity,
                logger=log,
                queues=self.queues,
            )

            # Run the main DB comms loop in this process
            self.trigger_runner.run()
            return self.trigger_runner._exit_code
        except Exception:
            self.log.exception("Exception when executing TriggerRunnerSupervisor.run")
            raise
        finally:
            self.log.info("Waiting for triggers to clean up")
            self.trigger_runner.kill(escalation_delay=10, force=True)
            self.log.info("Exited trigger loop")
        return None


log: FilteringBoundLogger = structlog.get_logger(logger_name=__name__)


# Using this as a simple namespace
class messages:
    class StartTriggerer(BaseModel):
        """Tell the async trigger runner process to start, and where to send status update messages."""

        type: Literal["StartTriggerer"] = "StartTriggerer"

    class TriggerStateChanges(BaseModel):
        """
        Report state change about triggers back to the TriggerRunnerSupervisor.

        The supervisor will respond with a TriggerStateSync message.
        """

        type: Literal["TriggerStateChanges"] = "TriggerStateChanges"
        events: Annotated[
            list[tuple[int, DiscrimatedTriggerEvent]] | None,
            Field(default=None),
        ]
        # Format of list[str] is the exc traceback format
        failures: list[tuple[int, list[str] | None]] | None = None
        finished: list[int] | None = None

    class TriggerStateSync(BaseModel):
        type: Literal["TriggerStateSync"] = "TriggerStateSync"

        to_create: list[workloads.RunTrigger]
        to_cancel: set[int]


class HITLDetailResponseResult(HITLDetailResponse):
    """Response to GetHITLDetailResponse request."""

    type: Literal["HITLDetailResponseResult"] = "HITLDetailResponseResult"

    @classmethod
    def from_api_response(cls, response: HITLDetailResponse) -> HITLDetailResponseResult:
        return cls(**response.model_dump(exclude_defaults=True), type="HITLDetailResponseResult")


ToTriggerRunner = Annotated[
    messages.StartTriggerer
    | messages.TriggerStateSync
    | ConnectionResult
    | VariableResult
    | XComResult
    | DagRunStateResult
    | DRCount
    | TICount
    | TaskStatesResult
    | HITLDetailResponseResult
    | ErrorResponse
    | OKResponse,
    Field(discriminator="type"),
]

ToTriggerSupervisor = Annotated[
    messages.TriggerStateChanges
    | GetConnection
    | DeleteVariable
    | GetVariable
    | PutVariable
    | DeleteXCom
    | GetXCom
    | SetXCom
    | GetTICount
    | GetTaskStates
    | GetDagRunState
    | GetDRCount
    | GetPreviousTI
    | GetHITLDetailResponse
    | UpdateHITLDetail
    | MaskSecret,
    Field(discriminator="type"),
]


@attrs.define(kw_only=True)
class TriggerLoggingFactory:
    log_path: str

    ti: RuntimeTI = attrs.field(repr=False)

    bound_logger: WrappedLogger = attrs.field(init=False, repr=False)

    _filehandle: TextIO | BinaryIO = attrs.field(init=False, repr=False)

    def __call__(self, processors: Iterable[structlog.typing.Processor]) -> WrappedLogger:
        if hasattr(self, "bound_logger"):
            return self.bound_logger

        from airflow.sdk.log import init_log_file

        log_file = init_log_file(self.log_path)

        pretty_logs = False
        if pretty_logs:
            self._filehandle = log_file.open("w", buffering=1)
            underlying_logger: WrappedLogger = structlog.WriteLogger(self._filehandle)
        else:
            self._filehandle = log_file.open("wb")
            underlying_logger = structlog.BytesLogger(self._filehandle)
        logger = structlog.wrap_logger(underlying_logger, processors=processors).bind()
        self.bound_logger = logger
        return logger

    def close(self):
        if hasattr(self, "_filehandle") and self._filehandle and not self._filehandle.closed:
            self._filehandle.close()

    def upload_to_remote(self):
        from airflow.sdk.log import upload_to_remote

        if not hasattr(self, "bound_logger"):
            return

        upload_to_remote(self.bound_logger, self.ti)


def in_process_api_server() -> InProcessExecutionAPI:
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

    api = InProcessExecutionAPI()
    return api


@attrs.define(kw_only=True)
class TriggerRunnerSupervisor(WatchedSubprocess):
    """
    TriggerRunnerSupervisor is responsible for monitoring the subprocess and marshalling DB access.

    This class (which runs in the main/sync process) is responsible for querying the DB via the
    execution API, sending RunTrigger workload messages to the subprocess, and collecting results
    and updating them via the API.

    All direct DB access has been replaced with HTTP calls to the execution API via
    self.client.triggers.*:

      load_triggers()          → POST /triggerer/{id}/next-triggers
      handle_events()          → POST /triggers/{id}/event
      handle_failed_triggers() → POST /triggers/{id}/failure
      clean_unused()           → absorbed into next-triggers endpoint (no separate call needed)
    """

    job: Job
    capacity: int
    queues: set[str] | None = None

    health_check_threshold = conf.getint("triggerer", "triggerer_health_check_threshold")

    runner: TriggerRunner | None = None
    stop: bool = False

    decoder: ClassVar[TypeAdapter[ToTriggerSupervisor]] = TypeAdapter(ToTriggerSupervisor)

    running_triggers: set[int] = attrs.field(factory=set, init=False)
    logger_cache: dict[int, TriggerLoggingFactory] = attrs.field(factory=dict, init=False)
    cancelling_triggers: set[int] = attrs.field(factory=set, init=False)
    creating_triggers: deque[workloads.RunTrigger] = attrs.field(factory=deque, init=False)
    events: deque[tuple[int, TriggerEvent]] = attrs.field(factory=deque, init=False)
    failed_triggers: deque[tuple[int, list[str] | None]] = attrs.field(factory=deque, init=False)

    def is_alive(self) -> bool:
        return self._exit_code is None

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        job: Job,
        logger=None,
        **kwargs,
    ):
        proc = super().start(id=job.id, job=job, target=cls.run_in_process, logger=logger, **kwargs)
        msg = messages.StartTriggerer()
        proc.send_msg(msg, request_id=0)
        return proc

    @functools.cached_property
    def client(self) -> Client:
        from airflow.sdk.api.client import Client

        client = Client(base_url=None, token="", dry_run=True, transport=in_process_api_server().transport)
        client.base_url = "http://in-process.invalid./"
        return client

    def _handle_request(self, msg: ToTriggerSupervisor, log: FilteringBoundLogger, req_id: int) -> None:
        from airflow.sdk.api.datamodels._generated import (
            ConnectionResponse,
            TaskStatesResponse,
            VariableResponse,
            XComResponse,
        )

        resp: BaseModel | None = None
        dump_opts = {}

        if isinstance(msg, messages.TriggerStateChanges):
            if msg.events:
                self.events.extend(msg.events)
            if msg.failures:
                self.failed_triggers.extend(msg.failures)
            for id in msg.finished or ():
                self.running_triggers.discard(id)
                self.cancelling_triggers.discard(id)
                if factory := self.logger_cache.pop(id, None):
                    factory.upload_to_remote()
                    factory.close()

            response = messages.TriggerStateSync(
                to_create=[],
                to_cancel=self.cancelling_triggers,
            )
            while self.creating_triggers:
                workload = self.creating_triggers.popleft()
                response.to_create.append(workload)
            self.running_triggers.update(m.id for m in response.to_create)
            resp = response

        elif isinstance(msg, GetConnection):
            conn = self.client.connections.get(msg.conn_id)
            if isinstance(conn, ConnectionResponse):
                conn_result = ConnectionResult.from_conn_response(conn)
                resp = conn_result
                dump_opts = {"exclude_unset": True, "by_alias": True}
            else:
                resp = conn
        elif isinstance(msg, DeleteVariable):
            resp = self.client.variables.delete(msg.key)
        elif isinstance(msg, GetVariable):
            var = self.client.variables.get(msg.key)
            if isinstance(var, VariableResponse):
                if var.value:
                    from airflow.sdk.log import mask_secret

                    mask_secret(var.value, var.key)
                var_result = VariableResult.from_variable_response(var)
                resp = var_result
                dump_opts = {"exclude_unset": True}
            else:
                resp = var
        elif isinstance(msg, PutVariable):
            self.client.variables.set(msg.key, msg.value, msg.description)
        elif isinstance(msg, DeleteXCom):
            self.client.xcoms.delete(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index)
        elif isinstance(msg, GetXCom):
            xcom = self.client.xcoms.get(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index)
            if isinstance(xcom, XComResponse):
                xcom_result = XComResult.from_xcom_response(xcom)
                resp = xcom_result
                dump_opts = {"exclude_unset": True}
            else:
                resp = xcom
        elif isinstance(msg, SetXCom):
            self.client.xcoms.set(
                msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.value, msg.map_index, msg.mapped_length
            )
        elif isinstance(msg, GetDRCount):
            dr_count = self.client.dag_runs.get_count(
                dag_id=msg.dag_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
                states=msg.states,
            )
            resp = dr_count
        elif isinstance(msg, GetDagRunState):
            dr_resp = self.client.dag_runs.get_state(msg.dag_id, msg.run_id)
            resp = DagRunStateResult.from_api_response(dr_resp)
        elif isinstance(msg, GetTICount):
            resp = self.client.task_instances.get_count(
                dag_id=msg.dag_id,
                map_index=msg.map_index,
                task_ids=msg.task_ids,
                task_group_id=msg.task_group_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
                states=msg.states,
            )
        elif isinstance(msg, GetTaskStates):
            run_id_task_state_map = self.client.task_instances.get_task_states(
                dag_id=msg.dag_id,
                map_index=msg.map_index,
                task_ids=msg.task_ids,
                task_group_id=msg.task_group_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
            )
            if isinstance(run_id_task_state_map, TaskStatesResponse):
                resp = TaskStatesResult.from_api_response(run_id_task_state_map)
            else:
                resp = run_id_task_state_map
        elif isinstance(msg, GetPreviousTI):
            resp = self.client.task_instances.get_previous(
                dag_id=msg.dag_id,
                task_id=msg.task_id,
                logical_date=msg.logical_date,
                map_index=msg.map_index,
                state=msg.state,
            )
        elif isinstance(msg, UpdateHITLDetail):
            api_resp = self.client.hitl.update_response(
                ti_id=msg.ti_id,
                chosen_options=msg.chosen_options,
                params_input=msg.params_input,
            )
            resp = HITLDetailResponseResult.from_api_response(response=api_resp)
        elif isinstance(msg, GetHITLDetailResponse):
            api_resp = self.client.hitl.get_detail_response(ti_id=msg.ti_id)
            resp = HITLDetailResponseResult.from_api_response(response=api_resp)
        elif isinstance(msg, MaskSecret):
            from airflow.sdk.log import mask_secret

            mask_secret(msg.value, msg.name)
        else:
            raise ValueError(f"Unknown message type {type(msg)}")

        self.send_msg(resp, request_id=req_id, error=None, **dump_opts)

    def run(self) -> None:
        """
        Run synchronously; all DB access goes via the execution API.

          load_triggers()          → POST /triggerer/{id}/next-triggers
          handle_events()          → POST /triggers/{id}/event
          handle_failed_triggers() → POST /triggers/{id}/failure
          clean_unused()           → removed; absorbed into next-triggers endpoint
        """
        while not self.stop:
            if not self.is_alive():
                log.error("Trigger runner process has died! Exiting.")
                break
            self.load_triggers()
            self._service_subprocess(1)
            self.handle_events()
            self.handle_failed_triggers()
            # NOTE: clean_unused() removed — now done server-side in next-triggers endpoint
            self.heartbeat()
            self.emit_metrics()

    def heartbeat(self):
        perform_heartbeat(self.job, heartbeat_callback=self.heartbeat_callback, only_if_necessary=True)

    def heartbeat_callback(self, session: Session | None = None) -> None:
        Stats.incr("triggerer_heartbeat", 1, 1)

    def load_triggers(self) -> None:
        """
        Fetch and claim triggers via the execution API.

        Replaces: Trigger.assign_unassigned(), Trigger.ids_for_triggerer()
        and the downstream update_triggers() which called Trigger.bulk_fetch()
        and Trigger.fetch_trigger_ids_with_non_task_associations().
        """
        response: NextTriggersResponse = self.client.triggers.get_next_triggers(
            triggerer_id=self.job.id,
            capacity=self.capacity,
            queues=self.queues,
        )
        self._apply_next_triggers(response)

    def _apply_next_triggers(self, response: NextTriggersResponse) -> None:
        """
        Process the server response and update creating_triggers / cancelling_triggers.

        Replaces update_triggers() which operated on ORM objects from bulk_fetch().
        Logic is otherwise identical — the server has pre-serialized trigger details.
        """
        requested_trigger_ids = {t.id for t in response.triggers}
        non_task_ids = set(response.trigger_ids_with_non_task_associations)

        known_trigger_ids = (
            self.running_triggers.union(x[0] for x in self.events)
            .union(self.cancelling_triggers)
            .union(trigger[0] for trigger in self.failed_triggers)
            .union(trigger.id for trigger in self.creating_triggers)
        )

        new_trigger_ids = requested_trigger_ids - known_trigger_ids
        cancel_trigger_ids = self.running_triggers - requested_trigger_ids

        trigger_detail_by_id = {t.id: t for t in response.triggers}

        to_create: list[workloads.RunTrigger] = []

        for new_id in new_trigger_ids:
            detail = trigger_detail_by_id.get(new_id)
            if not detail:
                log.warning("Trigger disappeared before we could start it", id=new_id)
                continue

            if detail.task_instance is None and new_id not in non_task_ids:
                log.info(
                    (
                        "TaskInstance Trigger is None. It was likely updated by another trigger job. "
                        "Skipping trigger instantiation."
                    ),
                    id=new_id,
                )
                continue

            workload = workloads.RunTrigger(
                classpath=detail.classpath,
                id=new_id,
                encrypted_kwargs=detail.encrypted_kwargs,
                ti=None,
            )

            if detail.task_instance:
                # Convert TriggerTaskInstanceDTO (SDK model) → TaskInstanceDTO (workloads model)
                # RunTrigger.ti requires TaskInstanceDTO which has pool_slots/queue/priority_weight
                ti_dto = TaskInstanceDTO.model_validate(detail.task_instance.model_dump())
                self.logger_cache[new_id] = TriggerLoggingFactory(
                    log_path=f"{detail.log_path}.trigger.{self.job.id}.log",
                    ti=ti_dto,  # type: ignore[arg-type]
                )
                workload.ti = ti_dto
                workload.timeout_after = detail.timeout_after

            to_create.append(workload)

        self.creating_triggers.extend(to_create)

        if cancel_trigger_ids:
            self.cancelling_triggers.update(cancel_trigger_ids)

    def handle_events(self) -> None:
        """
        Dispatch outbound events via the execution API.

        Replaces: Trigger.submit_event(trigger_id, event)
        Now calls: POST /execution/triggers/{trigger_id}/event
        """
        while self.events:
            trigger_id, event = self.events.popleft()
            self.client.triggers.submit_event(
                trigger_id=trigger_id,
                event=event.model_dump(mode="json"),
            )
            Stats.incr("triggers.succeeded")

    def handle_failed_triggers(self) -> None:
        """
        Handle triggers that errored without sending an event.

        Replaces: Trigger.submit_failure(trigger_id, exc)
        Now calls: POST /execution/triggers/{trigger_id}/failure
        """
        while self.failed_triggers:
            trigger_id, saved_exc = self.failed_triggers.popleft()
            self.client.triggers.submit_failure(
                trigger_id=trigger_id,
                error=saved_exc,
            )
            Stats.incr("triggers.failed")

    def emit_metrics(self) -> None:
        DualStatsManager.gauge(
            "triggers.running",
            len(self.running_triggers),
            tags={},
            extra_tags={"hostname": self.job.hostname},
        )
        capacity_left = self.capacity - len(self.running_triggers)
        DualStatsManager.gauge(
            "triggerer.capacity_left",
            capacity_left,
            tags={},
            extra_tags={"hostname": self.job.hostname},
        )

    def _register_pipe_readers(self, stdout: socket, stderr: socket, requests: socket, logs: socket):
        super()._register_pipe_readers(stdout, stderr, requests, logs)
        self.selector.unregister(logs)
        self.selector.register(
            logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                self._process_log_messages_from_subprocess(), on_close=self._on_socket_closed
            ),
        )

    def _process_log_messages_from_subprocess(self) -> Generator[None, bytes | bytearray, None]:
        import msgspec
        from structlog.stdlib import NAME_TO_LEVEL

        from airflow.sdk.log import configure_logging

        configure_logging()

        fallback_log = structlog.get_logger(logger_name=__name__)

        from airflow.sdk.log import logging_processors

        processors = logging_processors(json_output=True)

        def get_logger(trigger_id: int) -> WrappedLogger:
            if factory := self.logger_cache.get(trigger_id):
                return factory(processors)
            return fallback_log

        while True:
            line = yield

            try:
                event: dict[str, Any] = msgspec.json.decode(line)
            except Exception:
                fallback_log.exception("Malformed json log", line=line)
                continue

            if trigger_id := event.pop("trigger_id", None):
                log = get_logger(trigger_id)
            else:
                log = fallback_log

            if exc := event.pop("exception", None):
                event["error_detail"] = exc
            if lvl_name := NAME_TO_LEVEL.get(event.pop("level")):
                log.log(lvl_name, event.pop("event", None), **event)

    @classmethod
    def run_in_process(cls):
        TriggerRunner().run()


class TriggerDetails(TypedDict):
    """Type class for the trigger details dictionary."""

    task: asyncio.Task
    is_watcher: bool
    name: str
    events: int


@attrs.define(kw_only=True)
class TriggerCommsDecoder(CommsDecoder[ToTriggerRunner, ToTriggerSupervisor]):
    _async_writer: asyncio.StreamWriter = attrs.field(alias="async_writer")
    _async_reader: asyncio.StreamReader = attrs.field(alias="async_reader")

    body_decoder: TypeAdapter[ToTriggerRunner] = attrs.field(
        factory=lambda: TypeAdapter(ToTriggerRunner), repr=False
    )

    def _read_frame(self):
        from asgiref.sync import async_to_sync

        return async_to_sync(self._aread_frame)()

    def send(self, msg: ToTriggerSupervisor) -> ToTriggerRunner | None:
        from asgiref.sync import async_to_sync

        return async_to_sync(self.asend)(msg)

    async def _aread_frame(self):
        try:
            len_bytes = await self._async_reader.readexactly(4)
        except ConnectionResetError:
            asyncio.current_task().cancel("Supervisor closed")
        length = int.from_bytes(len_bytes, byteorder="big")
        if length >= 2**32:
            raise OverflowError(f"Refusing to receive messages larger than 4GiB {length=}")
        buffer = await self._async_reader.readexactly(length)
        return self.resp_decoder.decode(buffer)

    async def _aget_response(self, expect_id: int) -> ToTriggerRunner | None:
        frame = await self._aread_frame()
        if frame.id != expect_id:
            raise RuntimeError(f"Response read out of order! Got {frame.id=}, {expect_id=}")
        return self._from_frame(frame)

    async def asend(self, msg: ToTriggerSupervisor) -> ToTriggerRunner | None:
        frame = _RequestFrame(id=next(self.id_counter), body=msg.model_dump())
        bytes = frame.as_bytes()
        async with self._async_lock:
            self._async_writer.write(bytes)
            return await self._aget_response(frame.id)


class TriggerRunner:
    """
    Runtime environment for all triggers.

    Runs inside its own subprocess. All communication with the supervisor
    is done via sockets. This class is UNCHANGED from the original —
    the client-server separation only affects TriggerRunnerSupervisor.
    """

    triggers: dict[int, TriggerDetails]
    trigger_cache: dict[str, type[BaseTrigger]]
    to_create: deque[workloads.RunTrigger]
    to_cancel: deque[int]
    events: deque[tuple[int, TriggerEvent]]
    failed_triggers: deque[tuple[int, BaseException | None]]
    stop: bool = False
    _stop_event: anyio.Event | None = None
    log: FilteringBoundLogger = structlog.get_logger()
    comms_decoder: TriggerCommsDecoder

    def __init__(self):
        super().__init__()
        self.triggers = {}
        self.trigger_cache = {}
        self.to_create = deque()
        self.to_cancel = deque()
        self.events = deque()
        self.failed_triggers = deque()
        self.job_id = None
        self._stop_event = None

    def _handle_signal(self, signum, frame) -> None:
        self.stop = True
        if self._stop_event is not None:
            self._stop_event.set()

    def run(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        asyncio.run(self.arun())

    async def arun(self):
        await self.init_comms()

        watchdog = asyncio.create_task(self.block_watchdog())
        stop_event = self._stop_event = anyio.Event()

        last_status = time.monotonic()
        try:
            while not self.stop:
                if watchdog.done():
                    watchdog.result()
                finished_ids = await self.cleanup_finished_triggers()
                await self.sync_state_to_supervisor(finished_ids)
                await self.create_triggers()
                await self.cancel_triggers()
                with anyio.move_on_after(1):
                    await stop_event.wait()
                if (now := time.monotonic()) - last_status >= 60:
                    watchers = len([t for t in self.triggers.values() if t["is_watcher"]])
                    triggers = len(self.triggers) - watchers
                    self.log.info("%i triggers currently running", triggers)
                    self.log.info("%i watchers currently running", watchers)
                    last_status = now
        except Exception:
            with suppress(BrokenPipeError):
                await log.aexception("Trigger runner failed")
            self.stop = True
            raise
        await watchdog

    async def init_comms(self):
        from airflow.sdk.execution_time import task_runner

        reader, writer = await asyncio.open_connection(sock=socket(fileno=0))
        self.comms_decoder = TriggerCommsDecoder(async_writer=writer, async_reader=reader)
        task_runner.SUPERVISOR_COMMS = self.comms_decoder
        msg = await self.comms_decoder._aget_response(expect_id=0)
        if not isinstance(msg, messages.StartTriggerer):
            raise RuntimeError(f"Required first message to be a messages.StartTriggerer, it was {msg}")

    async def create_triggers(self):
        while self.to_create:
            await asyncio.sleep(0)
            workload = self.to_create.popleft()
            trigger_id = workload.id
            if trigger_id in self.triggers:
                self.log.warning("Trigger %s had insertion attempted twice", trigger_id)
                continue
            try:
                trigger_class = self.get_trigger_by_classpath(workload.classpath)
            except BaseException as e:
                self.log.error("Trigger failed to load code", error=e, classpath=workload.classpath)
                self.failed_triggers.append((trigger_id, e))
                continue
            await asyncio.sleep(0)
            try:
                from airflow.serialization.decoders import smart_decode_trigger_kwargs

                kw = Trigger._decrypt_kwargs(workload.encrypted_kwargs)
                deserialised_kwargs = {k: smart_decode_trigger_kwargs(v) for k, v in kw.items()}
                trigger_instance = trigger_class(**deserialised_kwargs)
            except TypeError as err:
                self.log.error("Trigger failed to inflate", error=err)
                self.failed_triggers.append((trigger_id, err))
                continue
            trigger_instance.trigger_id = trigger_id
            trigger_instance.triggerer_job_id = self.job_id
            trigger_instance.task_instance = ti = workload.ti
            trigger_instance.timeout_after = workload.timeout_after
            trigger_name = (
                f"{ti.dag_id}/{ti.run_id}/{ti.task_id}/{ti.map_index}/{ti.try_number} (ID {trigger_id})"
                if ti
                else f"ID {trigger_id}"
            )
            self.triggers[trigger_id] = {
                "task": asyncio.create_task(
                    self.run_trigger(trigger_id, trigger_instance, workload.timeout_after),
                    name=trigger_name,
                ),
                "is_watcher": isinstance(trigger_instance, BaseEventTrigger),
                "name": trigger_name,
                "events": 0,
            }

    async def cancel_triggers(self):
        while self.to_cancel:
            trigger_id = self.to_cancel.popleft()
            if trigger_id in self.triggers:
                self.triggers[trigger_id]["task"].cancel()
            await asyncio.sleep(0)

    async def cleanup_finished_triggers(self) -> list[int]:
        finished_ids: list[int] = []
        for trigger_id, details in list(self.triggers.items()):
            if details["task"].done():
                finished_ids.append(trigger_id)
                saved_exc = None
                try:
                    result = details["task"].result()
                except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
                    del self.triggers[trigger_id]
                    continue
                except BaseException as e:
                    self.log.exception(
                        "Trigger %s exited with error %s", details["name"], e, trigger_id=trigger_id
                    )
                    saved_exc = e
                else:
                    if isinstance(result, TriggerEvent):
                        self.log.error(
                            "Trigger returned a TriggerEvent rather than yielding it",
                            trigger=details["name"],
                            trigger_id=trigger_id,
                        )
                if details["events"] == 0:
                    self.log.error(
                        "Trigger exited without sending an event. Dependent tasks will be failed.",
                        name=details["name"],
                        trigger_id=trigger_id,
                    )
                    self.failed_triggers.append((trigger_id, saved_exc))
                del self.triggers[trigger_id]
            await asyncio.sleep(0)
        return finished_ids

    def process_trigger_events(self, finished_ids: list[int]) -> messages.TriggerStateChanges:
        events_to_send: list[tuple[int, DiscrimatedTriggerEvent]] = []
        failures_to_send: list[tuple[int, list[str] | None]] = []
        while self.events:
            trigger_id, trigger_event = self.events.popleft()
            events_to_send.append((trigger_id, trigger_event))
        while self.failed_triggers:
            trigger_id, exc = self.failed_triggers.popleft()
            tb = format_exception(type(exc), exc, exc.__traceback__) if exc else None
            failures_to_send.append((trigger_id, tb))
        return messages.TriggerStateChanges(
            events=events_to_send if events_to_send else None,
            finished=finished_ids if finished_ids else None,
            failures=failures_to_send if failures_to_send else None,
        )

    def sanitize_trigger_events(self, msg: messages.TriggerStateChanges) -> messages.TriggerStateChanges:
        req_encoder = _new_encoder()
        events_to_send: list[tuple[int, DiscrimatedTriggerEvent]] = []
        if msg.events:
            for trigger_id, trigger_event in msg.events:
                try:
                    req_encoder.encode(trigger_event)
                except Exception as e:
                    logger.error(
                        "Trigger %s returned non-serializable result %r. Cancelling trigger.",
                        trigger_id,
                        trigger_event,
                    )
                    self.failed_triggers.append((trigger_id, e))
                else:
                    events_to_send.append((trigger_id, trigger_event))
        return messages.TriggerStateChanges(
            events=events_to_send if events_to_send else None,
            finished=msg.finished,
            failures=msg.failures,
        )

    async def sync_state_to_supervisor(self, finished_ids: list[int]) -> None:
        msg = self.process_trigger_events(finished_ids=finished_ids)
        try:
            resp = await self.asend(msg)
        except NotImplementedError:
            resp = await self.asend(self.sanitize_trigger_events(msg))
        if resp:
            self.to_create.extend(resp.to_create)
            self.to_cancel.extend(resp.to_cancel)

    async def asend(self, msg: messages.TriggerStateChanges) -> messages.TriggerStateSync | None:
        try:
            response = await self.comms_decoder.asend(msg)
            if not isinstance(response, messages.TriggerStateSync):
                raise RuntimeError(f"Expected TriggerStateSync, got {type(msg)}")
            return response
        except asyncio.IncompleteReadError:
            if task := asyncio.current_task():
                task.cancel("EOF - shutting down")
                return None
            raise

    async def block_watchdog(self):
        while not self.stop:
            last_run = time.monotonic()
            await asyncio.sleep(0.1)
            time_elapsed = time.monotonic() - last_run
            if time_elapsed > 0.2:
                await self.log.ainfo(
                    "Triggerer's async thread was blocked for %.2f seconds, "
                    "likely by a badly-written trigger. Set PYTHONASYNCIODEBUG=1 "
                    "to get more information on overrunning coroutines.",
                    time_elapsed,
                )
                Stats.incr("triggers.blocked_main_thread")

    async def run_trigger(self, trigger_id: int, trigger: BaseTrigger, timeout_after: datetime | None = None):
        if not os.environ.get("AIRFLOW_DISABLE_GREENBACK_PORTAL", "").lower() == "true":
            import greenback

            await greenback.ensure_portal()

        bind_log_contextvars(trigger_id=trigger_id)
        name = self.triggers[trigger_id]["name"]
        self.log.info("trigger %s starting", name)
        try:
            async for event in trigger.run():
                await self.log.ainfo(
                    "Trigger fired event", name=self.triggers[trigger_id]["name"], result=event
                )
                self.triggers[trigger_id]["events"] += 1
                self.events.append((trigger_id, event))
        except asyncio.CancelledError:
            if timeout := timeout_after:
                timeout = timeout.replace(tzinfo=timezone.utc) if not timeout.tzinfo else timeout
                if timeout < timezone.utcnow():
                    await self.log.aerror("Trigger cancelled due to timeout")
            raise
        finally:
            with suppress(Exception):
                await trigger.cleanup()
            await self.log.ainfo("trigger completed", name=name)

    def get_trigger_by_classpath(self, classpath: str) -> type[BaseTrigger]:
        if classpath not in self.trigger_cache:
            self.trigger_cache[classpath] = import_string(classpath)
        return self.trigger_cache[classpath]
