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
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, TypedDict

import attrs
import structlog
from pydantic import BaseModel, Field, TypeAdapter
from sqlalchemy import func, select
from structlog.contextvars import bind_contextvars as bind_log_contextvars

from airflow._shared.module_loading import import_string
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.executors import workloads
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import perform_heartbeat
from airflow.models.trigger import Trigger
from airflow.observability.stats import Stats
from airflow.observability.trace import DebugTrace, Trace, add_debug_span
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
from airflow.utils.helpers import log_filename_template_renderer
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from structlog.typing import FilteringBoundLogger, WrappedLogger

    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
    from airflow.jobs.job import Job
    from airflow.sdk.api.client import Client
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

            self.trigger_runner.kill(signal.SIGKILL)
            sys.exit(os.EX_SOFTWARE)

    def _execute(self) -> int | None:
        self.log.info("Starting the triggerer")
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
            # Tell the subtproc to stop and then wait for it.
            # If the user interrupts/terms again, _graceful_exit will allow them
            # to force-kill here.
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
            # We have to specify a default here, as otherwise Pydantic struggles to deal with the discriminated
            # union :shrug:
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
        """
        Create result class from API Response.

        API Response is autogenerated from the API schema, so we need to convert it to Result
        for communication between the Supervisor and the task process since it needs a
        discriminator field.
        """
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
"""
The types of messages we can send in to the Trigger Runner process (the process that runs the actual async
code).
"""


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
"""
The types of messages that the async Trigger Runner can send back up to the supervisor process.
"""


@attrs.define(kw_only=True)
class TriggerLoggingFactory:
    log_path: str

    ti: RuntimeTI = attrs.field(repr=False)

    bound_logger: WrappedLogger = attrs.field(init=False, repr=False)

    def __call__(self, processors: Iterable[structlog.typing.Processor]) -> WrappedLogger:
        if hasattr(self, "bound_logger"):
            return self.bound_logger

        from airflow.sdk.log import init_log_file

        log_file = init_log_file(self.log_path)

        pretty_logs = False
        if pretty_logs:
            underlying_logger: WrappedLogger = structlog.WriteLogger(log_file.open("w", buffering=1))
        else:
            underlying_logger = structlog.BytesLogger(log_file.open("wb"))
        logger = structlog.wrap_logger(underlying_logger, processors=processors).bind()
        self.bound_logger = logger
        return logger

    def upload_to_remote(self):
        from airflow.sdk.log import upload_to_remote

        if not hasattr(self, "bound_logger"):
            # Never actually called, nothing to do
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

    This class (which runs in the main/sync process) is responsible for querying the DB, sending RunTrigger
    workload messages to the subprocess, and collecting results and updating them in the DB.
    """

    job: Job
    capacity: int
    queues: set[str] | None = None

    health_check_threshold = conf.getint("triggerer", "triggerer_health_check_threshold")

    runner: TriggerRunner | None = None
    stop: bool = False

    decoder: ClassVar[TypeAdapter[ToTriggerSupervisor]] = TypeAdapter(ToTriggerSupervisor)

    # Maps trigger IDs that we think are running in the sub process
    running_triggers: set[int] = attrs.field(factory=set, init=False)

    logger_cache: dict[int, TriggerLoggingFactory] = attrs.field(factory=dict, init=False)

    # A list of triggers that we have told the async process to cancel. We keep them here until we receive the
    # FinishedTriggers message
    cancelling_triggers: set[int] = attrs.field(factory=set, init=False)

    # A list of RunTrigger workloads to send to the async process when it next checks in. We can't send it
    # directly as all comms has to be initiated by the subprocess
    creating_triggers: deque[workloads.RunTrigger] = attrs.field(factory=deque, init=False)

    # Outbound queue of events
    events: deque[tuple[int, TriggerEvent]] = attrs.field(factory=deque, init=False)

    # Outbound queue of failed triggers
    failed_triggers: deque[tuple[int, list[str] | None]] = attrs.field(factory=deque, init=False)

    def is_alive(self) -> bool:
        # Set by `_service_subprocess` in the loop
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
        # Mypy is wrong -- the setter accepts a string on the property setter! `URLType = URL | str`
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
                # Remove logger from the cache, and since structlog doesn't have an explicit close method, we
                # only need to remove the last reference to it to close the open FH
                if factory := self.logger_cache.pop(id, None):
                    factory.upload_to_remote()

            response = messages.TriggerStateSync(
                to_create=[],
                to_cancel=self.cancelling_triggers,
            )

            # Pull out of these dequeues in a thread-safe manner
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
                # `by_alias=True` is used to convert the `schema` field to `schema_` in the Connection model
                dump_opts = {"exclude_unset": True, "by_alias": True}
            else:
                resp = conn
        elif isinstance(msg, DeleteVariable):
            resp = self.client.variables.delete(msg.key)
        elif isinstance(msg, GetVariable):
            var = self.client.variables.get(msg.key)
            if isinstance(var, VariableResponse):
                # TODO: call for help to figure out why this is needed
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
        """Run synchronously and handle all database reads/writes."""
        while not self.stop:
            if not self.is_alive():
                log.error("Trigger runner process has died! Exiting.")
                break
            with DebugTrace.start_span(span_name="triggerer_job_loop", component="TriggererJobRunner"):
                self.load_triggers()

                # Wait for up to 1 second for activity
                self._service_subprocess(1)

                self.handle_events()
                self.handle_failed_triggers()
                self.clean_unused()
                self.heartbeat()

                self.emit_metrics()

    def heartbeat(self):
        perform_heartbeat(self.job, heartbeat_callback=self.heartbeat_callback, only_if_necessary=True)

    def heartbeat_callback(self, session: Session | None = None) -> None:
        Stats.incr("triggerer_heartbeat", 1, 1)

    @add_debug_span
    def load_triggers(self):
        """Query the database for the triggers we're supposed to be running and update the runner."""
        Trigger.assign_unassigned(
            self.job.id,
            self.capacity,
            self.health_check_threshold,
            queues=self.queues,
        )
        ids = Trigger.ids_for_triggerer(self.job.id, queues=self.queues)
        self.update_triggers(set(ids))

    @add_debug_span
    def handle_events(self):
        """Dispatch outbound events to the Trigger model which pushes them to the relevant task instances."""
        while self.events:
            # Get the event and its trigger ID
            trigger_id, event = self.events.popleft()
            # Tell the model to wake up its tasks
            Trigger.submit_event(trigger_id=trigger_id, event=event)
            # Emit stat event
            Stats.incr("triggers.succeeded")

    @add_debug_span
    def clean_unused(self):
        """Clean out unused or finished triggers."""
        Trigger.clean_unused()

    @add_debug_span
    def handle_failed_triggers(self):
        """
        Handle "failed" triggers. - ones that errored or exited before they sent an event.

        Task Instances that depend on them need failing.
        """
        while self.failed_triggers:
            # Tell the model to fail this trigger's deps
            trigger_id, saved_exc = self.failed_triggers.popleft()
            Trigger.submit_failure(trigger_id=trigger_id, exc=saved_exc)
            # Emit stat event
            Stats.incr("triggers.failed")

    def emit_metrics(self):
        Stats.gauge(f"triggers.running.{self.job.hostname}", len(self.running_triggers))
        Stats.gauge("triggers.running", len(self.running_triggers), tags={"hostname": self.job.hostname})

        capacity_left = self.capacity - len(self.running_triggers)
        Stats.gauge(f"triggerer.capacity_left.{self.job.hostname}", capacity_left)
        Stats.gauge("triggerer.capacity_left", capacity_left, tags={"hostname": self.job.hostname})

        span = Trace.get_current_span()
        span.set_attributes(
            {
                "trigger host": self.job.hostname,
                "triggers running": len(self.running_triggers),
                "capacity left": capacity_left,
            }
        )

    def update_triggers(self, requested_trigger_ids: set[int]):
        """
        Request that we update what triggers we're running.

        Works out the differences - ones to add, and ones to remove - then
        adds them to the dequeues so the subprocess can actually mutate the running
        trigger set.
        """
        render_log_fname = log_filename_template_renderer()

        known_trigger_ids = (
            self.running_triggers.union(x[0] for x in self.events)
            .union(self.cancelling_triggers)
            .union(trigger[0] for trigger in self.failed_triggers)
            .union(trigger.id for trigger in self.creating_triggers)
        )
        # Work out the two difference sets
        new_trigger_ids = requested_trigger_ids - known_trigger_ids
        cancel_trigger_ids = self.running_triggers - requested_trigger_ids
        # Bulk-fetch new trigger records
        new_triggers = Trigger.bulk_fetch(new_trigger_ids)
        trigger_ids_with_non_task_associations = Trigger.fetch_trigger_ids_with_non_task_associations()
        to_create: list[workloads.RunTrigger] = []
        # Add in new triggers
        for new_id in new_trigger_ids:
            # Check it didn't vanish in the meantime
            if new_id not in new_triggers:
                log.warning("Trigger disappeared before we could start it", id=new_id)
                continue

            new_trigger_orm = new_triggers[new_id]

            # If the trigger is not associated to a task, an asset, or a callback, this means the TaskInstance
            # row was updated by either Trigger.submit_event or Trigger.submit_failure
            # and can happen when a single trigger Job is being run on multiple TriggerRunners
            # in a High-Availability setup.
            if new_trigger_orm.task_instance is None and new_id not in trigger_ids_with_non_task_associations:
                log.info(
                    (
                        "TaskInstance Trigger is None. It was likely updated by another trigger job. "
                        "Skipping trigger instantiation."
                    ),
                    id=new_id,
                )
                continue

            workload = workloads.RunTrigger(
                classpath=new_trigger_orm.classpath,
                id=new_id,
                encrypted_kwargs=new_trigger_orm.encrypted_kwargs,
                ti=None,
            )
            if new_trigger_orm.task_instance:
                log_path = render_log_fname(ti=new_trigger_orm.task_instance)
                if not new_trigger_orm.task_instance.dag_version_id:
                    # This is to handle 2 to 3 upgrade where TI.dag_version_id can be none
                    log.warning(
                        "TaskInstance associated with Trigger has no associated Dag Version, skipping the trigger",
                        ti_id=new_trigger_orm.task_instance.id,
                    )
                    continue
                ser_ti = workloads.TaskInstance.model_validate(
                    new_trigger_orm.task_instance, from_attributes=True
                )
                # When producing logs from TIs, include the job id producing the logs to disambiguate it.
                self.logger_cache[new_id] = TriggerLoggingFactory(
                    log_path=f"{log_path}.trigger.{self.job.id}.log",
                    ti=ser_ti,  # type: ignore
                )

                workload.ti = ser_ti
                workload.timeout_after = new_trigger_orm.task_instance.trigger_timeout

            to_create.append(workload)

        self.creating_triggers.extend(to_create)

        if cancel_trigger_ids:
            # Enqueue orphaned triggers for cancellation
            self.cancelling_triggers.update(cancel_trigger_ids)

    def _register_pipe_readers(self, stdout: socket, stderr: socket, requests: socket, logs: socket):
        super()._register_pipe_readers(stdout, stderr, requests, logs)

        # We want to handle logging differently here, so un-register the one our parent class created
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
            # TODO: Is a separate dict worth it, or should we make `self.running_triggers` a dict?
            if factory := self.logger_cache.get(trigger_id):
                return factory(processors)
            return fallback_log

        # We need to look at the json, pull out the
        while True:
            # Generator receive syntax, values are "sent" in  by the `make_buffered_socket_reader` and returned to
            # the yield.
            line = yield

            try:
                event: dict[str, Any] = msgspec.json.decode(line)
            except Exception:
                fallback_log.exception("Malformed json log", line=line)
                continue

            if trigger_id := event.pop("trigger_id", None):
                log = get_logger(trigger_id)
            else:
                # Log message about the TriggerRunner itself -- just output it
                log = fallback_log

            if exc := event.pop("exception", None):
                # TODO: convert the dict back to a pretty stack trace
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

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, repr=False)

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
            # Given the lock we take out in `asend`, this _shouldn't_ be possible, but I'd rather fail with
            # this explicit error return the wrong type of message back to a Trigger
            raise RuntimeError(f"Response read out of order! Got {frame.id=}, {expect_id=}")
        return self._from_frame(frame)

    async def asend(self, msg: ToTriggerSupervisor) -> ToTriggerRunner | None:
        frame = _RequestFrame(id=next(self.id_counter), body=msg.model_dump())
        bytes = frame.as_bytes()

        async with self._lock:
            self._async_writer.write(bytes)

            return await self._aget_response(frame.id)


class TriggerRunner:
    """
    Runtime environment for all triggers.

    Mainly runs inside its own process, where it hands control off to an asyncio
    event loop. All communication between this and it's (sync) supervisor is done via sockets
    """

    # Maps trigger IDs to their running tasks and other info
    triggers: dict[int, TriggerDetails]

    # Cache for looking up triggers by classpath
    trigger_cache: dict[str, type[BaseTrigger]]

    # Inbound queue of new triggers
    to_create: deque[workloads.RunTrigger]

    # Inbound queue of deleted triggers
    to_cancel: deque[int]

    # Outbound queue of events
    events: deque[tuple[int, TriggerEvent]]

    # Outbound queue of failed triggers
    failed_triggers: deque[tuple[int, BaseException | None]]

    # Should-we-stop flag
    # TODO: set this in a sig-int handler
    stop: bool = False

    # TODO: connect this to the parent process
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

    def run(self):
        """Sync entrypoint - just run a run in an async loop."""
        asyncio.run(self.arun())

    async def arun(self):
        """
        Run trigger addition/deletion/cleanup; main (asynchronous) logic loop.

        Actual triggers run in their own separate coroutines.
        """
        # Make sure comms are initialized before allowing any Triggers to run
        await self.init_comms()

        watchdog = asyncio.create_task(self.block_watchdog())

        last_status = time.monotonic()
        try:
            while not self.stop:
                # Raise exceptions from the tasks
                if watchdog.done():
                    watchdog.result()

                # Run core logic

                finished_ids = await self.cleanup_finished_triggers()
                # This also loads the triggers we need to create or cancel
                await self.sync_state_to_supervisor(finished_ids)
                await self.create_triggers()
                await self.cancel_triggers()
                # Sleep for a bit
                await asyncio.sleep(1)
                # Every minute, log status
                if (now := time.monotonic()) - last_status >= 60:
                    watchers = len([trigger for trigger in self.triggers.values() if trigger["is_watcher"]])
                    triggers = len(self.triggers) - watchers
                    self.log.info("%i triggers currently running", triggers)
                    self.log.info("%i watchers currently running", watchers)
                    last_status = now

        except Exception:
            with suppress(BrokenPipeError):
                await log.aexception("Trigger runner failed")
            self.stop = True
            raise
        # Wait for supporting tasks to complete
        await watchdog

    async def init_comms(self):
        """
        Set up the communications pipe between this process and the supervisor.

        This also sets up the SUPERVISOR_COMMS so that TaskSDK code can work as expected too (but that will
        need to be wrapped in an ``sync_to_async()`` call)
        """
        from airflow.sdk.execution_time import task_runner

        # Yes, we read and write to stdin! It's a socket, not a normal stdin.
        reader, writer = await asyncio.open_connection(sock=socket(fileno=0))

        self.comms_decoder = TriggerCommsDecoder(
            async_writer=writer,
            async_reader=reader,
        )

        task_runner.SUPERVISOR_COMMS = self.comms_decoder

        msg = await self.comms_decoder._aget_response(expect_id=0)

        if not isinstance(msg, messages.StartTriggerer):
            raise RuntimeError(f"Required first message to be a messages.StartTriggerer, it was {msg}")

    async def create_triggers(self):
        """Drain the to_create queue and create all new triggers that have been requested in the DB."""
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
                # Either the trigger code or the path to it is bad. Fail the trigger.
                self.log.error("Trigger failed to load code", error=e, classpath=workload.classpath)
                self.failed_triggers.append((trigger_id, e))
                continue

            # Loading the trigger class could have been expensive. Lets give other things a chance to run!
            await asyncio.sleep(0)

            try:
                from airflow.serialization.decoders import smart_decode_trigger_kwargs

                # Decrypt and clean trigger kwargs before for execution
                # Note: We only clean up serialization artifacts (__var, __type keys) here,
                # not in `_decrypt_kwargs` because it is used during hash comparison in
                # add_asset_trigger_references and could lead to adverse effects like hash mismatches
                # that could cause None values in collections.
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
                    self.run_trigger(trigger_id, trigger_instance, workload.timeout_after), name=trigger_name
                ),
                "is_watcher": isinstance(trigger_instance, BaseEventTrigger),
                "name": trigger_name,
                "events": 0,
            }

    async def cancel_triggers(self):
        """
        Drain the to_cancel queue and ensure all triggers that are not in the DB are cancelled.

        This allows the cleanup job to delete them.
        """
        while self.to_cancel:
            trigger_id = self.to_cancel.popleft()
            if trigger_id in self.triggers:
                # We only delete if it did not exit already
                self.triggers[trigger_id]["task"].cancel()
            await asyncio.sleep(0)

    async def cleanup_finished_triggers(self) -> list[int]:
        """
        Go through all trigger tasks (coroutines) and clean up entries for ones that have exited.

        Optionally warn users if the exit was not normal.
        """
        finished_ids: list[int] = []
        for trigger_id, details in list(self.triggers.items()):
            if details["task"].done():
                finished_ids.append(trigger_id)
                # Check to see if it exited for good reasons
                saved_exc = None
                try:
                    result = details["task"].result()
                except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
                    # These are "expected" exceptions and we stop processing here
                    # If we don't, then the system requesting a trigger be removed -
                    # which turns into CancelledError - results in a failure.
                    del self.triggers[trigger_id]
                    continue
                except BaseException as e:
                    # This is potentially bad, so log it.
                    self.log.exception(
                        "Trigger %s exited with error %s", details["name"], e, trigger_id=trigger_id
                    )
                    saved_exc = e
                else:
                    # See if they foolishly returned a TriggerEvent
                    if isinstance(result, TriggerEvent):
                        self.log.error(
                            "Trigger returned a TriggerEvent rather than yielding it",
                            trigger=details["name"],
                            trigger_id=trigger_id,
                        )
                # See if this exited without sending an event, in which case
                # any task instances depending on it need to be failed
                if details["events"] == 0:
                    self.log.error(
                        "Trigger exited without sending an event. Dependent tasks will be failed.",
                        name=details["name"],
                        trigger_id=trigger_id,
                    )
                    # TODO: better formatting of the exception?
                    self.failed_triggers.append((trigger_id, saved_exc))
                del self.triggers[trigger_id]
            await asyncio.sleep(0)
        return finished_ids

    async def sync_state_to_supervisor(self, finished_ids: list[int]):
        # Copy out of our dequeues in threadsafe manner to sync state with parent
        events_to_send = []
        while self.events:
            data = self.events.popleft()
            events_to_send.append(data)

        failures_to_send = []
        while self.failed_triggers:
            id, exc = self.failed_triggers.popleft()
            tb = format_exception(type(exc), exc, exc.__traceback__) if exc else None
            failures_to_send.append((id, tb))

        msg = messages.TriggerStateChanges(
            events=events_to_send, finished=finished_ids, failures=failures_to_send
        )

        if not events_to_send:
            msg.events = None

        if not failures_to_send:
            msg.failures = None

        if not finished_ids:
            msg.finished = None

        # Tell the monitor that we've finished triggers so it can update things
        resp = await self.send_changes(msg)

        if resp:
            self.to_create.extend(resp.to_create)
            self.to_cancel.extend(resp.to_cancel)

    async def send_changes(self, msg: messages.TriggerStateChanges) -> messages.TriggerStateSync | None:
        try:
            response = await self.comms_decoder.asend(msg)

            if not isinstance(response, messages.TriggerStateSync):
                raise RuntimeError(f"Expected to get a TriggerStateSync message, instead we got {type(msg)}")

            return response
        except asyncio.IncompleteReadError:
            if task := asyncio.current_task():
                task.cancel("EOF - shutting down")
                return None
            raise
        except NotImplementedError:
            validated_events = self.validate_events(msg)
            if not validated_events:
                raise
            msg.events = validated_events
            return await self.send_changes(msg)

    def validate_events(self, msg: messages.TriggerStateChanges) -> list[tuple[int, DiscrimatedTriggerEvent]]:
        validated_events: list[tuple[int, DiscrimatedTriggerEvent]] = []

        if msg.events:
            req_encoder = _new_encoder()

            for trigger_id, trigger_event in msg.events:
                try:
                    req_encoder.encode(trigger_event)
                    validated_events.append((trigger_id, trigger_event))
                except NotImplementedError:
                    logger.error(
                        "Trigger %s returned non-serializable result %r. Cancelling trigger.",
                        trigger_id,
                        trigger_event,
                    )
                    self.to_cancel.append(trigger_id)

        return validated_events

    async def block_watchdog(self):
        """
        Watchdog loop that detects blocking (badly-written) triggers.

        Triggers should be well-behaved async coroutines and await whenever
        they need to wait; this loop tries to run every 100ms to see if
        there are badly-written triggers taking longer than that and blocking
        the event loop.

        Unfortunately, we can't tell what trigger is blocking things, but
        we can at least detect the top-level problem.
        """
        while not self.stop:
            last_run = time.monotonic()
            await asyncio.sleep(0.1)
            # We allow a generous amount of buffer room for now, since it might
            # be a busy event loop.
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
        """Run a trigger (they are async generators) and push their events into our outbound event deque."""
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
            # We get cancelled by the scheduler changing the task state. But if we do lets give a nice error
            # message about it
            if timeout := timeout_after:
                timeout = timeout.replace(tzinfo=timezone.utc) if not timeout.tzinfo else timeout
                if timeout < timezone.utcnow():
                    await self.log.aerror("Trigger cancelled due to timeout")
            raise
        finally:
            # CancelledError will get injected when we're stopped - which is
            # fine, the cleanup process will understand that, but we want to
            # allow triggers a chance to cleanup, either in that case or if
            # they exit cleanly. Exception from cleanup methods are ignored.
            with suppress(Exception):
                await trigger.cleanup()

            await self.log.ainfo("trigger completed", name=name)

    def get_trigger_by_classpath(self, classpath: str) -> type[BaseTrigger]:
        """
        Get a trigger class by its classpath ("path.to.module.classname").

        Uses a cache dictionary to speed up lookups after the first time.
        """
        if classpath not in self.trigger_cache:
            self.trigger_cache[classpath] = import_string(classpath)
        return self.trigger_cache[classpath]
