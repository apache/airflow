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
import math
import os
import selectors
import signal
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Generator, Hashable, Iterable, Iterator
from contextlib import contextmanager, suppress
from datetime import datetime
from socket import socket
from traceback import format_exception
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, ClassVar, Literal, TextIO, TypedDict
from uuid import uuid4

import anyio
import attrs
import greenback
import httpx
import structlog
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from pydantic import BaseModel, Field, TypeAdapter
from sqlalchemy import func, select
from structlog.contextvars import bind_contextvars as bind_log_contextvars

from airflow._shared.module_loading import import_string
from airflow._shared.observability.metrics import stats
from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import JWTGenerator, get_signing_args
from airflow.configuration import conf

# Imported at runtime (not under TYPE_CHECKING) because Pydantic models below
# (e.g. TriggerStateSync) resolve `workloads.RunTrigger` annotations at class build time.
from airflow.executors import workloads
from airflow.executors.base_executor import get_execution_api_server_url
from airflow.executors.workloads.task import TaskInstanceDTO
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import Job
from airflow.models.trigger import Trigger
from airflow.observability.metrics import stats_utils
from airflow.sdk.api.client import Client, ServerResponseError
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
    GetVariableKeys,
    GetXCom,
    MaskSecret,
    OKResponse,
    PutVariable,
    SetXCom,
    TaskStatesResult,
    TICount,
    UpdateHITLDetail,
    VariableKeysResult,
    VariableResult,
    XComResult,
    _new_encoder,
    _RequestFrame,
)
from airflow.sdk.execution_time.request_handlers import (
    handle_delete_variable,
    handle_delete_xcom,
    handle_get_connection,
    handle_get_dag_run_state,
    handle_get_dr_count,
    handle_get_previous_ti,
    handle_get_task_states,
    handle_get_ti_count,
    handle_get_variable,
    handle_get_variable_keys,
    handle_get_xcom,
    handle_mask_secret,
    handle_put_variable,
    handle_set_xcom,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess, make_buffered_socket_reader
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.serde import serialize as serde_serialize
from airflow.serialization.serialized_objects import DagSerialization
from airflow.triggers.base import BaseEventTrigger, BaseTrigger, DiscrimatedTriggerEvent, TriggerEvent
from airflow.triggers.shared_stream import SharedStreamManager
from airflow.utils.helpers import log_filename_template_renderer
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.session import provide_session

if TYPE_CHECKING:
    from opentelemetry.util._decorator import _AgnosticContextManager
    from sqlalchemy.orm import Session
    from structlog.typing import FilteringBoundLogger, WrappedLogger

    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)
# Private sentinel passed as the cancel message when a trigger is cancelled by user action
_USER_ACTION_CANCEL_MSG = "__airflow_user_action__"

_ON_CANCEL_TIMEOUT: int = conf.getint("triggerer", "on_kill_timeout", fallback=30)

# Sentinel ``sub`` accepted by the execution_api ``require_auth`` (scope defaults to "execution";
# the trigger/job routes do not enforce ``ti:self``). Mirrors the parse-time token pattern.
_SENTINEL_SUB = "00000000-0000-0000-0000-000000000000"

# httpx/Client errors caught at the guarded per-loop call sites. This is deliberately broad
# (it catches 4xx as well as 5xx) so the ``except`` clauses are cheap to write; ``_is_transient``
# below then re-raises the non-transient ones so a persistent 4xx crashes loudly instead of
# looping forever at WARNING.
_TRANSIENT_API_ERRORS = (ServerResponseError, httpx.HTTPError)


def _is_transient(exc: Exception) -> bool:
    """
    Return ``True`` only for failures worth retrying next loop (a transient API blip).

    A persistent 4xx (e.g. a malformed body or auth problem) is a logic bug the client never
    retries; swallowing it would wedge the triggerer silently. ``ServerResponseError`` subclasses
    ``httpx.HTTPStatusError``, so the first branch covers both.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    # Connection resets, timeouts, DNS failures, etc. -- genuinely transient.
    return isinstance(exc, httpx.TransportError)


# How long to keep retrying the initial Job registration while the api-server is still starting up.
# The triggerer often boots alongside the api-server (e.g. docker-compose), so an early transient
# error means "not ready yet, wait" rather than a fatal misconfiguration. After this, give up loudly.
_STARTUP_REGISTER_TIMEOUT = 300.0


def _make_trigger_span(
    ti: TaskInstanceDTO | None, trigger_id: int, name: str
) -> _AgnosticContextManager[trace.Span]:
    parent_context = (
        TraceContextTextMapPropagator().extract(ti.context_carrier) if ti and ti.context_carrier else None
    )
    attributes: dict[str, str | int] = {
        "airflow.trigger.name": name,
    }
    if isinstance(ti, TaskInstanceDTO):
        span_name = f"trigger.{ti.task_id}" if ti else f"trigger.{trigger_id}"
        if ti.map_index >= 0:
            span_name += f"_{ti.map_index}"
        attributes = {
            **attributes,
            "airflow.dag_id": ti.dag_id,
            "airflow.task_id": ti.task_id,
            "airflow.dag_run.run_id": ti.run_id,
            "airflow.task_instance.try_number": ti.try_number,
            "airflow.task_instance.map_index": ti.map_index,
        }
    else:
        span_name = f"trigger.{name}"
    return tracer.start_as_current_span(span_name, attributes=attributes, context=parent_context)


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
        team_name: str | None = None,
    ):
        super().__init__(job)
        if capacity is None:
            self.capacity = conf.getint("triggerer", "capacity")
        elif isinstance(capacity, int) and capacity > 0:
            self.capacity = capacity
        else:
            raise ValueError(f"Capacity number {capacity!r} is invalid")
        self.queues = queues
        self.team_name = team_name
        # Set up only when _execute() starts the subprocess; keep it defined so that
        # signal handlers (or other code) firing before startup don't hit AttributeError.
        self.trigger_runner: TriggerRunnerSupervisor | None = None

    def register_signals(self) -> None:
        """Register signals that stop child processes."""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    @classmethod
    @provide_session
    def is_needed(cls, *, session: Session) -> bool:
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
        stats.initialize(
            factory=stats_utils.get_stats_factory(),
            export_legacy_names=conf.getboolean("metrics", "legacy_names_on"),
        )
        try:
            # AIP-92: the triggerer orchestrates through the Execution API with no metadata-DB
            # access. Kick off the runner sub-process without DB access. ``job=None``: the
            # supervisor registers and heartbeats its liveness Job through the Execution API,
            # not a metadata-DB row.
            self.trigger_runner = TriggerRunnerSupervisor.start(
                job=None,
                capacity=self.capacity,
                logger=log,
                queues=self.queues,
                team_name=self.team_name,
            )

            # Run the main comms loop in this process
            self.trigger_runner.run()
            return self.trigger_runner._exit_code
        except Exception:
            self.log.exception("Exception when executing TriggerRunnerSupervisor.run")
            raise
        finally:
            self.log.info("Waiting for triggers to clean up")
            # Tell the subtproc to stop and then wait for it.
            # If the user interrupts/terms again, _graceful_exit will allow them
            # to force-kill here. trigger_runner may be None if start() raised.
            if self.trigger_runner is not None:
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
    | VariableKeysResult
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
    | GetVariableKeys
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
        # Explicitly close the file descriptor.
        if hasattr(self, "_filehandle") and self._filehandle and not self._filehandle.closed:
            self._filehandle.close()

    def upload_to_remote(self):
        from airflow.sdk.log import upload_to_remote

        if not hasattr(self, "bound_logger"):
            # Never actually called, nothing to do
            return

        upload_to_remote(self.bound_logger, self.ti)


@attrs.define(kw_only=True)
class TriggerRunnerSupervisor(WatchedSubprocess):
    """
    Monitor the async TriggerRunner subprocess and orchestrate triggers over the Execution API.

    This class (which runs in the main/sync process) holds **no** metadata-DB access (AIP-92).
    Instead of reading/writing the ``trigger``/``job`` tables directly, it routes all
    orchestration through the Execution API: it registers a ``Job`` over HTTP, claims triggers,
    fetches their ``RunTrigger`` workloads, sends them to the subprocess, and reports
    events/failures/cleanup back to the server.

    ``job`` is always ``None``: the supervisor registers and heartbeats a liveness ``Job`` through
    the Execution API rather than a metadata-DB row. Keeping the supervisor DB-free is what lets it
    eventually move into the ``task_sdk`` module, which has no metadata-DB access.
    """

    job: Job | None = None
    capacity: int
    queues: set[str] | None = None
    team_name: str | None = None

    health_check_threshold = conf.getint("triggerer", "triggerer_health_check_threshold")
    runner_health_check_threshold = conf.getfloat("triggerer", "runner_health_check_threshold")

    runner: TriggerRunner | None = None
    stop: bool = False

    # Id of the Job row this triggerer registered over the API. Used wherever a Job id is needed
    # (heartbeat, trigger-load, log filename). Set in ``run_context`` once the client is available.
    _triggerer_id: int | None = attrs.field(init=False, default=None)

    # Monotonic timestamp of the last successful Job heartbeat, used to throttle to the heartrate
    # so we don't POST every single loop iteration.
    _last_heartbeat: float = attrs.field(init=False, default=0.0)
    _heartrate: float = attrs.field(init=False, factory=lambda: Job._heartrate("TriggererJob"))

    # Lazily-built log-filename renderer (DB-free: reads only ``logging.log_filename_template``).
    # Used to reconstruct the per-trigger log path so logs are captured and uploaded on finish.
    _log_fname_renderer: Callable[..., str] | None = attrs.field(init=False, default=None)

    # Timestamp of the last message received from the TriggerRunner subprocess.  Updated on
    # every message; if it goes silent for longer than runner_health_check_threshold the
    # subprocess's async event loop has likely deadlocked.  Initialised to +inf so the watchdog
    # stays silent until the first message arrives — avoids false positives on slow/cold-start
    # hosts where startup can exceed the threshold.
    _last_runner_comms: float = attrs.field(init=False, default=math.inf)
    _runner_comms_silence_logged: bool = attrs.field(init=False, default=False)

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
        job: Job | None = None,
        logger=None,
        **kwargs,
    ):
        proc_id = job.id if job is not None else uuid4()
        proc = super().start(id=proc_id, job=job, target=cls.run_in_process, logger=logger, **kwargs)

        msg = messages.StartTriggerer()
        proc.send_msg(msg, request_id=0)
        return proc

    @functools.cached_property
    def client(self) -> Client:
        return self.make_client()

    def make_client(self) -> Client:
        """Build a REMOTE client pointing at the execution API, authenticated with a minted token."""
        token = JWTGenerator(
            valid_for=conf.getint("execution_api", "jwt_expiration_time"),
            audience=conf.get_mandatory_list_value("execution_api", "jwt_audience")[0],
            issuer=conf.get("api_auth", "jwt_issuer", fallback=None),
            **get_signing_args(make_secret_key_if_needed=False),
        ).generate({"sub": _SENTINEL_SUB})
        return Client(base_url=get_execution_api_server_url(), token=token, dry_run=False)

    @property
    def triggerer_id(self) -> int:
        if self._triggerer_id is None:
            raise RuntimeError(
                "Triggerer Job has not been registered yet; run_context() must run before this is used."
            )
        return self._triggerer_id

    def _handle_request(self, msg: ToTriggerSupervisor, log: FilteringBoundLogger, req_id: int) -> None:

        resp: BaseModel | None = None
        dump_opts: dict[str, bool] = {}
        self._last_runner_comms = time.monotonic()

        if isinstance(msg, messages.TriggerStateChanges):
            if msg.events:
                self.events.extend(msg.events)
            if msg.failures:
                self.failed_triggers.extend(msg.failures)
            for id in msg.finished or ():
                self.running_triggers.discard(id)
                self.cancelling_triggers.discard(id)
                if factory := self.logger_cache.pop(id, None):
                    try:
                        factory.upload_to_remote()
                    except Exception:
                        log.exception("Failed to upload trigger logs to remote", trigger_id=id)
                    finally:
                        # Close the FD explicitly even if upload raised, otherwise the file
                        # handle leaks for every failed upload.
                        factory.close()

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
            resp, dump_opts = handle_get_connection(self.client, msg)
        elif isinstance(msg, DeleteVariable):
            resp, dump_opts = handle_delete_variable(self.client, msg)
        elif isinstance(msg, GetVariable):
            resp, dump_opts = handle_get_variable(self.client, msg)
        elif isinstance(msg, GetVariableKeys):
            resp, dump_opts = handle_get_variable_keys(self.client, msg)
        elif isinstance(msg, PutVariable):
            resp, dump_opts = handle_put_variable(self.client, msg)
        elif isinstance(msg, DeleteXCom):
            resp, dump_opts = handle_delete_xcom(self.client, msg)
        elif isinstance(msg, GetXCom):
            resp, dump_opts = handle_get_xcom(self.client, msg)
        elif isinstance(msg, SetXCom):
            resp, dump_opts = handle_set_xcom(self.client, msg)
        elif isinstance(msg, GetDRCount):
            resp, dump_opts = handle_get_dr_count(self.client, msg)
        elif isinstance(msg, GetDagRunState):
            resp, dump_opts = handle_get_dag_run_state(self.client, msg)

        elif isinstance(msg, GetTICount):
            resp, dump_opts = handle_get_ti_count(self.client, msg)

        elif isinstance(msg, GetTaskStates):
            resp, dump_opts = handle_get_task_states(self.client, msg)
        elif isinstance(msg, GetPreviousTI):
            resp, dump_opts = handle_get_previous_ti(self.client, msg)
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
            handle_mask_secret(msg)
        else:
            raise ValueError(f"Unknown message type {type(msg)}")

        self.send_msg(resp, request_id=req_id, error=None, **dump_opts)

    def run(self) -> None:
        """Run the synchronous supervisor loop, orchestrating triggers over the Execution API."""
        with self.run_context():
            while not self.should_stop():
                if not self.is_alive():
                    log.error("Trigger runner process has died! Exiting.")
                    break
                self.run_once()

    def _register_triggerer_job(self) -> int:
        """
        Register the liveness Job over the API, retrying while the api-server is still starting.

        The triggerer can boot before the api-server is ready to serve (they often start together),
        so a transient error here means "not up yet, wait" rather than a fatal misconfiguration.
        Retry with capped backoff until ``_STARTUP_REGISTER_TIMEOUT``, then let the error propagate.
        """
        deadline = time.monotonic() + _STARTUP_REGISTER_TIMEOUT
        delay = 1.0
        while True:
            try:
                return self.client.jobs.register("TriggererJob", get_hostname())
            except _TRANSIENT_API_ERRORS as e:
                if not _is_transient(e) or time.monotonic() >= deadline:
                    raise
                log.warning("Execution API not ready; retrying triggerer Job registration", exc_info=True)
                time.sleep(delay)
                delay = min(delay * 2, 10.0)

    @contextmanager
    def run_context(self) -> Iterator[None]:
        """Register a Job over the API once before the run loop starts; finalize it on shutdown."""
        self._triggerer_id = self._register_triggerer_job()
        log.info("Registered triggerer Job via Execution API", triggerer_id=self._triggerer_id)
        try:
            yield
        finally:
            # Stamp the Job's end_date so it isn't left looking RUNNING forever. Best-effort:
            # a transient API blip here shouldn't mask the real shutdown reason.
            if self._triggerer_id is not None:
                try:
                    self.client.jobs.complete(self._triggerer_id)
                except _TRANSIENT_API_ERRORS:
                    log.warning("Failed to finalize triggerer Job via Execution API", exc_info=True)

    def should_stop(self) -> bool:
        """Return True when the run loop should exit."""
        return self.stop

    def run_once(self) -> None:
        """Perform a single iteration of the run loop."""
        self.load_triggers()

        # Wait for up to 1 second for activity
        self._service_subprocess(1)

        self.handle_events()
        self.handle_failed_triggers()
        self.clean_unused()
        self.heartbeat()

        self.emit_metrics()

    def heartbeat(self) -> None:
        """
        Heartbeat the Job over the API, throttled to the job heartrate.

        Keeps the runner-deadlock silence check so a hung event loop still stops heartbeating
        (which lets the scheduler reassign this triggerer's triggers).
        """
        elapsed = time.monotonic() - self._last_runner_comms
        if self.runner_health_check_threshold > 0 and elapsed > self.runner_health_check_threshold:
            if not self._runner_comms_silence_logged:
                log.error(
                    "TriggerRunner subprocess event loop appears deadlocked: no communication received "
                    "for %.1fs (threshold: %.1fs). Skipping heartbeat so the triggerer appears unhealthy "
                    "to the scheduler and its triggers are reassigned.",
                    elapsed,
                    self.runner_health_check_threshold,
                )
                self._runner_comms_silence_logged = True
            return
        self._runner_comms_silence_logged = False

        now = time.monotonic()
        if self._heartrate > 0 and (now - self._last_heartbeat) < self._heartrate:
            return
        try:
            self.client.jobs.heartbeat(self.triggerer_id)
        except _TRANSIENT_API_ERRORS as e:
            if not _is_transient(e):
                raise
            # A missed heartbeat is not fatal; the next loop retries. Crashing the loop on a
            # transient blip would be worse (it'd tear down all running triggers).
            log.warning("Failed to heartbeat Job via Execution API; will retry next cycle", exc_info=True)
            return
        self._last_heartbeat = now
        stats.incr("triggerer_heartbeat", 1, 1)

    def load_triggers(self) -> None:
        """Claim triggers over the API and reconcile the runner's running set."""
        try:
            ids = self.client.triggers.load(
                triggerer_id=self.triggerer_id,
                capacity=self.capacity,
                health_check_threshold=self.health_check_threshold,
                queues=list(self.queues) if self.queues else None,
                team_name=self.team_name,
            )
        except _TRANSIENT_API_ERRORS as e:
            if not _is_transient(e):
                raise
            log.warning("Failed to load triggers from Execution API; skipping this cycle", exc_info=True)
            return
        # Reuse the base diff logic (works out adds/cancels against what we already run).
        self.update_triggers(set(ids))

    def handle_events(self) -> None:
        """
        Submit fired events over the API, re-queuing on a transient blip.

        Popping the event then submitting it means a raised client call would lose the popped
        event if the loop crashed. Here a transient error re-queues the event (preserving FIFO
        order) and stops the loop so it retries next cycle.
        """
        while self.events:
            trigger_id, event = self.events.popleft()
            try:
                self.on_trigger_event(trigger_id, event)
            except _TRANSIENT_API_ERRORS as e:
                if not _is_transient(e):
                    raise
                log.warning(
                    "Failed to submit trigger event via Execution API; will retry next cycle",
                    exc_info=True,
                )
                self.events.appendleft((trigger_id, event))
                break
            stats.incr("triggers.succeeded")

    def on_trigger_event(self, trigger_id: int, event: TriggerEvent) -> None:
        """Report a fired event over the API (the run loop guards against transient errors)."""
        # The payload may be a non-JSON-native object (e.g. a pendulum DateTime from a temporal
        # trigger). Serialize it with serde so it survives the HTTP hop. The server splices this
        # serde form straight into the task instance's next_kwargs WITHOUT deserializing it -- the
        # worker deserializes it on resume. This keeps untrusted worker payloads from being
        # reconstructed inside the trusted api-server.
        self.client.triggers.submit_event(trigger_id, serde_serialize(event.payload))

    def clean_unused(self) -> None:
        """Ask the server to delete orphaned triggers; tolerate a transient API blip."""
        try:
            self.client.triggers.cleanup()
        except _TRANSIENT_API_ERRORS as e:
            if not _is_transient(e):
                raise
            log.warning("Failed to clean up triggers via Execution API; skipping this cycle", exc_info=True)

    def handle_failed_triggers(self) -> None:
        """Submit trigger failures over the API, re-queuing on a transient blip (see ``handle_events``)."""
        while self.failed_triggers:
            trigger_id, exc = self.failed_triggers.popleft()
            try:
                self.on_trigger_failure(trigger_id, exc)
            except _TRANSIENT_API_ERRORS as e:
                if not _is_transient(e):
                    raise
                log.warning(
                    "Failed to submit trigger failure via Execution API; will retry next cycle",
                    exc_info=True,
                )
                self.failed_triggers.appendleft((trigger_id, exc))
                break
            stats.incr("triggers.failed")

    def on_trigger_failure(self, trigger_id: int, exc: list[str] | None) -> None:
        """Report a trigger failure over the API (the run loop guards against transient errors)."""
        self.client.triggers.submit_failure(trigger_id, exc)

    def metric_tags(self) -> dict[str, str]:
        """Supply the ``hostname`` tag from the host (the supervisor has no metadata-DB Job row)."""
        return {"hostname": get_hostname()}

    def emit_metrics(self):
        tags = self.metric_tags()
        stats.gauge(
            "triggers.running",
            len(self.running_triggers),
            tags=tags,
        )

        capacity_left = self.capacity - len(self.running_triggers)
        stats.gauge(
            "triggerer.capacity_left",
            capacity_left,
            tags=tags,
        )

    def _trigger_log_id(self) -> int:
        """
        Return the id embedded in the on-disk trigger log filename.

        The reader side (``FileTaskHandler.add_triggerer_suffix``) formats the suffix from
        ``ti.triggerer_job.id`` (an int). Use the registered Job id so the writer's filename
        matches the reader's expectation.
        """
        return self.triggerer_id

    def _register_trigger_logger(self, trigger_id: int, ser_ti: TaskInstanceDTO, log_path: str) -> None:
        # When producing logs from TIs, include the supervisor id producing the logs to disambiguate it.
        log_id = self._trigger_log_id()
        self.logger_cache[trigger_id] = TriggerLoggingFactory(
            log_path=f"{log_path}.trigger.{log_id}.log",
            ti=ser_ti,  # type: ignore
        )

    def _render_log_path(self, ti: TaskInstanceDTO) -> str:
        if self._log_fname_renderer is None:
            self._log_fname_renderer = log_filename_template_renderer()
        return self._log_fname_renderer(ti=ti)

    def build_trigger_workloads(self, new_trigger_ids: set[int]) -> list[workloads.RunTrigger]:
        """
        Fetch workloads for newly-claimed triggers over the API and register their loggers.

        On a transient API blip we return an empty list: the triggers stay assigned to us and get
        re-fetched on the next load cycle, so nothing is lost.
        """
        try:
            raw = self.client.triggers.workloads(list(new_trigger_ids))
        except _TRANSIENT_API_ERRORS as e:
            if not _is_transient(e):
                raise
            log.warning(
                "Failed to fetch trigger workloads from Execution API; will retry next cycle",
                exc_info=True,
            )
            return []

        # The client returns the workloads as raw dicts (it must not import the core RunTrigger
        # type); validate them into RunTrigger objects here in airflow-core.
        built = [workloads.RunTrigger.model_validate(w) for w in raw]

        # Register a per-trigger logging factory for each task-backed workload so its logs are
        # captured and uploaded on finish. The workloads come over HTTP, so we re-derive the log
        # path here from the (DB-free) filename renderer and the workload's TaskInstanceDTO.
        for workload in built:
            if workload.ti is not None:
                self._register_trigger_logger(workload.id, workload.ti, self._render_log_path(workload.ti))
        return built

    def update_triggers(self, requested_trigger_ids: set[int]):
        """
        Request that we update what triggers we're running.

        Works out the differences - ones to add, and ones to remove - then
        adds them to the dequeues so the subprocess can actually mutate the running
        trigger set.
        """
        known_trigger_ids = self.running_triggers.union(
            (x[0] for x in self.events),
            self.cancelling_triggers,
            (trigger[0] for trigger in self.failed_triggers),
            (trigger.id for trigger in self.creating_triggers),
        )
        # Work out the two difference sets
        new_trigger_ids = requested_trigger_ids - known_trigger_ids
        cancel_trigger_ids = self.running_triggers - requested_trigger_ids
        if new_trigger_ids:
            self.creating_triggers.extend(self.build_trigger_workloads(new_trigger_ids))

        if cancel_trigger_ids:
            # Enqueue orphaned triggers for cancellation
            self.cancelling_triggers.update(cancel_trigger_ids)

    def _register_pipe_readers(
        self,
        stdout: socket,
        stderr: socket,
        requests: socket,
        logs: socket,
        *,
        data: dict[socket, bytes],
    ):
        super()._register_pipe_readers(stdout, stderr, requests, logs, data=data)

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

    _pending: dict[int, asyncio.Future] = attrs.field(factory=dict, repr=False)
    _loop: asyncio.AbstractEventLoop | None = attrs.field(default=None, repr=False)
    _loop_thread_id: int | None = attrs.field(default=None, repr=False)
    _reader_task: asyncio.Task | None = attrs.field(default=None, repr=False)

    async def _aread_frame(self):
        try:
            len_bytes = await self._async_reader.readexactly(4)
        except ConnectionResetError:
            asyncio.current_task().cancel("Supervisor closed")
            raise
        length = int.from_bytes(len_bytes, byteorder="big")
        if length >= 2**32:
            raise OverflowError(f"Refusing to receive messages larger than 4GiB {length=}")
        buffer = await self._async_reader.readexactly(length)
        return self.resp_decoder.decode(buffer)

    async def _reader_loop(self) -> None:
        try:
            while True:
                frame = await self._aread_frame()
                future = self._pending.pop(frame.id, None)
                if future is not None and not future.done():
                    future.set_result(frame)
                else:
                    self.log.warning("Got response for unknown request frame", frame_id=frame.id)
        finally:
            for fut in self._pending.values():
                if not fut.done():
                    fut.cancel("Reader loop exited")
            self._pending.clear()

    async def start_reader(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._loop_thread_id = threading.get_ident()
        self._reader_task = asyncio.create_task(self._reader_loop(), name="trigger-comms-reader")

    def send(self, msg: ToTriggerSupervisor) -> ToTriggerRunner | None:
        if self._loop is None:
            raise RuntimeError("start_reader() must be called before send()")
        if threading.get_ident() == self._loop_thread_id:
            # Called from the event loop thread itself (e.g. a trigger calling a sync SDK method
            # directly from async def run()). run_coroutine_threadsafe(...).result() would deadlock
            # here because .result() blocks the thread the event loop runs on.
            # greenback.await_() teleports the coroutine back into the running loop instead.
            if not greenback.has_portal():
                raise RuntimeError(
                    "Sync SDK methods (e.g. get_connection(), get_variable()) cannot be called "
                    "from a trigger's async def run() when AIRFLOW_DISABLE_GREENBACK_PORTAL is "
                    "set. Either remove that environment variable, or use the async equivalent "
                    "(e.g. aget_connection(), aget_variable())."
                )
            return greenback.await_(self.asend(msg))
        return asyncio.run_coroutine_threadsafe(self.asend(msg), self._loop).result()

    async def asend(self, msg: ToTriggerSupervisor) -> ToTriggerRunner | None:
        if self._loop is None:
            raise RuntimeError("start_reader() must be called before asend()")
        current_loop = asyncio.get_running_loop()
        if self._loop is not None and current_loop is not self._loop:
            # Called from a foreign event loop (e.g. via async_to_sync). Bridge to the main loop
            # so _reader_loop can resolve the future, then await via wrap_future which is
            # non-blocking for the foreign loop.
            cf = asyncio.run_coroutine_threadsafe(self.asend(msg), self._loop)
            return await asyncio.wrap_future(cf)
        frame = _RequestFrame(id=next(self.id_counter), body=msg.model_dump())
        future: asyncio.Future = current_loop.create_future()
        self._pending[frame.id] = future
        try:
            self._async_writer.write(frame.as_bytes())
            return self._from_frame(await future)
        except BaseException:
            self._pending.pop(frame.id, None)
            raise


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
    stop: bool = False
    _stop_event: anyio.Event | None = None

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
        self._stop_event = None
        self._shared_streams = SharedStreamManager(
            log=self.log,
            max_subscriber_queue=conf.getint("triggerer", "shared_stream_subscriber_queue_size"),
        )
        self.blocked_main_thread_warning_threshold = conf.getfloat(
            "triggerer", "blocked_main_thread_warning_threshold"
        )

    def _handle_signal(self, signum, frame) -> None:
        """Handle termination signals gracefully."""
        self.stop = True
        if self._stop_event is not None:
            self._stop_event.set()

    def run(self):
        """Sync entrypoint - just run arun in an async loop."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        asyncio.run(self.arun())

    async def arun(self):
        """
        Run trigger addition/deletion/cleanup; main (asynchronous) logic loop.

        Actual triggers run in their own separate coroutines.
        """
        # Make sure comms are initialized before allowing any Triggers to run
        await self.init_comms()

        watchdog = asyncio.create_task(self.block_watchdog())
        stop_event = self._stop_event = anyio.Event()

        last_status = time.monotonic()
        try:
            while not self.stop:
                # Raise exceptions from the tasks
                if watchdog.done():
                    watchdog.result()

                if self.comms_decoder._reader_task.done():
                    self.comms_decoder._reader_task.result()
                    raise RuntimeError("Supervisor connection lost")

                # Run core logic

                finished_ids = await self.cleanup_finished_triggers()
                # This also loads the triggers we need to create or cancel
                await self.sync_state_to_supervisor(finished_ids)
                await self.create_triggers()
                await self.cancel_triggers()
                # Sleep for a bit, or exit early if stop is requested.
                with anyio.move_on_after(1):
                    await stop_event.wait()
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
        finally:
            if (reader_task := self.comms_decoder._reader_task) is not None:
                reader_task.cancel()
                with suppress(asyncio.CancelledError):
                    await reader_task
            # Safety net: cancel any shared-stream poll tasks whose group
            # survived per-trigger cleanup. The normal eviction path is
            # ``SharedStreamManager.unsubscribe`` in ``run_trigger``'s
            # finally; this call only matters when that path was bypassed
            # (e.g. the unsubscribe coroutine raised and was swallowed).
            await self._shared_streams.stop_all()
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

        frame = await self.comms_decoder._aread_frame()
        msg = self.comms_decoder._from_frame(frame)
        if not isinstance(msg, messages.StartTriggerer):
            raise RuntimeError(f"Required first message to be a messages.StartTriggerer, it was {msg}")

        await self.comms_decoder.start_reader()

    @classmethod
    def create_runtime_ti(
        cls, task_instance: TaskInstanceDTO, encoded_dag: dict, dag_run_data: dict
    ) -> RuntimeTaskInstance:
        from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun as DRDataModel
        from airflow.sdk.api.datamodels._generated import TIRunContext

        task = DagSerialization.from_dict(encoded_dag).get_task(task_instance.task_id)

        # I need to recreate a TaskInstance from task_runner before invoking get_template_context (airflow.executors.workloads.TaskInstance)
        return RuntimeTaskInstance.model_construct(
            **task_instance.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=TIRunContext.model_construct(
                dag_run=DRDataModel(**dag_run_data),
                max_tries=task.retries,
            ),
        )

    async def create_triggers(self):
        """Drain the to_create queue and create all new triggers that have been requested in the DB."""
        while self.to_create:
            await asyncio.sleep(0)
            context: Context | None = None
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

                if ti := workload.ti:
                    trigger_name = f"{ti.dag_id}/{ti.run_id}/{ti.task_id}/{ti.map_index}/{ti.try_number} (ID {trigger_id})"
                    trigger_instance = trigger_class(**deserialised_kwargs)

                    if workload.dag_data:
                        runtime_ti = self.create_runtime_ti(ti, workload.dag_data, workload.dag_run_data)
                        context = runtime_ti.get_template_context()
                        trigger_instance.task_instance = runtime_ti
                    else:
                        trigger_instance.task_instance = ti
                else:
                    trigger_name = f"ID {trigger_id}"
                    trigger_instance = trigger_class(**deserialised_kwargs)
            except TypeError as err:
                self.log.error("Trigger failed to inflate", error=err)
                self.failed_triggers.append((trigger_id, err))
                continue
            trigger_instance.trigger_id = trigger_id
            trigger_instance.triggerer_job_id = self.job_id
            trigger_instance.timeout_after = workload.timeout_after

            self.triggers[trigger_id] = {
                "task": asyncio.create_task(
                    self.run_trigger(trigger_id, trigger_instance, workload.timeout_after, context),
                    name=trigger_name,
                ),
                "is_watcher": isinstance(trigger_instance, BaseEventTrigger),
                "name": trigger_name,
                "events": 0,
            }

    async def cancel_triggers(self):
        """
        Drain the to_cancel queue and ensure all triggers that are not in the DB are cancelled.

        This allows the cleanup job to delete them.
        Passes "user-action" as the cancel message so that run_trigger() knows to invoke
        on_kill(). Triggers in this queue are always removed because this is the path in which
        the user performed some action on the task. Trigger redistribution goes through a separate
        path.
        """
        while self.to_cancel:
            trigger_id = self.to_cancel.popleft()
            if trigger_id in self.triggers:
                self.triggers[trigger_id]["task"].cancel(_USER_ACTION_CANCEL_MSG)
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

    def process_trigger_events(self, finished_ids: list[int]) -> messages.TriggerStateChanges:
        # Copy out of our dequeues in threadsafe manner to sync state with parent
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

        # Tell the monitor that we've finished triggers so it can update things
        try:
            resp = await self.asend(msg)
        except NotImplementedError:
            # A non-serializable trigger event was detected, remove it and fail associated trigger
            resp = await self.asend(self.sanitize_trigger_events(msg))

        if resp:
            self.to_create.extend(resp.to_create)
            self.to_cancel.extend(resp.to_cancel)

    async def asend(self, msg: messages.TriggerStateChanges) -> messages.TriggerStateSync | None:
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
            if time_elapsed > self.blocked_main_thread_warning_threshold:
                await self.log.ainfo(
                    "Triggerer's async thread was blocked for %.2f seconds, "
                    "exceeding the configured warning threshold of %.2f seconds. "
                    "This is likely caused by a badly-written trigger. "
                    "Set PYTHONASYNCIODEBUG=1 to get more information on overrunning coroutines.",
                    time_elapsed,
                    self.blocked_main_thread_warning_threshold,
                )
                stats.incr("triggers.blocked_main_thread")

    async def run_trigger(
        self,
        trigger_id: int,
        trigger: BaseTrigger,
        timeout_after: datetime | None = None,
        context: Context | None = None,
    ):
        """Run a trigger (they are async generators) and push their events into our outbound event deque."""
        if not os.environ.get("AIRFLOW_DISABLE_GREENBACK_PORTAL", "").lower() == "true":
            await greenback.ensure_portal()

        ti = trigger.task_instance
        bind_log_contextvars(
            trigger_id=trigger_id,
            ti_id=str(ti.id) if ti else None,
            dag_id=ti.dag_id if ti else None,
            task_id=ti.task_id if ti else None,
            run_id=ti.run_id if ti else None,
            try_number=ti.try_number if ti else None,
            map_index=ti.map_index if ti else None,
        )

        name = self.triggers[trigger_id]["name"]
        self.log.info("trigger %s starting", name)

        # Triggers that opt into a shared underlying I/O stream
        # (BaseEventTrigger.shared_stream_key returns non-None) consume a
        # broadcast stream produced by SharedStreamManager and convert it
        # via filter_shared_stream(). Everything else stays on the original
        # standalone-run() path. The key is computed after
        # render_template_fields so any templated attributes are already
        # resolved when the key is constructed.
        event_trigger: BaseEventTrigger | None = None
        if isinstance(trigger, BaseEventTrigger):
            event_trigger = trigger
        shared_key: Hashable | None = None

        with _make_trigger_span(ti=trigger.task_instance, trigger_id=trigger_id, name=name) as span:
            try:
                if context is not None:
                    trigger.render_template_fields(context=context)

                if event_trigger is not None:
                    try:
                        shared_key = event_trigger.shared_stream_key()
                    except Exception:
                        self.log.exception(
                            "shared_stream_key() raised; falling back to standalone run",
                            trigger_id=trigger_id,
                        )
                        shared_key = None

                if shared_key is not None and event_trigger is not None:
                    shared_stream = self._shared_streams.subscribe(
                        trigger_id=trigger_id, trigger=event_trigger, key=shared_key
                    )
                    event_stream = event_trigger.filter_shared_stream(shared_stream)
                else:
                    event_stream = trigger.run()

                async for event in event_stream:
                    await self.log.ainfo(
                        "Trigger fired event", name=self.triggers[trigger_id]["name"], result=event
                    )
                    self.triggers[trigger_id]["events"] += 1
                    self.events.append((trigger_id, event))
                span.set_status(Status(StatusCode.OK))
            except asyncio.CancelledError as e:
                # A trigger can be cancelled for two reasons:
                #   - The user acted on the task (mark failed / clear / mark succeeded).
                #   - The triggerer is shutting down, here cancel_triggers() is not
                #     involved — the shutdown path cancels tasks directly without a message.
                # Only first case should invoke on_kill().
                #
                # For timeout, raise immediately without calling on_kill().
                if timeout := timeout_after:
                    timeout = timeout.replace(tzinfo=timezone.utc) if not timeout.tzinfo else timeout
                    if timeout < timezone.utcnow():
                        await self.log.aerror("Trigger cancelled due to timeout")
                        span.set_status(Status(StatusCode.ERROR), description=str(e))
                        raise
                if e.args and e.args[0] == _USER_ACTION_CANCEL_MSG:
                    await self.log.ainfo("Trigger cancelled by user action, invoking on_kill", name=name)
                    try:
                        await asyncio.wait_for(trigger.on_kill(), timeout=_ON_CANCEL_TIMEOUT)
                    except TimeoutError:
                        await self.log.awarning("on_kill() timed out", timeout=_ON_CANCEL_TIMEOUT, name=name)
                    except asyncio.CancelledError:
                        # on_kill() was itself cancelled (e.g. triggerer shutting down
                        # while on_kill() was running). Swallow it so the outer
                        # CancelledError (variable `e`) can be re-raised below.
                        pass
                    except Exception:
                        await self.log.awarning("on_kill() raised an exception", name=name, exc_info=True)
                span.set_status(Status(StatusCode.OK), description=str(e))
                raise
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR), description=str(e))
                raise
            finally:
                # CancelledError will get injected when we're stopped - which is
                # fine, the cleanup process will understand that, but we want to
                # allow triggers a chance to cleanup, either in that case or if
                # they exit cleanly. Exception from cleanup methods are ignored.
                if shared_key is not None:
                    try:
                        await self._shared_streams.unsubscribe(trigger_id, shared_key)
                    except Exception:
                        # Best-effort cleanup, but log so we don't lose
                        # cancel-propagation or _handle_poll_terminate bugs.
                        self.log.exception(
                            "Failed to unsubscribe trigger from shared stream",
                            trigger_id=trigger_id,
                            key=shared_key,
                        )
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
