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
from traceback import format_exception
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, TypedDict, Union

import attrs
import structlog
from pydantic import BaseModel, Field, TypeAdapter
from sqlalchemy import func, select
from structlog.contextvars import bind_contextvars as bind_log_contextvars

from airflow.configuration import conf
from airflow.executors import workloads
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import perform_heartbeat
from airflow.models.trigger import Trigger
from airflow.sdk.execution_time.supervisor import WatchedSubprocess, make_buffered_socket_reader
from airflow.stats import Stats
from airflow.traces.tracer import Trace, add_span
from airflow.triggers import base as events
from airflow.utils import timezone
from airflow.utils.helpers import log_filename_template_renderer
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import provide_session

if TYPE_CHECKING:
    from socket import socket

    from sqlalchemy.orm import Session
    from structlog.typing import FilteringBoundLogger, WrappedLogger

    from airflow.jobs.job import Job
    from airflow.triggers.base import BaseTrigger

HANDLER_SUPPORTS_TRIGGERER = False
"""
If this value is true, root handler is configured to log individual trigger messages
visible in task logs.

:meta private:
"""

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
    ):
        super().__init__(job)
        if capacity is None:
            self.capacity = conf.getint("triggerer", "default_capacity", fallback=1000)
        elif isinstance(capacity, int) and capacity > 0:
            self.capacity = capacity
        else:
            raise ValueError(f"Capacity number {capacity!r} is invalid")

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
                job=self.job, capacity=self.capacity, logger=log
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

        requests_fd: int
        kind: Literal["StartTriggerer"] = "StartTriggerer"

    class CancelTriggers(BaseModel):
        """Request to cancel running triggers."""

        ids: Iterable[int]
        kind: Literal["CancelTriggersMessage"] = "CancelTriggersMessage"

    class TriggerStateChanges(BaseModel):
        """Report state change about triggers back to the TriggerRunnerSupervisor."""

        kind: Literal["TriggerStateChanges"] = "TriggerStateChanges"
        events: Annotated[
            list[tuple[int, events.DiscrimatedTriggerEvent]] | None,
            # We have to specify a default here, as otherwise Pydantic struggles to deal with the discriminated
            # union :shrug:
            Field(default=None),
        ]
        # Format of list[str] is the exc traceback format
        failures: list[tuple[int, list[str] | None]] | None = None
        finished: list[int] | None = None


ToTriggerRunner = Annotated[
    Union[workloads.RunTrigger, messages.CancelTriggers, messages.StartTriggerer],
    Field(discriminator="kind"),
]
"""
The types of messages we can send in to the Trigger Runner process (the process that runs the actual async
code).
"""


ToTriggerSupervisor = Annotated[
    Union[messages.TriggerStateChanges],
    Field(discriminator="kind"),
]
"""
The types of messages that the async Trigger Runner can send back up to the supervisor process.
"""


@attrs.define(kw_only=True)
class TriggerLoggingFactory:
    log_path: str

    bound_logger: WrappedLogger = attrs.field(init=False)

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


@attrs.define(kw_only=True)
class TriggerRunnerSupervisor(WatchedSubprocess):
    """
    TriggerRunnerSupervisor is responsible for monitoring the subprocess and marshalling DB access.

    This class (which runs in the main process) is responsible for querying the DB, sending RunTrigger
    workload messages to the subprocess, and collecting results and updating them in the DB.
    """

    job: Job
    capacity: int

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

    # Outbound queue of events
    events: deque[tuple[int, events.TriggerEvent]] = attrs.field(factory=deque, init=False)

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

        msg = messages.StartTriggerer(requests_fd=proc._requests_fd)
        proc._send(msg)
        return proc

    def _handle_request(self, msg: ToTriggerSupervisor, log: FilteringBoundLogger) -> None:  # type: ignore[override]
        if isinstance(msg, messages.TriggerStateChanges):
            log.debug("State change from async process", state=msg)
            if msg.events:
                self.events.extend(msg.events)
            if msg.failures:
                self.failed_triggers.extend(msg.failures)
            for id in msg.finished or ():
                self.running_triggers.discard(id)
                self.cancelling_triggers.discard(id)
                # Remove logger from the cache, and since structlog doesn't have an explicit close method, we
                # only need to remove the last reference to it to close the open FH
                self.logger_cache.pop(id, None)
            return

        raise ValueError(f"Unknown message type {type(msg)}")

    def run(self) -> None:
        """Run synchronously and handle all database reads/writes."""
        while not self.stop:
            if not self.is_alive():
                log.error("Trigger runner process has died! Exiting.")
                break
            with Trace.start_span(span_name="triggerer_job_loop", component="TriggererJobRunner"):
                # Wait for up to 1 second for activity
                self._service_subprocess(1)

                self.load_triggers()
                self.handle_events()
                self.handle_failed_triggers()
                self.clean_unused()
                self.heartbeat()

                self.emit_metrics()

    def heartbeat(self):
        perform_heartbeat(self.job, heartbeat_callback=self.heartbeat_callback, only_if_necessary=True)

    def heartbeat_callback(self, session: Session | None = None) -> None:
        Stats.incr("triggerer_heartbeat", 1, 1)

    @add_span
    def load_triggers(self):
        """Query the database for the triggers we're supposed to be running and update the runner."""
        Trigger.assign_unassigned(self.job.id, self.capacity, self.health_check_threshold)
        ids = Trigger.ids_for_triggerer(self.job.id)
        self.update_triggers(set(ids))

    @add_span
    def handle_events(self):
        """Dispatch outbound events to the Trigger model which pushes them to the relevant task instances."""
        while self.events:
            # Get the event and its trigger ID
            trigger_id, event = self.events.popleft()
            # Tell the model to wake up its tasks
            Trigger.submit_event(trigger_id=trigger_id, event=event)
            # Emit stat event
            Stats.incr("triggers.succeeded")

    @add_span
    def clean_unused(self):
        """Clean out unused or finished triggers."""
        Trigger.clean_unused()

    @add_span
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

    def _send(self, msg: BaseModel):
        self.stdin.write(msg.model_dump_json().encode("utf-8") + b"\n")

    def update_triggers(self, requested_trigger_ids: set[int]):
        """
        Request that we update what triggers we're running.

        Works out the differences - ones to add, and ones to remove - then
        adds them to the deques so the subprocess can actually mutate the running
        trigger set.
        """
        render_log_fname = log_filename_template_renderer()

        known_trigger_ids = (
            self.running_triggers.union(x[0] for x in self.events)
            .union(self.cancelling_triggers)
            .union(trigger[0] for trigger in self.failed_triggers)
        )
        # Work out the two difference sets
        new_trigger_ids = requested_trigger_ids - known_trigger_ids
        cancel_trigger_ids = self.running_triggers - requested_trigger_ids
        # Bulk-fetch new trigger records
        new_triggers = Trigger.bulk_fetch(new_trigger_ids)
        triggers_with_assets = Trigger.fetch_trigger_ids_with_asset()
        to_create: list[workloads.RunTrigger] = []
        # Add in new triggers
        for new_id in new_trigger_ids:
            # Check it didn't vanish in the meantime
            if new_id not in new_triggers:
                log.warning("Trigger disappeared before we could start it", id=new_id)
                continue

            new_trigger_orm = new_triggers[new_id]

            # If the trigger is not associated to a task or an asset, this means the TaskInstance
            # row was updated by either Trigger.submit_event or Trigger.submit_failure
            # and can happen when a single trigger Job is being run on multiple TriggerRunners
            # in a High-Availability setup.
            if new_trigger_orm.task_instance is None and new_id not in triggers_with_assets:
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
                # When producing logs from TIs, include the job id producing the logs to disambiguate it.
                self.logger_cache[new_id] = TriggerLoggingFactory(
                    log_path=f"{log_path}.trigger.{self.job.id}.log"
                )

                ser_ti = workloads.TaskInstance.model_validate(
                    new_trigger_orm.task_instance, from_attributes=True
                )
                workload.ti = ser_ti
                workload.timeout_after = new_trigger_orm.task_instance.trigger_timeout

            to_create.append(workload)

        for workload in to_create:
            self._send(workload)
        self.running_triggers.update(m.id for m in to_create)

        if cancel_trigger_ids:
            # Enqueue orphaned triggers for cancellation
            self.cancelling_triggers.update(cancel_trigger_ids)
            self._send(messages.CancelTriggers(ids=cancel_trigger_ids))

    def _register_pipe_readers(
        self, logger: FilteringBoundLogger, stdout: socket, stderr: socket, requests: socket, logs: socket
    ):
        super()._register_pipe_readers(logger, stdout, stderr, requests, logs)

        # We want to handle logging differently here, so un-register the one our parent class created
        self.selector.unregister(logs)

        self.selector.register(
            logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                self._process_log_messages_from_subprocess(), on_close=self._on_socket_closed
            ),
        )

    def _process_log_messages_from_subprocess(self) -> Generator[None, bytes, None]:
        import msgspec
        from structlog.stdlib import NAME_TO_LEVEL

        fallback_log = structlog.get_logger(logger_name=__name__)

        from airflow.sdk.log import logging_processors

        processors, _ = logging_processors(enable_pretty_log=False)

        def get_logger(trigger_id: int) -> WrappedLogger:
            # TODO: Is a separate dict worth it, or should we make `self.running_triggers` a dict?
            return self.logger_cache[trigger_id](processors)

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

            if ts := event.get("timestamp"):
                # We use msgspec to decode the timestamp as it does it orders of magnitude quicker than
                # datetime.strptime
                #
                # We remove the timezone info here, as the json encoding has `+00:00`, and since the log came
                # from a subprocess we know that the timezone of the log message is the same, so having some
                # messages include tz (from subprocess) but others not (ones from supervisor process) is
                # confusing.
                event["timestamp"] = msgspec.json.decode(f'"{ts}"', type=datetime).replace(tzinfo=None)

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
    name: str
    events: int


class TriggerRunner:
    """
    Runtime environment for all triggers.

    Mainly runs inside its own thread, where it hands control off to an asyncio
    event loop, but is also sometimes interacted with from the main thread
    (where all the DB queries are done). All communication between threads is
    done via Deques.
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
    events: deque[tuple[int, events.TriggerEvent]]

    # Outbound queue of failed triggers
    failed_triggers: deque[tuple[int, BaseException | None]]

    # Should-we-stop flag
    # TODO: set this in a sig-int handler
    stop: bool = False

    # TODO: connect this to the parent process
    log: FilteringBoundLogger = structlog.get_logger()

    requests_sock: asyncio.StreamWriter

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
        watchdog = asyncio.create_task(self.block_watchdog())
        ready_event = asyncio.Event()
        read_workloads = asyncio.create_task(self.read_workloads(ready_event))

        await ready_event.wait()
        last_status = time.monotonic()
        try:
            while not self.stop:
                # Raise exceptions from the tasks
                if read_workloads.done():
                    read_workloads.result()
                if watchdog.done():
                    watchdog.result()

                # Run core logic
                await self.create_triggers()
                await self.cancel_triggers()
                finished_ids = await self.cleanup_finished_triggers()
                await self.sync_state_to_supervisor(finished_ids)
                # Sleep for a bit
                await asyncio.sleep(1)
                # Every minute, log status
                if (now := time.monotonic()) - last_status >= 60:
                    count = len(self.triggers)
                    self.log.info("%i triggers currently running", count)
                    last_status = now

        except Exception:
            log.exception("Trigger runner failed")
            self.stop = True
            raise
        read_workloads.cancel()
        # Wait for supporting tasks to complete
        await watchdog
        await read_workloads

    async def read_workloads(self, ready_event: asyncio.Event):
        """
        Read the triggers to run on stdin.

        This reads-and-decodes the JSON lines send by the TriggerRunnerSupervisor to us on our stdint
        """
        loop = asyncio.get_event_loop()

        task = asyncio.current_task(loop=loop)
        if TYPE_CHECKING:
            assert task
        # Set the event on done callback so that this FN fails the arun wakes up and we catch the exception
        task.add_done_callback(lambda _: ready_event.set())

        async def connect_stdin() -> asyncio.StreamReader:
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            await loop.connect_read_pipe(lambda: protocol, sys.stdin)
            return reader

        stdin = await connect_stdin()

        # The first message must be this type, else we can't operate
        line = await stdin.readline()

        decoder = TypeAdapter[ToTriggerRunner](ToTriggerRunner)
        msg = decoder.validate_json(line)
        if not isinstance(msg, messages.StartTriggerer) or msg.requests_fd <= 0:
            raise RuntimeError(f"First message to triggerer must be {messages.StartTriggerer.__name__}")

        writer_transport, writer_protocol = await loop.connect_write_pipe(
            lambda: asyncio.streams.FlowControlMixin(loop=loop),
            os.fdopen(msg.requests_fd, "wb"),
        )
        self.requests_sock = asyncio.streams.StreamWriter(writer_transport, writer_protocol, None, loop)

        # Tell `arun` it can start the main loop now
        ready_event.set()

        async for line in stdin:
            msg = decoder.validate_json(line)

            if isinstance(msg, workloads.RunTrigger):
                self.to_create.append(msg)
            elif isinstance(msg, messages.CancelTriggers):
                self.to_cancel.extend(msg.ids)
            else:
                raise ValueError(f"Unknown workload type {type(msg)}")

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
                kwargs = Trigger._decrypt_kwargs(workload.encrypted_kwargs)
                trigger_instance = trigger_class(**kwargs)
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
                    self.run_trigger(trigger_id, trigger_instance), name=trigger_name
                ),
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
                    if isinstance(result, events.TriggerEvent):
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
        # Copy out of our deques in threadsafe manner to sync state with parent
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

        # Only send a message if there is anything to say
        if not events_to_send:
            msg.events = None

        if not failures_to_send:
            msg.failures = None

        if not finished_ids:
            msg.finished = None

        if msg.events or msg.finished or msg.failures:
            # Tell the monitor that we've finished triggers so it can update things
            self.requests_sock.write(msg.model_dump_json(exclude_none=True).encode() + b"\n")

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

    async def run_trigger(self, trigger_id, trigger):
        """Run a trigger (they are async generators) and push their events into our outbound event deque."""
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
            if timeout := trigger.timeout_after:
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
