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
import signal
import sys
import threading
import time
import warnings
from collections import deque
from contextlib import suppress
from copy import copy
from queue import SimpleQueue
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from airflow.configuration import conf
from airflow.jobs.base_job_runner import BaseJobRunner
from airflow.jobs.job import perform_heartbeat
from airflow.models.trigger import Trigger
from airflow.stats import Stats
from airflow.traces.tracer import Trace, add_span
from airflow.triggers.base import TriggerEvent
from airflow.typing_compat import TypedDict
from airflow.utils import timezone
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.trigger_handler import (
    DropTriggerLogsFilter,
    LocalQueueHandler,
    TriggererHandlerWrapper,
    TriggerMetadataFilter,
    ctx_indiv_trigger,
    ctx_task_instance,
    ctx_trigger_end,
    ctx_trigger_id,
)
from airflow.utils.module_loading import import_string
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.jobs.job import Job
    from airflow.models import TaskInstance
    from airflow.triggers.base import BaseTrigger

HANDLER_SUPPORTS_TRIGGERER = False
"""
If this value is true, root handler is configured to log individual trigger messages
visible in task logs.

:meta private:
"""

SEND_TRIGGER_END_MARKER = True
"""
If handler natively supports triggers, may want to disable sending trigger end marker.

:meta private:
"""

logger = logging.getLogger(__name__)


DISABLE_WRAPPER = conf.getboolean("logging", "disable_trigger_handler_wrapper", fallback=False)
DISABLE_LISTENER = conf.getboolean("logging", "disable_trigger_handler_queue_listener", fallback=False)


def configure_trigger_log_handler():
    """
    Configure logging where each trigger logs to its own file and can be exposed via the airflow webserver.

    Generally speaking, we take the log handler configured for logger ``airflow.task``,
    wrap it with TriggerHandlerWrapper, and set it as the handler for root logger.

    If there already is a handler configured for the root logger and it supports triggers, we wrap it instead.

    :meta private:
    """
    global HANDLER_SUPPORTS_TRIGGERER

    def should_wrap(handler):
        return handler.__dict__.get("trigger_should_wrap", False) or handler.__class__.__dict__.get(
            "trigger_should_wrap", False
        )

    def should_queue(handler):
        return handler.__dict__.get("trigger_should_queue", True) or handler.__class__.__dict__.get(
            "trigger_should_queue", True
        )

    def send_trigger_end_marker(handler):
        val = handler.__dict__.get("trigger_send_end_marker", None)
        if val is not None:
            return val

        val = handler.__class__.__dict__.get("trigger_send_end_marker", None)
        if val is not None:
            return val
        return True

    def supports_triggerer(handler):
        return (
            should_wrap(handler)
            or handler.__dict__.get("trigger_supported", False)
            or handler.__class__.__dict__.get("trigger_supported", False)
        )

    def get_task_handler_from_logger(logger_):
        for h in logger_.handlers:
            if isinstance(h, FileTaskHandler) and not supports_triggerer(h):
                warnings.warn(
                    f"Handler {h.__class__.__name__} does not support "
                    "individual trigger logging. Please check the release notes "
                    "for your provider to see if a newer version supports "
                    "individual trigger logging.",
                    category=UserWarning,
                    stacklevel=3,
                )
            if supports_triggerer(h):
                return h

    def find_suitable_task_handler():
        # check root logger then check airflow.task to see if a handler
        # suitable for use with TriggerHandlerWrapper (has trigger_should_wrap
        # attr, likely inherits from FileTaskHandler)
        h = get_task_handler_from_logger(root_logger)
        if not h:
            # try to use handler configured from airflow task
            logger.debug("No task logger configured for root logger; trying `airflow.task`.")
            h = get_task_handler_from_logger(logging.getLogger("airflow.task"))
            if h:
                logger.debug("Using logging configuration from `airflow.task`")
        if not h:
            warnings.warn(
                "Could not find log handler suitable for individual trigger logging.",
                category=UserWarning,
                stacklevel=3,
            )
            return None
        return h

    def filter_trigger_logs_from_other_root_handlers(new_hdlr):
        # we add context vars to log records emitted for individual triggerer logging
        # we want these records to be processed by our special trigger handler wrapper
        # but not by any other handlers, so we filter out these messages from
        # other handlers by adding DropTriggerLogsFilter
        # we could consider only adding this filter to the default console logger
        # so as to leave other custom handlers alone
        for h in root_logger.handlers:
            if h is not new_hdlr:
                h.addFilter(DropTriggerLogsFilter())

    def add_handler_wrapper_to_root(base_handler):
        # first make sure we remove from root logger if it happens to be there
        # it could have come from root or airflow.task, but we only need
        # to make sure we remove from root, since messages will not flow
        # through airflow.task
        if base_handler in root_logger.handlers:
            root_logger.removeHandler(base_handler)

        logger.info("Setting up TriggererHandlerWrapper with handler %s", base_handler)
        h = TriggererHandlerWrapper(base_handler=base_handler, level=base_handler.level)
        # just extra cautious, checking if user manually configured it there
        if h not in root_logger.handlers:
            root_logger.addHandler(h)
        return h

    root_logger = logging.getLogger()
    task_handler = find_suitable_task_handler()
    if not task_handler:
        return None
    if TYPE_CHECKING:
        assert isinstance(task_handler, FileTaskHandler)
    if should_wrap(task_handler):
        trigger_handler = add_handler_wrapper_to_root(task_handler)
    else:
        trigger_handler = copy(task_handler)
        root_logger.addHandler(trigger_handler)
    filter_trigger_logs_from_other_root_handlers(trigger_handler)
    if send_trigger_end_marker(trigger_handler) is False:
        global SEND_TRIGGER_END_MARKER
        SEND_TRIGGER_END_MARKER = False
    HANDLER_SUPPORTS_TRIGGERER = True
    return should_queue(trigger_handler)


def setup_queue_listener():
    """
    Route log messages to a queue and process them with QueueListener.

    Airflow task handlers make blocking I/O calls.
    We replace trigger log handlers, with LocalQueueHandler,
    which sends log records to a queue.
    Then we start a QueueListener in a thread, which is configured
    to consume the queue and pass the records to the handlers as
    originally configured. This keeps the handler I/O out of the
    async event loop.

    :meta private:
    """
    queue = SimpleQueue()
    root_logger = logging.getLogger()

    handlers: list[logging.Handler] = []

    queue_handler = LocalQueueHandler(queue)
    queue_handler.addFilter(TriggerMetadataFilter())

    root_logger.addHandler(queue_handler)
    for h in root_logger.handlers[:]:
        if h is not queue_handler and "pytest" not in h.__module__:
            root_logger.removeHandler(h)
            handlers.append(h)

    this_logger = logging.getLogger(__name__)
    if handlers:
        this_logger.info("Setting up logging queue listener with handlers %s", handlers)
        listener = logging.handlers.QueueListener(queue, *handlers, respect_handler_level=True)
        listener.start()
        return listener
    else:
        this_logger.warning("Unable to set up individual trigger logging")
        return None


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
            raise ValueError(f"Capacity number {capacity} is invalid")

        self.health_check_threshold = conf.getint("triggerer", "triggerer_health_check_threshold")

        should_queue = True
        if DISABLE_WRAPPER:
            self.log.warning(
                "Skipping trigger log configuration; disabled by param "
                "`disable_trigger_handler_wrapper=True`."
            )
        else:
            should_queue = configure_trigger_log_handler()
        self.listener = None
        if DISABLE_LISTENER:
            self.log.warning(
                "Skipping trigger logger queue listener; disabled by param "
                "`disable_trigger_handler_queue_listener=True`."
            )
        elif should_queue is False:
            self.log.warning("Skipping trigger logger queue listener; disabled by handler setting.")
        else:
            self.listener = setup_queue_listener()
        # Set up runner async thread
        self.trigger_runner = TriggerRunner()

    @provide_session
    def heartbeat_callback(self, session: Session = NEW_SESSION) -> None:
        Stats.incr("triggerer_heartbeat", 1, 1)

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
        self.trigger_runner.stop = True

    def _kill_listener(self):
        if self.listener:
            for h in self.listener.handlers:
                h.close()
            self.listener.stop()

    def _exit_gracefully(self, signum, frame) -> None:
        """Clean up processor_agent to avoid leaving orphan processes."""
        # The first time, try to exit nicely
        if not self.trigger_runner.stop:
            self.log.info("Exiting gracefully upon receiving signal %s", signum)
            self.trigger_runner.stop = True
            self._kill_listener()
        else:
            self.log.warning("Forcing exit due to second exit signal %s", signum)
            sys.exit(os.EX_SOFTWARE)

    def _execute(self) -> int | None:
        self.log.info("Starting the triggerer")
        try:
            # set job_id so that it can be used in log file names
            self.trigger_runner.job_id = self.job.id

            # Kick off runner thread
            self.trigger_runner.start()
            # Start our own DB loop in the main thread
            self._run_trigger_loop()
        except Exception:
            self.log.exception("Exception when executing TriggererJobRunner._run_trigger_loop")
            raise
        finally:
            self.log.info("Waiting for triggers to clean up")
            # Tell the subthread to stop and then wait for it.
            # If the user interrupts/terms again, _graceful_exit will allow them
            # to force-kill here.
            self.trigger_runner.stop = True
            self.trigger_runner.join(30)
            self.log.info("Exited trigger loop")
        return None

    def _run_trigger_loop(self) -> None:
        """Run synchronously and handle all database reads/writes; the main-thread trigger loop."""
        while not self.trigger_runner.stop:
            if not self.trigger_runner.is_alive():
                self.log.error("Trigger runner thread has died! Exiting.")
                break
            with Trace.start_span(span_name="triggerer_job_loop", component="TriggererJobRunner") as span:
                # Clean out unused triggers
                if span.is_recording():
                    span.add_event(name="Trigger.clean_unused")
                Trigger.clean_unused()
                # Load/delete triggers
                if span.is_recording():
                    span.add_event(name="load_triggers")
                self.load_triggers()
                # Handle events
                if span.is_recording():
                    span.add_event(name="handle_events")
                self.handle_events()
                # Handle failed triggers
                if span.is_recording():
                    span.add_event(name="handle_failed_triggers")
                self.handle_failed_triggers()
                if span.is_recording():
                    span.add_event(name="perform_heartbeat")
                perform_heartbeat(
                    self.job, heartbeat_callback=self.heartbeat_callback, only_if_necessary=True
                )
                # Collect stats
                if span.is_recording():
                    span.add_event(name="emit_metrics")
                self.emit_metrics()
            # Idle sleep
            time.sleep(1)

    @add_span
    def load_triggers(self):
        """Query the database for the triggers we're supposed to be running and update the runner."""
        Trigger.assign_unassigned(self.job.id, self.capacity, self.health_check_threshold)
        ids = Trigger.ids_for_triggerer(self.job.id)
        self.trigger_runner.update_triggers(set(ids))

    @add_span
    def handle_events(self):
        """Dispatch outbound events to the Trigger model which pushes them to the relevant task instances."""
        while self.trigger_runner.events:
            # Get the event and its trigger ID
            trigger_id, event = self.trigger_runner.events.popleft()
            # Tell the model to wake up its tasks
            Trigger.submit_event(trigger_id=trigger_id, event=event)
            # Emit stat event
            Stats.incr("triggers.succeeded")

    @add_span
    def handle_failed_triggers(self):
        """
        Handle "failed" triggers. - ones that errored or exited before they sent an event.

        Task Instances that depend on them need failing.
        """
        while self.trigger_runner.failed_triggers:
            # Tell the model to fail this trigger's deps
            trigger_id, saved_exc = self.trigger_runner.failed_triggers.popleft()
            Trigger.submit_failure(trigger_id=trigger_id, exc=saved_exc)
            # Emit stat event
            Stats.incr("triggers.failed")

    @add_span
    def emit_metrics(self):
        Stats.gauge(f"triggers.running.{self.job.hostname}", len(self.trigger_runner.triggers))
        Stats.gauge(
            "triggers.running", len(self.trigger_runner.triggers), tags={"hostname": self.job.hostname}
        )
        span = Trace.get_current_span()
        span.set_attribute("trigger host", self.job.hostname)
        span.set_attribute("triggers running", len(self.trigger_runner.triggers))


class TriggerDetails(TypedDict):
    """Type class for the trigger details dictionary."""

    task: asyncio.Task
    name: str
    events: int


class TriggerRunner(threading.Thread, LoggingMixin):
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
    to_create: deque[tuple[int, BaseTrigger]]

    # Inbound queue of deleted triggers
    to_cancel: deque[int]

    # Outbound queue of events
    events: deque[tuple[int, TriggerEvent]]

    # Outbound queue of failed triggers
    failed_triggers: deque[tuple[int, BaseException]]

    # Should-we-stop flag
    stop: bool = False

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
        last_status = time.time()
        try:
            while not self.stop:
                # Run core logic
                await self.create_triggers()
                await self.cancel_triggers()
                await self.cleanup_finished_triggers()
                # Sleep for a bit
                await asyncio.sleep(1)
                # Every minute, log status
                if time.time() - last_status >= 60:
                    count = len(self.triggers)
                    self.log.info("%i triggers currently running", count)
                    last_status = time.time()
        except Exception:
            self.stop = True
            raise
        # Wait for watchdog to complete
        await watchdog

    async def create_triggers(self):
        """Drain the to_create queue and create all new triggers that have been requested in the DB."""
        while self.to_create:
            trigger_id, trigger_instance = self.to_create.popleft()
            if trigger_id not in self.triggers:
                ti: TaskInstance = trigger_instance.task_instance
                self.triggers[trigger_id] = {
                    "task": asyncio.create_task(self.run_trigger(trigger_id, trigger_instance)),
                    "name": f"{ti.dag_id}/{ti.run_id}/{ti.task_id}/{ti.map_index}/{ti.try_number} "
                    f"(ID {trigger_id})",
                    "events": 0,
                }
            else:
                self.log.warning("Trigger %s had insertion attempted twice", trigger_id)
            await asyncio.sleep(0)

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

    async def cleanup_finished_triggers(self):
        """
        Go through all trigger tasks (coroutines) and clean up entries for ones that have exited.

        Optionally warn users if the exit was not normal.
        """
        for trigger_id, details in list(self.triggers.items()):
            if details["task"].done():
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
                    self.log.exception("Trigger %s exited with error %s", details["name"], e)
                    saved_exc = e
                else:
                    # See if they foolishly returned a TriggerEvent
                    if isinstance(result, TriggerEvent):
                        self.log.error(
                            "Trigger %s returned a TriggerEvent rather than yielding it", details["name"]
                        )
                # See if this exited without sending an event, in which case
                # any task instances depending on it need to be failed
                if details["events"] == 0:
                    self.log.error(
                        "Trigger %s exited without sending an event. Dependent tasks will be failed.",
                        details["name"],
                    )
                    self.failed_triggers.append((trigger_id, saved_exc))
                del self.triggers[trigger_id]
            await asyncio.sleep(0)

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
                self.log.info(
                    "Triggerer's async thread was blocked for %.2f seconds, "
                    "likely by a badly-written trigger. Set PYTHONASYNCIODEBUG=1 "
                    "to get more information on overrunning coroutines.",
                    time_elapsed,
                )
                Stats.incr("triggers.blocked_main_thread")

    @staticmethod
    def set_individual_trigger_logging(trigger):
        """Configure trigger logging to allow individual files and stdout filtering."""
        # set logging context vars for routing to appropriate handler
        ctx_task_instance.set(trigger.task_instance)
        ctx_trigger_id.set(trigger.trigger_id)
        ctx_trigger_end.set(False)

        # mark that we're in the context of an individual trigger so log records can be filtered
        ctx_indiv_trigger.set(True)

    async def run_trigger(self, trigger_id, trigger):
        """Run a trigger (they are async generators) and push their events into our outbound event deque."""
        name = self.triggers[trigger_id]["name"]
        self.log.info("trigger %s starting", name)
        try:
            self.set_individual_trigger_logging(trigger)
            async for event in trigger.run():
                self.log.info("Trigger %s fired: %s", self.triggers[trigger_id]["name"], event)
                self.triggers[trigger_id]["events"] += 1
                self.events.append((trigger_id, event))
        except asyncio.CancelledError:
            if timeout := trigger.task_instance.trigger_timeout:
                timeout = timeout.replace(tzinfo=timezone.utc) if not timeout.tzinfo else timeout
                if timeout < timezone.utcnow():
                    self.log.error("Trigger cancelled due to timeout")
            raise
        finally:
            # CancelledError will get injected when we're stopped - which is
            # fine, the cleanup process will understand that, but we want to
            # allow triggers a chance to cleanup, either in that case or if
            # they exit cleanly. Exception from cleanup methods are ignored.
            with suppress(Exception):
                await trigger.cleanup()
            if SEND_TRIGGER_END_MARKER:
                self.mark_trigger_end(trigger)

            # unsetting ctx_indiv_trigger var restores stdout logging
            ctx_indiv_trigger.set(None)
            self.log.info("trigger %s completed", name)

    @staticmethod
    def mark_trigger_end(trigger):
        if not HANDLER_SUPPORTS_TRIGGERER:
            return
        ctx_trigger_end.set(True)
        # this is a special message required by TriggerHandlerWrapper
        # it tells the wrapper to close the handler for this trigger
        # we set level to 100 so that it will not be filtered by user logging settings
        # it is not emitted; see TriggererHandlerWrapper.handle method.
        trigger.log.log(level=100, msg="trigger end")

    def update_triggers(self, requested_trigger_ids: set[int]):
        """
        Request that we update what triggers we're running.

        Works out the differences - ones to add, and ones to remove - then
        adds them to the deques so the subthread can actually mutate the running
        trigger set.
        """
        # Note that `triggers` could be mutated by the other thread during this
        # line's execution, but we consider that safe, since there's a strict
        # add -> remove -> never again lifecycle this function is already
        # handling.
        running_trigger_ids = set(self.triggers.keys())
        known_trigger_ids = (
            running_trigger_ids.union(x[0] for x in self.events)
            .union(self.to_cancel)
            .union(x[0] for x in self.to_create)
            .union(trigger[0] for trigger in self.failed_triggers)
        )
        # Work out the two difference sets
        new_trigger_ids = requested_trigger_ids - known_trigger_ids
        cancel_trigger_ids = running_trigger_ids - requested_trigger_ids
        # Bulk-fetch new trigger records
        new_triggers = Trigger.bulk_fetch(new_trigger_ids)
        # Add in new triggers
        for new_id in new_trigger_ids:
            # Check it didn't vanish in the meantime
            if new_id not in new_triggers:
                self.log.warning("Trigger ID %s disappeared before we could start it", new_id)
                continue
            # Resolve trigger record into an actual class instance
            try:
                new_trigger_orm = new_triggers[new_id]
                trigger_class = self.get_trigger_by_classpath(new_trigger_orm.classpath)
            except BaseException as e:
                # Either the trigger code or the path to it is bad. Fail the trigger.
                self.failed_triggers.append((new_id, e))
                continue

            # If new_trigger_orm.task_instance is None, this means the TaskInstance
            # row was updated by either Trigger.submit_event or Trigger.submit_failure
            # and can happen when a single trigger Job is being run on multiple TriggerRunners
            # in a High-Availability setup.
            if new_trigger_orm.task_instance is None:
                self.log.info(
                    (
                        "TaskInstance for Trigger ID %s is None. It was likely updated by another trigger job. "
                        "Skipping trigger instantiation."
                    ),
                    new_id,
                )
                continue

            try:
                new_trigger_instance = trigger_class(**new_trigger_orm.kwargs)
            except TypeError as err:
                self.log.error("Trigger failed; message=%s", err)
                self.failed_triggers.append((new_id, err))
                continue

            self.set_trigger_logging_metadata(new_trigger_orm.task_instance, new_id, new_trigger_instance)
            self.to_create.append((new_id, new_trigger_instance))
        # Enqueue orphaned triggers for cancellation
        self.to_cancel.extend(cancel_trigger_ids)

    def set_trigger_logging_metadata(self, ti: TaskInstance, trigger_id, trigger):
        """
        Set up logging for triggers.

        We want to ensure that each trigger logs to its own file and that the log messages are not
        propagated to parent loggers.

        :meta private:
        """
        if ti:  # can be None in tests
            ti.is_trigger_log_context = True
        trigger.task_instance = ti
        trigger.triggerer_job_id = self.job_id
        trigger.trigger_id = trigger_id

    def get_trigger_by_classpath(self, classpath: str) -> type[BaseTrigger]:
        """
        Get a trigger class by its classpath ("path.to.module.classname").

        Uses a cache dictionary to speed up lookups after the first time.
        """
        if classpath not in self.trigger_cache:
            self.trigger_cache[classpath] = import_string(classpath)
        return self.trigger_cache[classpath]
