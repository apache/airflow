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
"""Base executor - this is the base class for all the implemented executors."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any

import pendulum

from airflow.cli.cli_config import DefaultHelpParser
from airflow.configuration import conf
from airflow.executors import workloads
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models import Log
from airflow.stats import Stats
from airflow.traces import NO_TRACE_ID
from airflow.traces.tracer import DebugTrace, Trace, add_debug_span, gen_context
from airflow.traces.utils import gen_span_id_from_ti_key
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState
from airflow.utils.thread_safe_dict import ThreadSafeDict

PARALLELISM: int = conf.getint("core", "PARALLELISM")

if TYPE_CHECKING:
    import argparse
    from datetime import datetime

    from sqlalchemy.orm import Session

    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.callbacks.base_callback_sink import BaseCallbackSink
    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.cli.cli_config import GroupCommand
    from airflow.executors.executor_utils import ExecutorName
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey

    # Event_buffer dict value type
    # Tuple of: state, info
    EventBufferValueType = tuple[str | None, Any]


log = logging.getLogger(__name__)


@dataclass
class RunningRetryAttemptType:
    """
    For keeping track of attempts to queue again when task still apparently running.

    We don't want to slow down the loop, so we don't block, but we allow it to be
    re-checked for at least MIN_SECONDS seconds.
    """

    MIN_SECONDS = 10
    total_tries: int = field(default=0, init=False)
    tries_after_min: int = field(default=0, init=False)
    first_attempt_time: datetime = field(default_factory=lambda: pendulum.now("UTC"), init=False)

    @property
    def elapsed(self):
        """Seconds since first attempt."""
        return (pendulum.now("UTC") - self.first_attempt_time).total_seconds()

    def can_try_again(self):
        """Return False if there has been at least one try greater than MIN_SECONDS, otherwise return True."""
        if self.tries_after_min > 0:
            return False

        self.total_tries += 1

        elapsed = self.elapsed
        if elapsed > self.MIN_SECONDS:
            self.tries_after_min += 1
        log.debug("elapsed=%s tries=%s", elapsed, self.total_tries)
        return True


class ExecutorConf:
    """
    This class is used to fetch configuration for an executor for a particular team_name.

    It wraps the implementation of the configuration.get() to look for the particular section and key
    prefixed with the team_name. This makes it easy for child classes (i.e. concrete executors) to fetch
    configuration values for a particular team_name without having to worry about passing through the
    team_name for every call to get configuration.

    Currently config only supports environment variables for team specific configuration.
    """

    def __init__(self, team_name: str | None = None) -> None:
        self.team_name: str | None = team_name

    def get(self, *args, **kwargs) -> str | None:
        return conf.get(*args, **kwargs, team_name=self.team_name)

    def getboolean(self, *args, **kwargs) -> bool:
        return conf.getboolean(*args, **kwargs, team_name=self.team_name)


class BaseExecutor(LoggingMixin):
    """
    Base class to inherit for concrete executors such as Celery, Kubernetes, Local, etc.

    :param parallelism: how many jobs should run at one time.
    """

    active_spans = ThreadSafeDict()

    supports_ad_hoc_ti_run: bool = False
    sentry_integration: str = ""

    is_local: bool = False
    is_production: bool = True

    serve_logs: bool = False

    job_id: None | int | str = None
    name: None | ExecutorName = None
    callback_sink: BaseCallbackSink | None = None

    @cached_property
    def jwt_generator(self) -> JWTGenerator:
        from airflow.api_fastapi.auth.tokens import (
            JWTGenerator,
            get_signing_args,
        )
        from airflow.configuration import conf

        generator = JWTGenerator(
            valid_for=conf.getint("execution_api", "jwt_expiration_time"),
            audience=conf.get_mandatory_list_value("execution_api", "jwt_audience")[0],
            issuer=conf.get("api_auth", "jwt_issuer", fallback=None),
            # Since this one is used across components/server, there is no point trying to generate one, error
            # instead
            **get_signing_args(make_secret_key_if_needed=False),
        )

        return generator

    def __init__(self, parallelism: int = PARALLELISM, team_name: str | None = None):
        super().__init__()
        # Ensure we set this now, so that each subprocess gets the same value
        from airflow.api_fastapi.auth.tokens import get_signing_args

        get_signing_args()

        self.parallelism: int = parallelism
        self.team_name: str | None = team_name
        self.queued_tasks: dict[TaskInstanceKey, workloads.ExecuteTask] = {}
        self.running: set[TaskInstanceKey] = set()
        self.event_buffer: dict[TaskInstanceKey, EventBufferValueType] = {}
        self._task_event_logs: deque[Log] = deque()
        self.conf = ExecutorConf(team_name)

        if self.parallelism <= 0:
            raise ValueError("parallelism is set to 0 or lower")

        """
        Deque for storing task event log messages.

        This attribute is only internally public and should not be manipulated
        directly by subclasses.

        :meta private:
        """

        self.attempts: dict[TaskInstanceKey, RunningRetryAttemptType] = defaultdict(RunningRetryAttemptType)

    def __repr__(self):
        return f"{self.__class__.__name__}(parallelism={self.parallelism})"

    @classmethod
    def set_active_spans(cls, active_spans: ThreadSafeDict):
        cls.active_spans = active_spans

    def start(self):  # pragma: no cover
        """Executors may need to get things started."""

    def log_task_event(self, *, event: str, extra: str, ti_key: TaskInstanceKey):
        """Add an event to the log table."""
        self._task_event_logs.append(Log(event=event, task_instance=ti_key, extra=extra))

    def queue_workload(self, workload: workloads.All, session: Session) -> None:
        if not isinstance(workload, workloads.ExecuteTask):
            raise ValueError(f"Un-handled workload kind {type(workload).__name__!r} in {type(self).__name__}")
        ti = workload.ti
        self.queued_tasks[ti.key] = workload

    def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
        """
        Process the given workloads.

        This method must be implemented by subclasses to define how they handle
        the execution of workloads (e.g., queuing them to workers, submitting to
        external systems, etc.).

        :param workloads: List of workloads to process
        """
        raise NotImplementedError(f"{type(self).__name__} must implement _process_workloads()")

    def has_task(self, task_instance: TaskInstance) -> bool:
        """
        Check if a task is either queued or running in this executor.

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        return (
            task_instance.id in self.queued_tasks
            or task_instance.id in self.running
            or task_instance.key in self.queued_tasks
            or task_instance.key in self.running
        )

    def sync(self) -> None:
        """
        Sync will get called periodically by the heartbeat method.

        Executors should override this to perform gather statuses.
        """

    @add_debug_span
    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs."""
        open_slots = self.parallelism - len(self.running)

        num_running_tasks = len(self.running)
        num_queued_tasks = len(self.queued_tasks)

        self._emit_metrics(open_slots, num_running_tasks, num_queued_tasks)
        self.trigger_tasks(open_slots)

        # Calling child class sync method
        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

    def _emit_metrics(self, open_slots, num_running_tasks, num_queued_tasks):
        """
        Emit metrics relevant to the Executor.

        In the case of multiple executors being configured, the metric names include the name of
        executor to differentiate them from metrics from other executors.

        If only one executor is configured, the metric names will not be changed.
        """
        name = self.__class__.__name__
        multiple_executors_configured = len(ExecutorLoader.get_executor_names()) > 1
        if multiple_executors_configured:
            metric_suffix = name

        open_slots_metric_name = (
            f"executor.open_slots.{metric_suffix}" if multiple_executors_configured else "executor.open_slots"
        )
        queued_tasks_metric_name = (
            f"executor.queued_tasks.{metric_suffix}"
            if multiple_executors_configured
            else "executor.queued_tasks"
        )
        running_tasks_metric_name = (
            f"executor.running_tasks.{metric_suffix}"
            if multiple_executors_configured
            else "executor.running_tasks"
        )

        span = Trace.get_current_span()
        if span.is_recording():
            span.add_event(
                name="executor",
                attributes={
                    open_slots_metric_name: open_slots,
                    queued_tasks_metric_name: num_queued_tasks,
                    running_tasks_metric_name: num_running_tasks,
                },
            )

        self.log.debug("%s running task instances for executor %s", num_running_tasks, name)
        self.log.debug("%s in queue for executor %s", num_queued_tasks, name)
        if open_slots == 0:
            self.log.info("Executor parallelism limit reached. 0 open slots.")
        else:
            self.log.debug("%s open slots for executor %s", open_slots, name)

        Stats.gauge(
            open_slots_metric_name,
            value=open_slots,
            tags={"status": "open", "name": name},
        )
        Stats.gauge(
            queued_tasks_metric_name,
            value=num_queued_tasks,
            tags={"status": "queued", "name": name},
        )
        Stats.gauge(
            running_tasks_metric_name,
            value=num_running_tasks,
            tags={"status": "running", "name": name},
        )

    def order_queued_tasks_by_priority(self) -> list[tuple[TaskInstanceKey, workloads.ExecuteTask]]:
        """
        Orders the queued tasks by priority.

        :return: List of workloads from the queued_tasks according to the priority.
        """
        if not self.queued_tasks:
            return []

        # V3 + new executor that supports workloads
        return sorted(
            self.queued_tasks.items(),
            key=lambda x: x[1].ti.priority_weight,
            reverse=True,
        )

    @add_debug_span
    def trigger_tasks(self, open_slots: int) -> None:
        """
        Initiate async execution of the queued tasks, up to the number of available slots.

        :param open_slots: Number of open slots
        """
        sorted_queue = self.order_queued_tasks_by_priority()
        workload_list = []

        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, item = sorted_queue.pop(0)

            # If a task makes it here but is still understood by the executor
            # to be running, it generally means that the task has been killed
            # externally and not yet been marked as failed.
            #
            # However, when a task is deferred, there is also a possibility of
            # a race condition where a task might be scheduled again during
            # trigger processing, even before we are able to register that the
            # deferred task has completed. In this case and for this reason,
            # we make a small number of attempts to see if the task has been
            # removed from the running set in the meantime.
            if key in self.attempts:
                del self.attempts[key]

            if isinstance(item, workloads.ExecuteTask) and hasattr(item, "ti"):
                ti = item.ti

                # If it's None, then the span for the current id hasn't been started.
                if self.active_spans is not None and self.active_spans.get("ti:" + str(ti.id)) is None:
                    if isinstance(ti, workloads.TaskInstance):
                        parent_context = Trace.extract(ti.parent_context_carrier)
                    else:
                        parent_context = Trace.extract(ti.dag_run.context_carrier)
                    # Start a new span using the context from the parent.
                    # Attributes will be set once the task has finished so that all
                    # values will be available (end_time, duration, etc.).

                    span = Trace.start_child_span(
                        span_name=f"{ti.task_id}",
                        parent_context=parent_context,
                        component="task",
                        start_as_current=False,
                    )
                    self.active_spans.set("ti:" + str(ti.id), span)
                    # Inject the current context into the carrier.
                    carrier = Trace.inject()
                    ti.context_carrier = carrier

                workload_list.append(item)
        if workload_list:
            self._process_workloads(workload_list)

    # TODO: This should not be using `TaskInstanceState` here, this is just "did the process complete, or did
    # it die". It is possible for the task itself to finish with success, but the state of the task to be set
    # to FAILED. By using TaskInstanceState enum here it confuses matters!
    def change_state(
        self, key: TaskInstanceKey, state: TaskInstanceState, info=None, remove_running=True
    ) -> None:
        """
        Change state of the task.

        :param key: Unique key for the task instance
        :param state: State to set for the task.
        :param info: Executor information for the task instance
        :param remove_running: Whether or not to remove the TI key from running set
        """
        self.log.debug("Changing state: %s", key)
        if remove_running:
            try:
                self.running.remove(key)
            except KeyError:
                self.log.debug("Could not find key: %s", key)
        self.event_buffer[key] = state, info

    def fail(self, key: TaskInstanceKey, info=None) -> None:
        """
        Set fail state for the event.

        :param info: Executor information for the task instance
        :param key: Unique key for the task instance
        """
        trace_id = Trace.get_current_span().get_span_context().trace_id
        if trace_id != NO_TRACE_ID:
            span_id = int(gen_span_id_from_ti_key(key, as_int=True))
            with DebugTrace.start_span(
                span_name="fail",
                component="BaseExecutor",
                parent_sc=gen_context(trace_id=trace_id, span_id=span_id),
            ) as span:
                span.set_attributes(
                    {
                        "dag_id": key.dag_id,
                        "run_id": key.run_id,
                        "task_id": key.task_id,
                        "try_number": key.try_number,
                        "error": True,
                    }
                )

        self.change_state(key, TaskInstanceState.FAILED, info)

    def success(self, key: TaskInstanceKey, info=None) -> None:
        """
        Set success state for the event.

        :param info: Executor information for the task instance
        :param key: Unique key for the task instance
        """
        trace_id = Trace.get_current_span().get_span_context().trace_id
        if trace_id != NO_TRACE_ID:
            span_id = int(gen_span_id_from_ti_key(key, as_int=True))
            with DebugTrace.start_span(
                span_name="success",
                component="BaseExecutor",
                parent_sc=gen_context(trace_id=trace_id, span_id=span_id),
            ) as span:
                span.set_attributes(
                    {
                        "dag_id": key.dag_id,
                        "run_id": key.run_id,
                        "task_id": key.task_id,
                        "try_number": key.try_number,
                    }
                )

        self.change_state(key, TaskInstanceState.SUCCESS, info)

    def queued(self, key: TaskInstanceKey, info=None) -> None:
        """
        Set queued state for the event.

        :param info: Executor information for the task instance
        :param key: Unique key for the task instance
        """
        self.change_state(key, TaskInstanceState.QUEUED, info)

    def running_state(self, key: TaskInstanceKey, info=None) -> None:
        """
        Set running state for the event.

        :param info: Executor information for the task instance
        :param key: Unique key for the task instance
        """
        self.change_state(key, TaskInstanceState.RUNNING, info, remove_running=False)

    def get_event_buffer(self, dag_ids=None) -> dict[TaskInstanceKey, EventBufferValueType]:
        """
        Return and flush the event buffer.

        In case dag_ids is specified it will only return and flush events
        for the given dag_ids. Otherwise, it returns and flushes all events.

        :param dag_ids: the dag_ids to return events for; returns all if given ``None``.
        :return: a dict of events
        """
        cleared_events: dict[TaskInstanceKey, EventBufferValueType] = {}
        if dag_ids is None:
            cleared_events = self.event_buffer
            self.event_buffer = {}
        else:
            for ti_key in list(self.event_buffer.keys()):
                if ti_key.dag_id in dag_ids:
                    cleared_events[ti_key] = self.event_buffer.pop(ti_key)

        return cleared_events

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        """
        Return the task logs.

        :param ti: A TaskInstance object
        :param try_number: current try_number to read log from
        :return: tuple of logs and messages
        """
        return [], []

    def end(self) -> None:  # pragma: no cover
        """Wait synchronously for the previously submitted job to complete."""
        raise NotImplementedError

    def terminate(self):
        """Get called when the daemon receives a SIGTERM."""
        raise NotImplementedError

    def revoke_task(self, *, ti: TaskInstance):
        """
        Attempt to remove task from executor.

        It should attempt to ensure that the task is no longer running on the worker,
        and ensure that it is cleared out from internal data structures.

        It should *not* change the state of the task in airflow, or add any events
        to the event buffer.

        It should not raise any error.

        :param ti: Task instance to remove
        """
        raise NotImplementedError

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

        Anything that is not adopted will be cleared by the scheduler (and then become eligible for
        re-scheduling)

        :return: any TaskInstances that were unable to be adopted
        """
        # By default, assume Executors cannot adopt tasks, so just say we failed to adopt anything.
        # Subclasses can do better!
        return tis

    @property
    def slots_available(self):
        """Number of new tasks this executor instance can accept."""
        return self.parallelism - len(self.running) - len(self.queued_tasks)

    @property
    def slots_occupied(self):
        """Number of tasks this executor instance is currently managing."""
        return len(self.running) + len(self.queued_tasks)

    def debug_dump(self):
        """Get called in response to SIGUSR2 by the scheduler."""
        self.log.info(
            "executor.queued (%d)\n\t%s",
            len(self.queued_tasks),
            "\n\t".join(map(repr, self.queued_tasks.items())),
        )
        self.log.info("executor.running (%d)\n\t%s", len(self.running), "\n\t".join(map(repr, self.running)))
        self.log.info(
            "executor.event_buffer (%d)\n\t%s",
            len(self.event_buffer),
            "\n\t".join(map(repr, self.event_buffer.items())),
        )

    def send_callback(self, request: CallbackRequest) -> None:
        """
        Send callback for execution.

        Provides a default implementation which sends the callback to the `callback_sink` object.

        :param request: Callback request to be executed.
        """
        if not self.callback_sink:
            raise ValueError("Callback sink is not ready.")
        self.callback_sink.send(request)

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        """
        Vends CLI commands to be included in Airflow CLI.

        Override this method to expose commands via Airflow CLI to manage this executor. This can
        be commands to setup/teardown the executor, inspect state, etc.
        Make sure to choose unique names for those commands, to avoid collisions.
        """
        return []

    @classmethod
    def _get_parser(cls) -> argparse.ArgumentParser:
        """
        Generate documentation; used by Sphinx argparse.

        :meta private:
        """
        from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

        parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
        subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
        for group_command in cls.get_cli_commands():
            _add_command(subparsers, group_command)
        return parser
