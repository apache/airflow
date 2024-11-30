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
import sys
from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

import pendulum
from deprecated import deprecated

from airflow.cli.cli_config import DefaultHelpParser
from airflow.configuration import conf
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models import Log
from airflow.stats import Stats
from airflow.traces import NO_TRACE_ID
from airflow.traces.tracer import Trace, add_span, gen_context
from airflow.traces.utils import gen_span_id_from_ti_key, gen_trace_id
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

PARALLELISM: int = conf.getint("core", "PARALLELISM")

if TYPE_CHECKING:
    import argparse
    from datetime import datetime

    from airflow.callbacks.base_callback_sink import BaseCallbackSink
    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.cli.cli_config import GroupCommand
    from airflow.executors import workloads
    from airflow.executors.executor_utils import ExecutorName
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey

    # Command to execute - list of strings
    # the first element is always "airflow".
    # It should be result of TaskInstance.generate_command method.
    CommandType = list[str]

    # Task that is queued. It contains all the information that is
    # needed to run the task.
    #
    # Tuple of: command, priority, queue name, TaskInstance
    QueuedTaskInstanceType = tuple[CommandType, int, Optional[str], TaskInstance]

    # Event_buffer dict value type
    # Tuple of: state, info
    EventBufferValueType = tuple[Optional[str], Any]

    # Task tuple to send to be executed
    TaskTuple = tuple[TaskInstanceKey, CommandType, Optional[str], Optional[Any]]

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


class BaseExecutor(LoggingMixin):
    """
    Base class to inherit for concrete executors such as Celery, Kubernetes, Local, Sequential, etc.

    :param parallelism: how many jobs should run at one time. Set to ``0`` for infinity.
    """

    supports_ad_hoc_ti_run: bool = False
    supports_sentry: bool = False

    is_local: bool = False
    is_single_threaded: bool = False
    is_production: bool = True

    change_sensor_mode_to_reschedule: bool = False
    serve_logs: bool = False

    job_id: None | int | str = None
    name: None | ExecutorName = None
    callback_sink: BaseCallbackSink | None = None

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__()
        self.parallelism: int = parallelism
        self.queued_tasks: dict[TaskInstanceKey, QueuedTaskInstanceType] = {}
        self.running: set[TaskInstanceKey] = set()
        self.event_buffer: dict[TaskInstanceKey, EventBufferValueType] = {}
        self._task_event_logs: deque[Log] = deque()
        """
        Deque for storing task event log messages.

        This attribute is only internally public and should not be manipulated
        directly by subclasses.

        :meta private:
        """

        self.attempts: dict[TaskInstanceKey, RunningRetryAttemptType] = defaultdict(RunningRetryAttemptType)

    def __repr__(self):
        return f"{self.__class__.__name__}(parallelism={self.parallelism})"

    def start(self):  # pragma: no cover
        """Executors may need to get things started."""

    def log_task_event(self, *, event: str, extra: str, ti_key: TaskInstanceKey):
        """Add an event to the log table."""
        self._task_event_logs.append(Log(event=event, task_instance=ti_key, extra=extra))

    def queue_command(
        self,
        task_instance: TaskInstance,
        command: CommandType,
        priority: int = 1,
        queue: str | None = None,
    ):
        """Queues command to task."""
        if task_instance.key not in self.queued_tasks:
            self.log.info("Adding to queue: %s", command)
            self.queued_tasks[task_instance.key] = (command, priority, queue, task_instance)
        else:
            self.log.error("could not queue task %s", task_instance.key)

    def queue_workload(self, workload: workloads.All) -> None:
        raise ValueError(f"Un-handled workload kind {type(workload).__name__!r} in {type(self).__name__}")

    def queue_task_instance(
        self,
        task_instance: TaskInstance,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        pool: str | None = None,
        cfg_path: str | None = None,
    ) -> None:
        """Queues task instance."""
        if TYPE_CHECKING:
            assert task_instance.task

        pool = pool or task_instance.pool

        command_list_to_run = task_instance.command_as_list(
            local=True,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            pool=pool,
            # cfg_path is needed to propagate the config values if using impersonation
            # (run_as_user), given that there are different code paths running tasks.
            # https://github.com/apache/airflow/pull/2991
            cfg_path=cfg_path,
        )
        self.log.debug("created command %s", command_list_to_run)
        self.queue_command(
            task_instance,
            command_list_to_run,
            priority=task_instance.priority_weight,
            queue=task_instance.task.queue,
        )

    def has_task(self, task_instance: TaskInstance) -> bool:
        """
        Check if a task is either queued or running in this executor.

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        return task_instance.key in self.queued_tasks or task_instance.key in self.running

    def sync(self) -> None:
        """
        Sync will get called periodically by the heartbeat method.

        Executors should override this to perform gather statuses.
        """

    @add_span
    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs."""
        if not self.parallelism:
            open_slots = len(self.queued_tasks)
        else:
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
            if self.parallelism:
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

    def order_queued_tasks_by_priority(self) -> list[tuple[TaskInstanceKey, QueuedTaskInstanceType]]:
        """
        Orders the queued tasks by priority.

        :return: List of tuples from the queued_tasks according to the priority.
        """
        return sorted(
            self.queued_tasks.items(),
            key=lambda x: x[1][1],
            reverse=True,
        )

    @add_span
    def trigger_tasks(self, open_slots: int) -> None:
        """
        Initiate async execution of the queued tasks, up to the number of available slots.

        :param open_slots: Number of open slots
        """
        span = Trace.get_current_span()
        sorted_queue = self.order_queued_tasks_by_priority()
        task_tuples = []

        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, ti) = sorted_queue.pop(0)

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
            if key in self.running:
                attempt = self.attempts[key]
                if attempt.can_try_again():
                    # if it hasn't been much time since first check, let it be checked again next time
                    self.log.info("queued but still running; attempt=%s task=%s", attempt.total_tries, key)
                    continue

                # Otherwise, we give up and remove the task from the queue.
                self.log.error(
                    "could not queue task %s (still running after %d attempts).",
                    key,
                    attempt.total_tries,
                )
                self.log_task_event(
                    event="task launch failure",
                    extra=(
                        "Task was in running set and could not be queued "
                        f"after {attempt.total_tries} attempts."
                    ),
                    ti_key=key,
                )
                del self.attempts[key]
                del self.queued_tasks[key]
            else:
                if key in self.attempts:
                    del self.attempts[key]
                task_tuples.append((key, command, queue, ti.executor_config))
                if span.is_recording():
                    span.add_event(
                        name="task to trigger",
                        attributes={"command": str(command), "conf": str(ti.executor_config)},
                    )

        if task_tuples:
            self._process_tasks(task_tuples)

    @add_span
    def _process_tasks(self, task_tuples: list[TaskTuple]) -> None:
        for key, command, queue, executor_config in task_tuples:
            task_instance = self.queued_tasks[key][3]  # TaskInstance in fourth element
            trace_id = int(gen_trace_id(task_instance.dag_run, as_int=True))
            span_id = int(gen_span_id_from_ti_key(key, as_int=True))
            links = [{"trace_id": trace_id, "span_id": span_id}]

            # assuming that the span_id will very likely be unique inside the trace
            with Trace.start_span(
                span_name=f"{key.dag_id}.{key.task_id}",
                component="BaseExecutor",
                span_id=span_id,
                links=links,
            ) as span:
                span.set_attributes(
                    {
                        "dag_id": key.dag_id,
                        "run_id": key.run_id,
                        "task_id": key.task_id,
                        "try_number": key.try_number,
                        "command": str(command),
                        "queue": str(queue),
                        "executor_config": str(executor_config),
                    }
                )
                del self.queued_tasks[key]
                self.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)
                self.running.add(key)

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
            with Trace.start_span(
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
            with Trace.start_span(
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

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:  # pragma: no cover
        """
        Execute the command asynchronously.

        :param key: Unique key for the task instance
        :param command: Command to run
        :param queue: name of the queue
        :param executor_config: Configuration passed to the executor.
        """
        raise NotImplementedError()

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

    @deprecated(
        reason="Replaced by function `revoke_task`.",
        category=RemovedInAirflow3Warning,
        action="ignore",
    )
    def cleanup_stuck_queued_tasks(self, tis: list[TaskInstance]) -> list[str]:
        """
        Handle remnants of tasks that were failed because they were stuck in queued.

        Tasks can get stuck in queued. If such a task is detected, it will be marked
        as `UP_FOR_RETRY` if the task instance has remaining retries or marked as `FAILED`
        if it doesn't.

        :param tis: List of Task Instances to clean up
        :return: List of readable task instances for a warning message
        """
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
        if self.parallelism:
            return self.parallelism - len(self.running) - len(self.queued_tasks)
        else:
            return sys.maxsize

    @property
    def slots_occupied(self):
        """Number of tasks this executor instance is currently managing."""
        return len(self.running) + len(self.queued_tasks)

    @staticmethod
    def validate_airflow_tasks_run_command(command: list[str]) -> tuple[str | None, str | None]:
        """
        Check if the command to execute is airflow command.

        Returns tuple (dag_id,task_id) retrieved from the command (replaced with None values if missing)
        """
        if command[0:3] != ["airflow", "tasks", "run"]:
            raise ValueError('The command must start with ["airflow", "tasks", "run"].')
        if len(command) > 3 and "--help" not in command:
            dag_id: str | None = None
            task_id: str | None = None
            for arg in command[3:]:
                if not arg.startswith("--"):
                    if dag_id is None:
                        dag_id = arg
                    else:
                        task_id = arg
                        break
            return dag_id, task_id
        return None, None

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
