#
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
"""
LocalExecutor.

.. seealso::
    For more information on how the LocalExecutor works, take a look at the guide:
    :ref:`executor:LocalExecutor`
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import subprocess
from multiprocessing import Queue, SimpleQueue
from typing import TYPE_CHECKING, Any, Optional, Tuple

from airflow import settings
from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from airflow.traces.tracer import add_span
from airflow.utils.dag_parsing_context import _airflow_parsing_context_manager
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.executors.base_executor import CommandType
    from airflow.models.taskinstancekey import TaskInstanceKey

    # This is a work to be executed by a worker.
    # It can Key and Command - but it can also be None, None which is actually a
    # "Poison Pill" - worker seeing Poison Pill should take the pill and ... die instantly.
    ExecutorWorkType = Optional[Tuple[TaskInstanceKey, CommandType]]
    TaskInstanceStateType = Tuple[TaskInstanceKey, TaskInstanceState, Optional[Exception]]


def _run_worker(logger_name: str, input: SimpleQueue[ExecutorWorkType], output: Queue[TaskInstanceStateType]):
    import signal

    from setproctitle import setproctitle

    # Ignore ctrl-c in this process -- we don't want to kill _this_ one. we let tasks run to completion
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    log = logging.getLogger(logger_name)

    # We know we've just started a new process, so lets disconnect from the metadata db now
    settings.engine.pool.dispose()
    settings.engine.dispose()

    setproctitle("airflow worker -- LocalExecutor: <idle>")

    while True:
        try:
            item = input.get()
        except EOFError:
            log.info(
                "Failed to read tasks from the task queue because the other "
                "end has closed the connection. Terminating worker %s.",
                multiprocessing.current_process().name,
            )
            break

        if item is None:
            # Received poison pill, no more tasks to run
            return

        (key, command) = item
        try:
            state = _execute_work(log, key, command)

            output.put((key, state, None))
        except Exception as e:
            output.put((key, TaskInstanceState.FAILED, e))


def _execute_work(log: logging.Logger, key: TaskInstanceKey, command: CommandType) -> TaskInstanceState:
    """
    Execute command received and stores result state in queue.

    :param key: the key to identify the task instance
    :param command: the command to execute
    """
    from setproctitle import setproctitle

    setproctitle(f"airflow worker -- LocalExecutor: {command}")
    dag_id, task_id = BaseExecutor.validate_airflow_tasks_run_command(command)
    try:
        with _airflow_parsing_context_manager(dag_id=dag_id, task_id=task_id):
            if settings.EXECUTE_TASKS_NEW_PYTHON_INTERPRETER:
                return _execute_work_in_subprocess(log, command)
            else:
                return _execute_work_in_fork(log, command)
    finally:
        # Remove the command since the worker is done executing the task
        setproctitle("airflow worker -- LocalExecutor: <idle>")


def _execute_work_in_subprocess(log: logging.Logger, command: CommandType) -> TaskInstanceState:
    try:
        subprocess.check_call(command, close_fds=True)
        return TaskInstanceState.SUCCESS
    except subprocess.CalledProcessError as e:
        log.error("Failed to execute task %s.", e)
        return TaskInstanceState.FAILED


def _execute_work_in_fork(log: logging.Logger, command: CommandType) -> TaskInstanceState:
    pid = os.fork()
    if pid:
        # In parent, wait for the child
        pid, ret = os.waitpid(pid, 0)
        return TaskInstanceState.SUCCESS if ret == 0 else TaskInstanceState.FAILED

    from airflow.sentry import Sentry

    ret = 1
    try:
        import signal

        from setproctitle import setproctitle

        from airflow.cli.cli_parser import get_parser

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGUSR2, signal.SIG_DFL)

        parser = get_parser()
        # [1:] - remove "airflow" from the start of the command
        args = parser.parse_args(command[1:])
        args.shut_down_logging = False

        setproctitle(f"airflow task supervisor: {command}")

        args.func(args)
        ret = 0
        return TaskInstanceState.SUCCESS
    except Exception as e:
        log.exception("Failed to execute task %s.", e)
        return TaskInstanceState.FAILED
    finally:
        Sentry.flush()
        logging.shutdown()
        os._exit(ret)


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel.

    It uses the multiprocessing Python library and queues to parallelize the execution of tasks.

    :param parallelism: how many parallel processes are run in the executor
    """

    is_local: bool = True

    serve_logs: bool = True

    activity_queue: SimpleQueue[ExecutorWorkType]
    result_queue: SimpleQueue[TaskInstanceStateType]
    workers: dict[int, multiprocessing.Process]
    _outstanding_messages: int = 0

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        if self.parallelism < 0:
            raise ValueError("parallelism must be greater than or equal to 0")

    def start(self) -> None:
        """Start the executor."""
        # We delay opening these queues until the start method mostly for unit tests. ExecutorLoader caches
        # instances, so each test reusues the same instance! (i.e. test 1 runs, closes the queues, then test 2
        # comes back and gets the same LocalExecutor instance, so we have to open new here.)
        self.activity_queue = SimpleQueue()
        self.result_queue = SimpleQueue()
        self.workers = {}
        self._outstanding_messages = 0

    @add_span
    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ) -> None:
        """Execute asynchronously."""
        self.validate_airflow_tasks_run_command(command)
        self.activity_queue.put((key, command))
        self._outstanding_messages += 1
        self._check_workers(can_start=True)

    def _check_workers(self, can_start: bool = True):
        # Reap any dead workers
        to_remove = set()
        for pid, proc in self.workers.items():
            if not proc.is_alive():
                to_remove.add(pid)
                proc.close()

        if to_remove:
            self.workers = {pid: proc for pid, proc in self.workers.items() if pid not in to_remove}

        # If we're using spawn in multiprocessing (default on macos now) to start tasks, this can get called a
        # via sync() a few times before the spawned process actually starts picking up messages. Try not to
        # create too much

        if self._outstanding_messages <= 0 or self.activity_queue.empty():
            # Nothing to do, should we shut down idle workers?
            return

        need_more_workers = len(self.workers) < self._outstanding_messages
        if need_more_workers and (self.parallelism == 0 or len(self.workers) < self.parallelism):
            self._spawn_worker()

    def _spawn_worker(self):
        p = multiprocessing.Process(
            target=_run_worker,
            kwargs={
                "logger_name": self.log.name,
                "input": self.activity_queue,
                "output": self.result_queue,
            },
        )
        p.start()
        if TYPE_CHECKING:
            assert p.pid  # Since we've called start
        self.workers[p.pid] = p

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        self._read_results()
        self._check_workers()

    def _read_results(self):
        while not self.result_queue.empty():
            key, state, exc = self.result_queue.get()
            self._outstanding_messages = self._outstanding_messages - 1

            if exc:
                # TODO: This needs a better stacktrace, it appears from here
                if hasattr(exc, "add_note"):
                    exc.add_note("(This stacktrace is incorrect -- the exception came from a subprocess)")
                raise exc

            self.change_state(key, state)

    def end(self) -> None:
        """End the executor."""
        self.log.info(
            "Shutting down LocalExecutor"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )

        # We can't tell which proc will pick which close message up, so we send all the messages, and then
        # wait on all the procs

        for proc in self.workers.values():
            # Send the shutdown message once for each alive worker
            if proc.is_alive():
                self.activity_queue.put(None)

        for proc in self.workers.values():
            if proc.is_alive():
                proc.join()
            proc.close()

        # Process any extra results before closing
        self._read_results()

        self.activity_queue.close()
        self.result_queue.close()

    def terminate(self):
        """Terminate the executor is not doing anything."""
