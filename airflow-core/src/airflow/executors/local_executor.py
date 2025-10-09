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

import ctypes
import logging
import multiprocessing
import multiprocessing.sharedctypes
import os
import sys
from multiprocessing import Queue, SimpleQueue
from typing import TYPE_CHECKING

from airflow.executors import workloads
from airflow.executors.base_executor import PARALLELISM, BaseExecutor
from airflow.utils.state import TaskInstanceState

# add logger to parameter of setproctitle to support logging
if sys.platform == "darwin":
    setproctitle = lambda title, logger: logger.debug("Mac OS detected, skipping setproctitle")
else:
    from setproctitle import setproctitle as real_setproctitle

    setproctitle = lambda title, logger: real_setproctitle(title)

if TYPE_CHECKING:
    TaskInstanceStateType = tuple[workloads.TaskInstance, TaskInstanceState, Exception | None]


def _run_worker(
    logger_name: str,
    input: SimpleQueue[workloads.All | None],
    output: Queue[TaskInstanceStateType],
    unread_messages: multiprocessing.sharedctypes.Synchronized[int],
):
    import signal

    # Ignore ctrl-c in this process -- we don't want to kill _this_ one. we let tasks run to completion
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    log = logging.getLogger(logger_name)
    log.info("Worker starting up pid=%d", os.getpid())

    while True:
        setproctitle("airflow worker -- LocalExecutor: <idle>", log)
        try:
            workload = input.get()
        except EOFError:
            log.info(
                "Failed to read tasks from the task queue because the other "
                "end has closed the connection. Terminating worker %s.",
                multiprocessing.current_process().name,
            )
            break

        if workload is None:
            # Received poison pill, no more tasks to run
            return

        if not isinstance(workload, workloads.ExecuteTask):
            raise ValueError(f"LocalExecutor does not know how to handle {type(workload)}")

        # Decrement this as soon as we pick up a message off the queue
        with unread_messages:
            unread_messages.value -= 1
        key = None
        if ti := getattr(workload, "ti", None):
            key = ti.key
        else:
            raise TypeError(f"Don't know how to get ti key from {type(workload).__name__}")

        try:
            _execute_work(log, workload)

            output.put((key, TaskInstanceState.SUCCESS, None))
        except Exception as e:
            log.exception("uhoh")
            output.put((key, TaskInstanceState.FAILED, e))


def _execute_work(log: logging.Logger, workload: workloads.ExecuteTask) -> None:
    """
    Execute command received and stores result state in queue.

    :param key: the key to identify the task instance
    :param command: the command to execute
    """
    from airflow.configuration import conf
    from airflow.sdk.execution_time.supervisor import supervise

    setproctitle(f"airflow worker -- LocalExecutor: {workload.ti.id}", log)

    base_url = conf.get("api", "base_url", fallback="/")
    # If it's a relative URL, use localhost:8080 as the default
    if base_url.startswith("/"):
        base_url = f"http://localhost:8080{base_url}"
    default_execution_api_server = f"{base_url.rstrip('/')}/execution/"

    # This will return the exit code of the task process, but we don't care about that, just if the
    # _supervisor_ had an error reporting the state back (which will result in an exception.)
    supervise(
        # This is the "wrong" ti type, but it duck types the same. TODO: Create a protocol for this.
        ti=workload.ti,  # type: ignore[arg-type]
        dag_rel_path=workload.dag_rel_path,
        bundle_info=workload.bundle_info,
        token=workload.token,
        server=conf.get("core", "execution_api_server_url", fallback=default_execution_api_server),
        log_path=workload.log_path,
    )


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel.

    It uses the multiprocessing Python library and queues to parallelize the execution of tasks.

    :param parallelism: how many parallel processes are run in the executor
    """

    is_local: bool = True

    serve_logs: bool = True

    activity_queue: SimpleQueue[workloads.All | None]
    result_queue: SimpleQueue[TaskInstanceStateType]
    workers: dict[int, multiprocessing.Process]
    _unread_messages: multiprocessing.sharedctypes.Synchronized[int]

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

        # Mypy sees this value as `SynchronizedBase[c_uint]`, but that isn't the right runtime type behaviour
        # (it looks like an int to python)
        self._unread_messages = multiprocessing.Value(ctypes.c_uint)

    def _check_workers(self):
        # Reap any dead workers
        to_remove = set()
        for pid, proc in self.workers.items():
            if not proc.is_alive():
                to_remove.add(pid)
                proc.close()

        if to_remove:
            self.workers = {pid: proc for pid, proc in self.workers.items() if pid not in to_remove}

        with self._unread_messages:
            num_outstanding = self._unread_messages.value

        if num_outstanding <= 0 or self.activity_queue.empty():
            # Nothing to do. Future enhancement if someone wants: shut down workers that have been idle for N
            # seconds
            return

        # If we're using spawn in multiprocessing (default on macOS now) to start tasks, this can get called a
        # via `sync()` a few times before the spawned process actually starts picking up messages. Try not to
        # create too much
        if num_outstanding and (self.parallelism == 0 or len(self.workers) < self.parallelism):
            # This only creates one worker, which is fine as we call this directly after putting a message on
            # activity_queue in execute_async
            self._spawn_worker()

    def _spawn_worker(self):
        p = multiprocessing.Process(
            target=_run_worker,
            kwargs={
                "logger_name": self.log.name,
                "input": self.activity_queue,
                "output": self.result_queue,
                "unread_messages": self._unread_messages,
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

    def _process_workloads(self, workloads):
        for workload in workloads:
            self.activity_queue.put(workload)
            del self.queued_tasks[workload.ti.key]
        with self._unread_messages:
            self._unread_messages.value += len(workloads)
        self._check_workers()
