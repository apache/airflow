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

import contextlib
import multiprocessing
import os
import sys
from multiprocessing import Queue, SimpleQueue
from typing import TYPE_CHECKING

import structlog

from airflow.executors.base_executor import BaseExecutor, get_execution_api_server_url
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.types import state_class_for_key

# add logger to parameter of setproctitle to support logging
if sys.platform == "darwin":
    setproctitle = lambda title, logger: logger.debug("Mac OS detected, skipping setproctitle")
else:
    from setproctitle import setproctitle as real_setproctitle

    setproctitle = lambda title, logger: real_setproctitle(title)

if TYPE_CHECKING:
    from airflow.executors.workloads import ExecutorWorkload
    from airflow.executors.workloads.types import WorkloadKey, WorkloadState
    from airflow.models.taskinstance import TaskInstance

    LocalResult = tuple[int, WorkloadKey, WorkloadState | None, Exception | None]


def _get_executor_process_title_prefix(team_name: str | None) -> str:
    """
    Build the process title prefix for LocalExecutor workers.

    :param team_name: Team name from executor configuration
    """
    team_suffix = f" [{team_name}]" if team_name else ""
    return f"airflow worker -- LocalExecutor{team_suffix}:"


def _run_worker(
    logger_name: str,
    input: SimpleQueue[ExecutorWorkload | None],
    output: Queue[LocalResult],
    team_conf,
):
    import signal

    # Ignore ctrl-c in this process -- we don't want to kill _this_ one. we let tasks run to completion
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    log = structlog.get_logger(logger_name)
    log.info("Worker starting up pid=%d", os.getpid())

    while True:
        setproctitle(f"{_get_executor_process_title_prefix(team_conf.team_name)} <idle>", log)
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

        key = LocalExecutor.get_workload_key(workload)
        output.put((os.getpid(), key, workload.running_state, None))

        try:
            BaseExecutor.run_workload(
                workload,
                server=get_execution_api_server_url(team_conf),
                proctitle=f"{_get_executor_process_title_prefix(team_conf.team_name)} {workload.display_name}",
                subprocess_logs_to_stdout=True,
            )
            output.put((os.getpid(), key, workload.success_state, None))
        except Exception as e:
            log.exception("Workload execution failed.", workload_type=type(workload).__name__)
            output.put((os.getpid(), key, workload.failure_state, e))


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel.

    It uses the multiprocessing Python library and queues to parallelize the execution of tasks.

    :param parallelism: how many parallel processes are run in the executor, must be > 0
    """

    is_local: bool = True
    is_mp_using_fork: bool

    supports_multi_team: bool = True
    supports_task_instance_uuid = True
    serve_logs: bool = True
    # The connection-test supervisor uses ``signal.SIGALRM`` (via ``TimeoutPosix``) to bound hook
    # execution, so ``TEST_CONNECTION`` support requires a POSIX worker (LocalExecutor runs on the host).
    supported_workload_types: frozenset[WorkloadType] = frozenset(
        {WorkloadType.EXECUTE_TASK, WorkloadType.EXECUTE_CALLBACK, WorkloadType.TEST_CONNECTION}
    )

    result_queue: SimpleQueue[LocalResult]
    workers: dict[int, multiprocessing.Process]
    # One input queue per worker. A SimpleQueue read lock shared by all workers wedges the whole executor
    # if the worker holding it is SIGKILLed (e.g. by the OOM killer), so no two workers share a queue.
    _worker_queues: dict[int, SimpleQueue[ExecutorWorkload | None]]
    # Workers handed a workload that have not reported START yet. Kept apart from _worker_tasks so a worker
    # that dies before START still leaves the key for the scheduler's stuck-in-queued revoke, as before.
    _dispatched: dict[int, WorkloadKey]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._worker_tasks: dict[int, WorkloadKey] = {}
        self._dispatch_counts: dict[WorkloadKey, int] = {}
        self._worker_queues: dict[int, SimpleQueue[ExecutorWorkload | None]] = {}
        self._dispatched: dict[int, WorkloadKey] = {}

        # Resolve the start method at instantiation, not at import: the component CLI entry may have
        # set it via [<component>]/[core] mp_start_method before the executor is created.
        self.is_mp_using_fork = multiprocessing.get_start_method() == "fork"

        # Check if self has the ExecutorConf set on the self.conf attribute, and if not, set it to the global
        # configuration object. This allows the changes to be backwards compatible with older versions of
        # Airflow.
        # Can be removed when minimum supported provider version is equal to the version of core airflow
        # which introduces multi-team configuration.
        if not hasattr(self, "conf"):
            from airflow.configuration import conf

            self.conf = conf

    def start(self) -> None:
        """Start the executor."""
        # We delay opening these queues until the start method mostly for unit tests. ExecutorLoader caches
        # instances, so each test reusues the same instance! (i.e. test 1 runs, closes the queues, then test 2
        # comes back and gets the same LocalExecutor instance, so we have to open new here.)
        self.result_queue = SimpleQueue()
        self.workers = {}
        self._worker_queues = {}
        self._dispatched = {}
        self._worker_tasks.clear()
        self._dispatch_counts.clear()

        if self.is_mp_using_fork:
            # This creates the maximum number of worker processes (parallelism) at once
            # to minimize gc freeze/unfreeze cycles when using fork in multiprocessing
            self._spawn_workers_with_gc_freeze(self.parallelism)

    def _check_workers(self):
        self._read_results()
        # Reap any dead workers. Each worker owns its input queue, so a dead one cannot block the others.
        for pid, proc in list(self.workers.items()):
            if not proc.is_alive():
                # Drain what the worker wrote before dying (e.g. its START) so its key is attributed to it.
                self._read_results()
                self._dispatched.pop(pid, None)
                # A worker killed before START keeps its key in _dispatched; the scheduler's stuck-in-queued
                # revoke releases it, as before.
                if (key := self._worker_tasks.pop(pid, None)) is not None:
                    self._finish_dispatch(key, state_class_for_key(key).FAILED)
                proc.close()
                del self.workers[pid]
                del self._worker_queues[pid]

    def _pick_worker(self) -> int:
        self._check_workers()
        idle = next(
            (pid for pid in self.workers if pid not in self._worker_tasks and pid not in self._dispatched),
            None,
        )
        if idle is not None:
            return idle
        if len(self.workers) < self.parallelism:
            return self._spawn_worker()
        # ponytail: all workers busy yet a slot is free (a stale result not read yet); queue behind one.
        return next(iter(self.workers))

    def _spawn_worker(self) -> int:
        queue: SimpleQueue[ExecutorWorkload | None] = SimpleQueue()
        p = multiprocessing.Process(
            target=_run_worker,
            kwargs={
                "logger_name": self.log.name,
                "input": queue,
                "output": self.result_queue,
                "team_conf": self.conf,
            },
        )
        p.start()
        if TYPE_CHECKING:
            assert p.pid  # Since we've called start
        self.workers[p.pid] = p
        self._worker_queues[p.pid] = queue
        return p.pid

    def _spawn_workers_with_gc_freeze(self, spawn_number):
        """
        Freeze the GC before forking worker process and unfreeze it after forking.

        This is done to prevent memory increase due to COW (Copy-on-Write) by moving all
        existing objects to the permanent generation before forking the process. After forking,
        unfreeze is called to ensure there is no impact on gc operations
        in the original running process.

        Ref: https://docs.python.org/3/library/gc.html#gc.freeze
        """
        import gc

        gc.freeze()
        try:
            for _ in range(spawn_number):
                self._spawn_worker()
        finally:
            gc.unfreeze()

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        self._check_workers()

    def _read_results(self):
        try:
            while not self.result_queue.empty():
                pid, key, state, exc = self.result_queue.get()
                if pid not in self.workers:
                    continue
                if key not in self.running:
                    # Late result for a key already finished or revoked; a terminal one frees its worker.
                    if state is not None and state != "running" and self._dispatched.get(pid) == key:
                        del self._dispatched[pid]
                    continue
                if state is None or state == "running":
                    self._worker_tasks[pid] = key
                    self._dispatched.pop(pid, None)
                    if state is not None:
                        self.change_state(key, state, remove_running=False)
                elif self._worker_tasks.get(pid) == key or self._dispatched.get(pid) == key:
                    self._worker_tasks.pop(pid, None)
                    self._dispatched.pop(pid, None)
                    self._finish_dispatch(key, state)
        except (OSError, EOFError):
            self.log.exception("Error reading from result queue")

    def end(self) -> None:
        """End the executor."""
        self.log.info(
            "Shutting down LocalExecutor"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )

        # We can't tell which proc will pick which close message up, so we send all the messages, and then
        # wait on all the procs

        for pid, proc in self.workers.items():
            # Send the shutdown message to each alive worker's own queue
            if proc.is_alive():
                self._worker_queues[pid].put(None)

        # To prevent deadlock, we should consume results from result_queue while waiting for processes to join.
        # Otherwise, a worker blocked on putting results into a full result_queue pipe will never exit,
        # and an unbounded proc.join() will hang the scheduler indefinitely.
        try:
            for proc in self.workers.values():
                while proc.is_alive():
                    self._read_results()
                    proc.join(timeout=0.05)
        except (KeyboardInterrupt, SystemExit):
            self.log.error("KeyboardInterrupt received during shutdown. Force terminating workers.")
            for proc in self.workers.values():
                self._terminate_worker_process(proc)
            raise
        finally:
            # Process any extra results before closing
            self._read_results()

            for proc in self.workers.values():
                with contextlib.suppress(ValueError):
                    proc.close()

            for queue in self._worker_queues.values():
                queue.close()
            self.result_queue.close()

    def terminate(self):
        """Terminate all worker processes under control of the executor forcefully."""
        self.log.info("Terminating all LocalExecutor worker processes.")
        for proc in self.workers.values():
            self._terminate_worker_process(proc)

    def _terminate_worker_process(self, proc: multiprocessing.Process) -> None:
        """Terminate a worker process, escalating to kill if it stays alive."""
        if not proc.is_alive():
            return

        proc.terminate()
        proc.join(timeout=0.2)
        if proc.is_alive():
            self.log.warning("Worker process %s did not stop after SIGTERM. Sending SIGKILL.", proc.pid)
            proc.kill()
            proc.join(timeout=0.2)

    def _process_workloads(self, workload_list):
        for workload in workload_list:
            key = self.get_workload_key(workload)
            pid = self._pick_worker()
            self._worker_queues[pid].put(workload)
            self._dispatched[pid] = key
            removed = self.executor_queues[workload.type].pop(key, None)
            if not removed:
                raise KeyError(f"Workload {key} was not found in any queue")
            self.running.add(key)
            self._dispatch_counts[key] = self._dispatch_counts.get(key, 0) + 1

    def _finish_dispatch(self, key: WorkloadKey, state: WorkloadState) -> None:
        # A resumed attempt reuses its key, so the previous dispatch can finish while the next one is live.
        remaining = self._dispatch_counts.pop(key, 1) - 1
        if remaining > 0:
            self._dispatch_counts[key] = remaining
        super().change_state(key, state, remove_running=remaining <= 0)

    def _forget_workload(self, key: WorkloadKey) -> None:
        self._dispatch_counts.pop(key, None)
        self._worker_tasks = {
            pid: task_key for pid, task_key in self._worker_tasks.items() if task_key != key
        }
        self._dispatched = {pid: task_key for pid, task_key in self._dispatched.items() if task_key != key}

    def change_state(self, key, state, info=None, remove_running=True) -> None:
        if remove_running:
            self._forget_workload(key)
        super().change_state(key, state, info=info, remove_running=remove_running)

    def fail_connection_test(self, key) -> None:
        self._forget_workload(key)
        super().fail_connection_test(key)

    def revoke_task(self, *, ti: TaskInstance) -> None:
        key = self.get_task_key(ti)
        self.executor_queues[WorkloadType.EXECUTE_TASK].pop(key, None)
        for pid, task_key in self._worker_tasks.items():
            if task_key == key:
                self._terminate_worker_process(self.workers[pid])
        self._forget_workload(key)
        self.running.discard(key)
