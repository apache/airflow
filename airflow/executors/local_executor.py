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
LocalExecutor runs tasks by spawning processes in a controlled fashion in different
modes. Given that BaseExecutor has the option to receive a `parallelism` parameter to
limit the number of process spawned, when this parameter is `0` the number of processes
that LocalExecutor can spawn is unlimited.

The following strategies are implemented:
1. Unlimited Parallelism (self.parallelism == 0): In this strategy, LocalExecutor will
spawn a process every time `execute_async` is called, that is, every task submitted to the
LocalExecutor will be executed in its own process. Once the task is executed and the
result stored in the `result_queue`, the process terminates. There is no need for a
`task_queue` in this approach, since as soon as a task is received a new process will be
allocated to the task. Processes used in this strategy are of class LocalWorker.

2. Limited Parallelism (self.parallelism > 0): In this strategy, the LocalExecutor spawns
the number of processes equal to the value of `self.parallelism` at `start` time,
using a `task_queue` to coordinate the ingestion of tasks and the work distribution among
the workers, which will take a task as soon as they are ready. During the lifecycle of
the LocalExecutor, the worker processes are running waiting for tasks, once the
LocalExecutor receives the call to shutdown the executor a poison token is sent to the
workers to terminate them. Processes used in this strategy are of class QueuedLocalWorker.

Arguably, `SequentialExecutor` could be thought as a LocalExecutor with limited
parallelism of just 1 worker, i.e. `self.parallelism = 1`.
This option could lead to the unification of the executor implementations, running
locally, into just one `LocalExecutor` with multiple modes.
"""
import subprocess
from multiprocessing import Manager, Process, Queue as MultiprocessingQueue
from multiprocessing.managers import SyncManager
from queue import Empty, Queue  # pylint: disable=unused-import  # noqa: F401
from typing import List, Optional, Tuple, Union  # pylint: disable=unused-import # noqa: F401

from airflow import AirflowException
from airflow.executors.base_executor import PARALLELISM, BaseExecutor, CommandType
from airflow.models.taskinstance import (  # pylint: disable=unused-import # noqa: F401
    TaskInstanceKey, TaskInstanceState,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State

# This is a work to be executed by a worker.
# It can Key and Command - but it can also be None, None which is actually a
# "Poison Pill" - worker seeing Poison Pill should take the pill and ... die instantly.
ExecutorWorkTodo = Tuple[Optional[TaskInstanceKey], Optional[CommandType]]


class LocalWorkerBase(Process, LoggingMixin):
    """
    LocalWorker Process implementation to run airflow commands. Executes the given
    command and puts the result into a result queue when done, terminating execution.

    :param result_queue: the queue to store result state
    """
    def __init__(self, result_queue: 'MultiprocessingQueue[TaskInstanceState]'):
        super().__init__()
        self.daemon: bool = True
        self.result_queue: 'MultiprocessingQueue[TaskInstanceState]' = result_queue

    def execute_work(self, key: TaskInstanceKey, command: CommandType):
        """
        Executes command received and stores result state in queue.

        :param key: the key to identify the task instance
        :param command: the command to execute
        """
        if key is None:
            return
        self.log.info("%s running %s", self.__class__.__name__, command)
        try:
            subprocess.check_call(command, close_fds=True)
            state = State.SUCCESS
        except subprocess.CalledProcessError as e:
            state = State.FAILED
            self.log.error("Failed to execute task %s.", str(e))
        self.result_queue.put((key, state))


class LocalWorker(LocalWorkerBase):
    """
    Local worker that executes the task.

    :param result_queue: queue wehere results of the tasks are put.
    :param key: key identifying task instance
    :param command: Command to execute
    """
    def __init__(self,
                 result_queue: 'MultiprocessingQueue[TaskInstanceState]',
                 key: TaskInstanceKey,
                 command: CommandType):
        super().__init__(result_queue)
        self.key: TaskInstanceKey = key
        self.command: CommandType = command

    def run(self) -> None:
        self.execute_work(key=self.key, command=self.command)


class QueuedLocalWorker(LocalWorkerBase):
    """
    LocalWorker implementation that is waiting for tasks from a queue and will
    continue executing commands as they become available in the queue.
    It will terminate execution once the poison token is found.

    :param task_queue: queue from which worker reads tasks
    :param result_queue: queue where worker puts results after finishing tasks
    """
    def __init__(self,
                 task_queue: 'MultiprocessingQueue[TaskInstanceState]',
                 result_queue: 'MultiprocessingQueue[TaskInstanceState]'):
        super().__init__(result_queue=result_queue)
        self.task_queue: MultiprocessingQueue = task_queue

    def run(self):
        while True:
            key, command = self.task_queue.get()
            try:
                if key is None:
                    # Received poison pill, no more tasks to run
                    break
                self.execute_work(key=key, command=command)
            finally:
                self.task_queue.task_done()


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel.
    It uses the multiprocessing Python library and queues to parallelize the execution
    of tasks.

    :param parallelism: how many parallel processes are run in the executor
    """
    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)
        self.manager: Optional[SyncManager] = None
        self.result_queue: Optional['MultiprocessingQueue[TaskInstanceState]'] = None
        self.workers: List[QueuedLocalWorker] = []
        self.workers_used: int = 0
        self.workers_active: int = 0
        self.impl: Optional[Union['LocalExecutor.UnlimitedParallelism',
                                  'LocalExecutor.LimitedParallelism']] = None

    class UnlimitedParallelism:
        """
        Implements LocalExecutor with unlimited parallelism, starting one process
        per each command to execute.

        :param executor: the executor instance to implement.
        """
        def __init__(self, executor: 'LocalExecutor'):
            self.executor: 'LocalExecutor' = executor

        def start(self):
            """Starts the executor."""
            self.executor.workers_used = 0
            self.executor.workers_active = 0

        def execute_async(self,
                          key: TaskInstanceKey,
                          command: CommandType):
            """
            Executes task asynchronously.

            :param key: the key to identify the task instance
            :param command: the command to execute
            """
            if not self.executor.result_queue:
                raise AirflowException("Executor should be started first")
            local_worker = LocalWorker(self.executor.result_queue, key=key, command=command)
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

        def sync(self):
            """
            Sync will get called periodically by the heartbeat method.
            """
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 1

        def end(self):
            """
            This method is called when the caller is done submitting job and
            wants to wait synchronously for the job submitted previously to be
            all done.
            """
            while self.executor.workers_active > 0:
                self.executor.sync()

    class LimitedParallelism:
        """
        Implements LocalExecutor with limited parallelism using a task queue to
        coordinate work distribution.

        :param executor: the executor instance to implement.
        """
        def __init__(self, executor: 'LocalExecutor'):
            self.executor: 'LocalExecutor' = executor
            self.queue: Optional['Queue[ExecutorWorkTodo]'] = None

        def start(self):
            """Starts limited parallelism implementation."""
            self.queue = self.executor.manager.Queue()
            self.executor.workers = [
                QueuedLocalWorker(self.queue, self.executor.result_queue)
                for _ in range(self.executor.parallelism)
            ]

            self.executor.workers_used = len(self.executor.workers)

            for worker in self.executor.workers:
                worker.start()

        def execute_async(self,
                          key: TaskInstanceKey,
                          command: CommandType):
            """
            Executes task asynchronously.

            :param key: the key to identify the task instance
            :param command: the command to execute
            """
            if not self.queue:
                raise AirflowException("The executor has not been started.")
            self.queue.put((key, command))

        def sync(self):
            """
            Sync will get called periodically by the heartbeat method.
            """
            while True:
                try:
                    results = self.executor.result_queue.get_nowait()
                    try:
                        self.executor.change_state(*results)
                    finally:
                        self.executor.result_queue.task_done()
                except Empty:
                    break

        def end(self):
            """Ends the executor. Sends the poison pill to all workers."""
            for _ in self.executor.workers:
                self.queue.put((None, None))

            # Wait for commands to finish
            self.queue.join()
            self.executor.sync()

    def start(self):
        """Starts the executor"""
        self.manager = Manager()
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = (LocalExecutor.UnlimitedParallelism(self) if self.parallelism == 0
                     else LocalExecutor.LimitedParallelism(self))

        self.impl.start()

    def execute_async(self, key: TaskInstanceKey,
                      command: CommandType,
                      _=None):
        """Execute asynchronously."""
        if not self.impl:
            raise AirflowException("The executor should be started first.")
        self.impl.execute_async(key=key, command=command)

    def sync(self):
        """
        Sync will get called periodically by the heartbeat method.
        """
        self.impl.sync()

    def end(self):
        """
        Ends the executor.
        :return:
        """
        self.impl.end()
        self.manager.shutdown()
