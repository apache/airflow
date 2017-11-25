# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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

import multiprocessing
import subprocess
import time

from builtins import range

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State


class LocalWorker(multiprocessing.Process, LoggingMixin):

    """LocalWorker Process implementation to run airflow commands. Executes the given
    command and puts the result into a result queue when done, terminating execution."""

    def __init__(self, result_queue):
        """
        :param result_queue: the queue to store result states tuples (key, State)
        :type result_queue: multiprocessing.Queue
        """
        super(LocalWorker, self).__init__()
        self.daemon = True
        self.result_queue = result_queue
        self.key = None
        self.command = None

    def execute_work(self, key, command):
        """
        Executes command received and stores result state in queue.
        :param key: the key to identify the TI
        :type key: Tuple(dag_id, task_id, execution_date)
        :param command: the command to execute
        :type command: string
        """
        if key is None:
            return
        self.log.info("%s running %s", self.__class__.__name__, command)
        command = "exec bash -c '{0}'".format(command)
        try:
            subprocess.check_call(command, shell=True, close_fds=True)
            state = State.SUCCESS
        except subprocess.CalledProcessError as e:
            state = State.FAILED
            self.log.error("Failed to execute task %s.", str(e))
            # TODO: Why is this commented out?
            # raise e
        self.result_queue.put((key, state))

    def run(self):
        self.execute_work(self.key, self.command)
        time.sleep(1)


class QueuedLocalWorker(LocalWorker):

    """LocalWorker implementation that is waiting for tasks from a queue and will
    continue executing commands as they become available in the queue. It will terminate
    execution once the poison token is found."""

    def __init__(self, task_queue, result_queue):
        super(QueuedLocalWorker, self).__init__(result_queue=result_queue)
        self.task_queue = task_queue

    def run(self):
        while True:
            key, command = self.task_queue.get()
            if key is None:
                # Received poison pill, no more tasks to run
                self.task_queue.task_done()
                break
            self.execute_work(key, command)
            self.task_queue.task_done()
            time.sleep(1)


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """

    class _UnlimitedParallelism(object):
        """Implements LocalExecutor with unlimited parallelism, starting one process
        per each command to execute."""

        def __init__(self, executor):
            """
            :param executor: the executor instance to implement.
            :type executor: LocalExecutor
            """
            self.executor = executor

        def start(self):
            self.executor.workers_used = 0
            self.executor.workers_active = 0

        def execute_async(self, key, command):
            """
            :param key: the key to identify the TI
            :type key: Tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: string
            """
            local_worker = LocalWorker(self.executor.result_queue)
            local_worker.key = key
            local_worker.command = command
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

        def sync(self):
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 1

        def end(self):
            while self.executor.workers_active > 0:
                self.executor.sync()
                time.sleep(0.5)

    class _LimitedParallelism(object):
        """Implements LocalExecutor with limited parallelism using a task queue to
        coordinate work distribution."""

        def __init__(self, executor):
            self.executor = executor

        def start(self):
            self.executor.queue = multiprocessing.JoinableQueue()

            self.executor.workers = [
                QueuedLocalWorker(self.executor.queue, self.executor.result_queue)
                for _ in range(self.executor.parallelism)
            ]

            self.executor.workers_used = len(self.executor.workers)

            for w in self.executor.workers:
                w.start()

        def execute_async(self, key, command):
            """
            :param key: the key to identify the TI
            :type key: Tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: string
            """
            self.executor.queue.put((key, command))

        def sync(self):
            while not self.executor.result_queue.empty():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)

        def end(self):
            # Sending poison pill to all worker
            for _ in self.executor.workers:
                self.executor.queue.put((None, None))

            # Wait for commands to finish
            self.executor.queue.join()
            self.executor.sync()

    def start(self):
        self.result_queue = multiprocessing.Queue()
        self.queue = None
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = (LocalExecutor._UnlimitedParallelism(self) if self.parallelism == 0
                     else LocalExecutor._LimitedParallelism(self))

        self.impl.start()

    def execute_async(self, key, command, queue=None):
        self.impl.execute_async(key=key, command=command)

    def sync(self):
        self.impl.sync()

    def end(self):
        self.impl.end()
