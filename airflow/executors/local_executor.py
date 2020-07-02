# -*- coding: utf-8 -*-
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
LocalExecutor

.. seealso::
    For more information on how the LocalExecutor works, take a look at the guide:
    :ref:`executor:LocalExecutor`
"""

import multiprocessing
import subprocess

from builtins import range
from queue import Empty

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
        :type key: tuple(dag_id, task_id, execution_date)
        :param command: the command to execute
        :type command: str
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
            # TODO: Why is this commented out?
            # raise e
        self.result_queue.put((key, state))

    def run(self):
        self.execute_work(self.key, self.command)


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
            try:
                if key is None:
                    # Received poison pill, no more tasks to run
                    break
                self.execute_work(key, command)
            finally:
                self.task_queue.task_done()


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
            :type key: tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: str
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

    class _LimitedParallelism(object):
        """Implements LocalExecutor with limited parallelism using a task queue to
        coordinate work distribution."""

        def __init__(self, executor):
            self.executor = executor

        def start(self):
            self.queue = self.executor.manager.Queue()
            self.executor.workers = [
                QueuedLocalWorker(self.queue, self.executor.result_queue)
                for _ in range(self.executor.parallelism)
            ]

            self.executor.workers_used = len(self.executor.workers)

            for w in self.executor.workers:
                w.start()

        def execute_async(self, key, command):
            """
            :param key: the key to identify the TI
            :type key: tuple(dag_id, task_id, execution_date)
            :param command: the command to execute
            :type command: str
            """
            self.queue.put((key, command))

        def sync(self):
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
            # Sending poison pill to all worker
            for _ in self.executor.workers:
                self.queue.put((None, None))

            # Wait for commands to finish
            self.queue.join()
            self.executor.sync()

    def start(self):
        self.manager = multiprocessing.Manager()
        self.result_queue = self.manager.Queue()
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = (LocalExecutor._UnlimitedParallelism(self) if self.parallelism == 0
                     else LocalExecutor._LimitedParallelism(self))

        self.impl.start()

    def execute_async(self, key, command, queue=None, executor_config=None):
        if command[0:2] != ["airflow", "run"]:
            raise ValueError('The command must start with ["airflow", "run"].')
        self.impl.execute_async(key=key, command=command)

    def sync(self):
        self.impl.sync()

    def end(self):
        self.impl.end()
        self.manager.shutdown()
