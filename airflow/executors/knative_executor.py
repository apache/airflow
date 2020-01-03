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
This module contains KnativeExecutor which is optimized
for running lots of short tasks in a scalable, efficient manner.
"""

import asyncio
import datetime
import functools
import multiprocessing

import aiohttp
from cached_property import cached_property

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.kubernetes_executor import KubernetesExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State


async def make_request_async(
    task_id,
    dag_id,
    execution_date,
    host,
    log,
    host_header=None,
) -> aiohttp.ClientResponse:
    """

    Thius function crafts a request to an external airflow server. This server is assumed to be
    a knative service.
    @param task_id:
    @param dag_id:
    @param execution_date:
    @param host:
    @param log:
    @param host_header:
    @return:
    """
    req = "http://" + host + "/run"

    date = int(datetime.datetime.timestamp(execution_date))
    params = {
        "task_id": task_id,
        "dag_id": dag_id,
        "execution_date": date,
    }
    headers = {}
    if host_header:
        headers["Host"] = host_header
    timeout = aiohttp.ClientTimeout(total=60000)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url=req, params=params, headers=headers) as resp:
            log.info(resp.status)
            log.info(await resp.text())
            return resp


class KnativeRequestLoop(multiprocessing.Process, LoggingMixin):
    """

    This class asynchronously pulls tasks from the KnativeExecutor and runs them as coroutines using asyncio

    """
    def __init__(self,
                 task_pipe,
                 result_pipe,
                 host_header,
                 host,
                 ):

        super().__init__()
        self.host = host
        self.task_pipe = task_pipe
        self.result_pipe = result_pipe
        self.host_header = host_header

    def recieve_and_execute(self, loop):
        """
        This function receives tasks from task_pipe and executes them in FIFO order.
        @param loop:
        """
        key = self.task_pipe.recv()
        loop.create_task(self.execute_work(key=key))

    def run(self):
        """
        Starts loop and prepares pipe reader.
        """
        loop = asyncio.get_event_loop()
        loop.add_reader(fd=self.task_pipe, callback=functools.partial(self.recieve_and_execute, loop))
        loop.run_forever()

    async def execute_work(self, key):
        """
        Executes command received and stores result state in queue.
        :param task_instance:
        :param key: the key to identify the TI
        :type key: tuple(dag_id, task_id, execution_date)
        :param command: the command to execute
        :type command: str
        """
        if key is None:
            return
        self.log.info("%s running %s", self.__class__.__name__, key)
        (dag_id, task_id, execution_date, _) = key
        try:
            future = make_request_async(
                task_id,
                dag_id,
                execution_date,
                host=self.host, log=self.log, host_header=self.host_header)
            resp = await future
            if resp.status != 200:
                state = State.FAILED
                self.log.error("Failed to execute task %s.", str(resp.text))
                self.result_pipe.send((key, state))
            else:
                self.log.info("assuming final state has been handled")
                self.result_pipe.send((key, None))

        except asyncio.InvalidStateError as e:
            state = State.FAILED
            self.log.error("Failed to execute task %s.", str(e))
            self.result_pipe.send((key, state))


class KnativeExecutor(BaseExecutor):
    """
    KnativeExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """
    task_queue = None
    result_queue = None
    result_pipe = None

    def terminate(self):
        pass

    @cached_property
    def kube_executor(self) -> KubernetesExecutor:
        """
        lazily loads KubernetesExecutor
        @return:
        """
        self.kube_executor_initialized = True
        return KubernetesExecutor()

    def __init__(self):
        self.kube_executor_initialized = False
        super().__init__()
        self.task_pipe = None
        self.local_loop = None

    def start(self):
        req = conf.get("knative", "knative_host")
        try:
            host_header = conf.get("knative", "knative_host_header")
        except AirflowConfigException:
            host_header = None
        if req is None:
            raise AirflowException("you must set a knative host")

        self.result_pipe, child_result_pipe = multiprocessing.Pipe()
        self.task_pipe, child_task_pipe = multiprocessing.Pipe()

        self.local_loop = KnativeRequestLoop(
            task_pipe=child_task_pipe,
            result_pipe=child_result_pipe,
            host=req,
            host_header=host_header,
        )
        self.local_loop.start()

    def execute_async(self, key, command, queue=None, executor_config=None):
        if queue == "kubernetes":
            self.kube_executor.execute_async(key, command, queue, executor_config)
        else:
            self.task_pipe.send(key)

    def sync(self):
        while self.result_pipe.poll():
            results = self.result_pipe.recv()
            self.change_state(*results)
        if self.kube_executor_initialized:
            self.kube_executor.sync()

    def end(self):
        self.sync()
