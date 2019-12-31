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
"""Dask asynchronous executor."""
import asyncio
import random
from typing import Any, Dict, Optional

from distributed import Client, Future
from distributed.security import Security

from airflow import AirflowException
from airflow.configuration import conf
from airflow.executors.base_executor import NOT_STARTED_MESSAGE, BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKeyType


class DaskAsyncExecutor(BaseExecutor):
    """
    DaskAsyncExecutor runs an async event loop to manage async tasks submitted to a Dask
    Distributed cluster. The Dask client is an asynchronous client, based on Tornado
    coroutines with compatibility for asyncio.  Tasks sent to this executor are assumed to
    support async/await behavior to yield for blocking IO. This executor is not suited
    for CPU-bounds tasks.

    .. seealso::
        - https://distributed.dask.org/en/latest/asynchronous.html
    """

    def __init__(self, cluster_address=None):
        super().__init__(parallelism=0)  # TODO: what does 'parallelism=0' mean?
        if cluster_address is None:
            cluster_address = conf.get("dask", "cluster_address")
        if not cluster_address:
            raise ValueError("Please provide a Dask cluster address in airflow.cfg")
        self.cluster_address = cluster_address

        self.use_async = True

        self._event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._security: Optional[Security] = None
        self._client: Optional[Client] = None
        self._futures: Optional[Dict[Future, TaskInstanceKeyType]] = None

    @property
    def security(self) -> Optional[Security]:
        """
        An optional security object for the ``.client``; it depends on
        airflow.cfg settings for dask security.
        """
        if self._security:
            return self._security
        # ssl / tls parameters
        tls_ca = conf.get("dask", "tls_ca")
        tls_key = conf.get("dask", "tls_key")
        tls_cert = conf.get("dask", "tls_cert")
        if tls_ca or tls_key or tls_cert:
            self._security = Security(
                tls_client_key=tls_key,
                tls_client_cert=tls_cert,
                tls_ca_file=tls_ca,
                require_encryption=True,
            )
            self.log.info("The %s security is initialized.", self.__class__.__name__)
            return self._security
        return None

    @property
    def client(self) -> Client:
        """
        A dask distributed client, with asynchronous futures
        """
        if self._client is None:
            self._client = Client(
                self.cluster_address, security=self.security, asynchronous=self.use_async
            )
            self.log.info("The %s client is initialized.", self.__class__.__name__)
        return self._client

    @property
    def futures(self) -> Dict[Future, TaskInstanceKeyType]:
        """
        A collection of asynchronous futures
        """
        if self._futures is None:
            self._futures = {}
            self.log.info("The %s futures are initialized.", self.__class__.__name__)
        return self._futures

    @property
    def event_loop(self) -> asyncio.AbstractEventLoop:
        """
        An asyncio event loop
        """
        if self._event_loop is None:
            self._event_loop = asyncio.new_event_loop()
            self.log.info("The %s event_loop is initialized.", self.__class__.__name__)
        return self._event_loop

    def start(self) -> None:
        # pylint: disable=do-not-use-asserts
        assert self.futures
        assert self.client
        assert self.event_loop
        # pylint: enable=do-not-use-asserts

    def execute_async(
        self,
        key: TaskInstanceKeyType,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ) -> None:

        # TODO: only submit awaitable tasks to the event loop;
        #       all other blocking tasks should raise an exception or something?

        async def async_submit():
            await self._ensure_started()
            future = self.client.submit(command)  # command must be awaitable?
            self.futures[future] = key
            await future  # TODO: can this raise?
            self._update_task_status(future)

        self.event_loop.run_until_complete(async_submit())

    def sync(self) -> None:
        pass  # the async_submit will auto-sync, right ???

    def end(self) -> None:
        """
        This method is called when the caller is done submitting job and
        wants to wait for the jobs submitted previously to be complete.
        """

        async def wait():
            while self.futures:
                # the async_submit function will pop futures when they are done;
                # so if there are any remaining, keep waiting.
                pause = random.uniform(1, 10)
                await asyncio.sleep(pause)
            # TODO: close the client and event_loop ?
            # await self._ensure_stopped()

        self.event_loop.run_until_complete(wait())

    def terminate(self) -> None:
        """
        This method is called when the daemon receives a SIGTERM
        """

        async def kill():
            await self._ensure_started()
            await self.client.cancel(list(self.futures.keys()))
            # does this require an additional self.client.gather or would that run them again?
            while self.futures:
                for future in self.futures.copy():
                    self._update_task_status(future)
                pause = random.uniform(1, 10)
                await asyncio.sleep(pause)
            await self._ensure_stopped()

        self.event_loop.run_until_complete(kill())

    async def _ensure_started(self):
        self.start()
        if self.futures is None:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if self.client is None:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if self.event_loop is None:
            raise AirflowException(NOT_STARTED_MESSAGE)

    async def _ensure_stopped(self):
        await self.client.close()
        self._client = None
        self._futures = None
        await self.event_loop.stop()
        await self.event_loop.close()
        self._event_loop = None

    def _update_task_status(self, future: Future) -> None:
        # TODO: figure out if this is a blocking operation or not
        #       - does it require any db-connection or db-operation
        #         to update the task status?
        if future.done():
            key = self.futures[future]
            if future.exception():
                self.log.error("Failed to execute task: %s", repr(future.exception()))
                self.fail(key)
            elif future.cancelled():
                self.log.error("Failed to execute task")
                self.fail(key)
            else:
                self.success(key)
            self.futures.pop(future)
