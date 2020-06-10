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
DaskExecutor

.. seealso::
    For more information on how the DaskExecutor works, take a look at the guide:
    :ref:`executor:DaskExecutor`
"""
import subprocess
from typing import Any, Dict, Optional

from distributed import Client, Future, as_completed
from distributed.security import Security

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import NOT_STARTED_MESSAGE, BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKeyType


class DaskExecutor(BaseExecutor):
    """
    DaskExecutor submits tasks to a Dask Distributed cluster.
    """
    def __init__(self, cluster_address=None):
        super().__init__(parallelism=0)
        if cluster_address is None:
            cluster_address = conf.get('dask', 'cluster_address')
        if not cluster_address:
            raise ValueError('Please provide a Dask cluster address in airflow.cfg')
        self.cluster_address = cluster_address
        # ssl / tls parameters
        self.tls_ca = conf.get('dask', 'tls_ca')
        self.tls_key = conf.get('dask', 'tls_key')
        self.tls_cert = conf.get('dask', 'tls_cert')
        self.client: Optional[Client] = None
        self.futures: Optional[Dict[Future, TaskInstanceKeyType]] = None

    def start(self) -> None:
        if self.tls_ca or self.tls_key or self.tls_cert:
            security = Security(
                tls_client_key=self.tls_key,
                tls_client_cert=self.tls_cert,
                tls_ca_file=self.tls_ca,
                require_encryption=True,
            )
        else:
            security = None

        self.client = Client(self.cluster_address, security=security)
        self.futures = {}

    def execute_async(self,
                      key: TaskInstanceKeyType,
                      command: CommandType,
                      queue: Optional[str] = None,
                      executor_config: Optional[Any] = None) -> None:

        if command[0:3] != ["airflow", "tasks", "run"]:
            raise ValueError('The command must start with ["airflow", "tasks", "run"].')

        def airflow_run():
            return subprocess.check_call(command, close_fds=True)

        if not self.client:
            raise AirflowException(NOT_STARTED_MESSAGE)

        future = self.client.submit(airflow_run, pure=False)
        self.futures[future] = key  # type: ignore

    def _process_future(self, future: Future) -> None:
        if not self.futures:
            raise AirflowException(NOT_STARTED_MESSAGE)
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

    def sync(self) -> None:
        if not self.futures:
            raise AirflowException(NOT_STARTED_MESSAGE)
        # make a copy so futures can be popped during iteration
        for future in self.futures.copy():
            self._process_future(future)

    def end(self) -> None:
        if not self.client:
            raise AirflowException(NOT_STARTED_MESSAGE)
        if not self.futures:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.client.cancel(list(self.futures.keys()))
        for future in as_completed(self.futures.copy()):
            self._process_future(future)

    def terminate(self):
        if not self.futures:
            raise AirflowException(NOT_STARTED_MESSAGE)
        self.client.cancel(self.futures.keys())
        self.end()
