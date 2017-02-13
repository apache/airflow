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

import distributed

import subprocess
import warnings

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor


class DaskExecutor(BaseExecutor):
    """
    DaskExecutor submits tasks to a Dask Distributed cluster.
    """
    def __init__(self, cluster_address=None):
        if cluster_address is None:
            cluster_address = configuration.get('dask', 'cluster_address')
        if not cluster_address:
            raise ValueError(
                'Please provide a Dask cluster address in airflow.cfg')
        self.cluster_address = cluster_address
        super(DaskExecutor, self).__init__(parallelism=0)

    def start(self):
        self.client = distributed.Client(self.cluster_address)
        self.futures = {}

    def execute_async(self, key, command, queue=None):
        if queue is not None:
            warnings.warn(
                'DaskExecutor does not support queues. All tasks will be run '
                'in the same cluster')

        def airflow_run():
            return subprocess.check_call(command, shell=True)

        future = self.client.submit(airflow_run, pure=False)
        self.futures[future] = key

    def _process_future(self, future):
        if future.done():
            key = self.futures[future]
            if future.exception():
                self.fail(key)
                self.logger.error("Failed to execute task: {}".format(
                    repr(future.exception())))
            elif future.cancelled():
                self.fail(key)
                self.logger.error("Failed to execute task")
            else:
                self.success(key)
            self.futures.pop(future)

    def sync(self):
        # make a copy so futures can be popped during iteration
        for future in self.futures.copy():
            self._process_future(future)

    def end(self):
        for future in distributed.as_completed(self.futures.copy()):
            self._process_future(future)

    def terminate(self):
        self.client.cancel(self.futures.keys())
        self.end()
