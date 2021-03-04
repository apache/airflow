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


# to use this test, please install and run
# https://github.com/irvinlim/mesosmock
# with the default configuration


import unittest
from queue import Queue

try:
    from avmesos.client import MesosClient

    from airflow.executors.mesos_executor import AirflowMesosScheduler, MesosExecutor

    mock_mesos = True
except ImportError:
    mock_mesos = None  # type: ignore


class MesosExecutorTest(unittest.TestCase):
    task_queue = Queue()  # type: ignore
    master = "localhost:5050"
    fake_framework_name = "fake-framework-name"
    task_cpu = 2
    task_memory = 4
    master_urls = "http://" + master

    @unittest.skipIf(mock_mesos is None, "mesos python modules are not installed")
    def test_mesos_executor(self):
        # create task queue, empty result queue, task_cpu and task_memory
        mesos_executor = MesosExecutor()
        mesos_executor.client = MesosClient(
            mesos_urls=self.master_urls.split(','), frameworkName=self.fake_framework_name, frameworkId=None
        )
        driver = AirflowMesosScheduler(mesos_executor, self.task_queue, self.task_cpu, self.task_memory)
        mesos_executor.driver = driver
        mesos_executor.thread = MesosExecutor.MesosFramework(mesos_executor.client)
        mesos_executor.thread.start()
        mesos_executor.thread.client.disconnect = True


if __name__ == '__main__':
    unittest.main()
