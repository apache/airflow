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
import unittest
from unittest import mock

from airflow.task.task_runner.cgroup_task_runner import CgroupTaskRunner


class TestCgroupTaskRunner(unittest.TestCase):
    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.__init__")
    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.on_finish")
    def test_cgroup_task_runner_super_calls(self, mock_super_on_finish, mock_super_init):
        """
        This test ensures that initiating CgroupTaskRunner object
        calls init method of BaseTaskRunner,
        and when task finishes, CgroupTaskRunner.on_finish() calls
        super().on_finish() to delete the temp cfg file.
        """
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = None
        local_task_job.task_instance.command_as_list.return_value = ['sleep', '1000']

        runner = CgroupTaskRunner(local_task_job)
        self.assertTrue(mock_super_init.called)

        runner.on_finish()
        self.assertTrue(mock_super_on_finish.called)
