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
from __future__ import annotations

from unittest import mock

from cgroupspy.nodes import Node

from airflow.task.task_runner.cgroup_task_runner import CgroupTaskRunner


class TestCgroupTaskRunner:
    def setup_method(self):
        job = mock.Mock()
        job.job_type = None
        job.task_instance = mock.MagicMock()
        job.task_instance.run_as_user = None
        job.task_instance.command_as_list.return_value = ["sleep", "1000"]
        job.task_instance.task.resources = None
        self.job = job

    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.__init__")
    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.on_finish")
    def test_cgroup_task_runner_super_calls(self, mock_super_on_finish, mock_super_init):
        """
        This test ensures that initiating CgroupTaskRunner object
        calls init method of BaseTaskRunner,
        and when task finishes, CgroupTaskRunner.on_finish() calls
        super().on_finish() to delete the temp cfg file.
        """
        runner = CgroupTaskRunner(self.job)
        assert mock_super_init.called

        runner.on_finish()
        assert mock_super_on_finish.called

    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.__init__")
    @mock.patch("cgroupspy.nodes.Node.create_cgroup")
    def test_create_cgroup_not_exist(self, mock_create_cgroup, mock_super_init):
        mock_create_cgroup.return_value = Node("test_node")
        node = CgroupTaskRunner(self.job)._create_cgroup("./test_cgroup")
        assert node.name.decode() == "test_node"

    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.__init__")
    @mock.patch("cgroupspy.nodes.Node.delete_cgroup")
    def test_delete_cgroup_exist(self, mock_delete_cgroup, mock_super_init):
        CgroupTaskRunner(self.job)._delete_cgroup("./test_cgroup")
        assert not mock_delete_cgroup.called

    @mock.patch("airflow.utils.log.logging_mixin.LoggingMixin.__init__")
    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.run_command")
    @mock.patch("cgroupspy.nodes.Node.create_cgroup")
    def test_start_task(self, mock_create_cgroup, mock_run_command, logging_init):
        CgroupTaskRunner(self.job).start()
        assert mock_create_cgroup.called
        assert mock_run_command.called
        assert logging_init.called

    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.__init__")
    def test_return_code_none(self, mock_super_init):
        return_code = CgroupTaskRunner(self.job).return_code()
        assert not return_code

    @mock.patch("airflow.task.task_runner.base_task_runner.BaseTaskRunner.__init__")
    @mock.patch("builtins.open", new_callable=mock.mock_open)
    def test_log_memory_usage(self, mock_open_file, mock_super_init):
        mock_open_file.return_value.read.return_value = "12345789"
        mem_cgroup_node = mock.Mock()
        mem_cgroup_node.full_path = "/test/cgroup"
        mem_cgroup_node.controller = mock.MagicMock()
        mem_cgroup_node.controller.limit_in_bytes = 123456
        CgroupTaskRunner(self.job)._log_memory_usage(mem_cgroup_node)
        assert mock_open_file.called
