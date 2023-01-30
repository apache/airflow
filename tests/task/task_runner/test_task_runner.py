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

import pytest

from airflow.task.task_runner import CORE_TASK_RUNNERS, get_task_runner
from airflow.utils.module_loading import import_string

custom_task_runner = mock.MagicMock()


class TestGetTaskRunner:
    @pytest.mark.parametrize("import_path", CORE_TASK_RUNNERS.values())
    def test_should_have_valid_imports(self, import_path):
        assert import_string(import_path) is not None

    @mock.patch("airflow.task.task_runner.base_task_runner.subprocess")
    @mock.patch("airflow.task.task_runner._TASK_RUNNER_NAME", "StandardTaskRunner")
    def test_should_support_core_task_runner(self, mock_subprocess):
        ti = mock.MagicMock(map_index=-1, run_as_user=None)
        ti.get_template_context.return_value = {"ti": ti}
        ti.get_dagrun.return_value.get_log_template.return_value.filename = "blah"
        local_task_job = mock.MagicMock(task_instance=ti)
        task_runner = get_task_runner(local_task_job)

        assert "StandardTaskRunner" == task_runner.__class__.__name__

    @mock.patch(
        "airflow.task.task_runner._TASK_RUNNER_NAME",
        "tests.task.task_runner.test_task_runner.custom_task_runner",
    )
    def test_should_support_custom_task_runner(self):
        local_task_job = mock.MagicMock(
            **{"task_instance.get_template_context.return_value": {"ti": mock.MagicMock()}}
        )
        custom_task_runner.reset_mock()

        task_runner = get_task_runner(local_task_job)

        custom_task_runner.assert_called_once_with(local_task_job)

        assert custom_task_runner.return_value == task_runner
