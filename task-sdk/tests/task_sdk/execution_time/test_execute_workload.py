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

from airflow.executors.workloads import ExecuteTask
from airflow.sdk.execution_time.execute_workload import execute_workload


@mock.patch("airflow.executors.base_executor.BaseExecutor.run_workload", autospec=True)
@mock.patch("airflow.sdk.log.configure_logging", autospec=True)
@mock.patch("airflow.settings.dispose_orm", autospec=True)
@mock.patch("airflow.settings.ensure_initialized", autospec=True)
def test_initializes_without_orm_before_running_the_workload(
    mock_ensure_initialized, mock_dispose_orm, mock_configure_logging, mock_run_workload
):
    calls = mock.Mock()
    calls.attach_mock(mock_ensure_initialized, "ensure_initialized")
    calls.attach_mock(mock_configure_logging, "configure_logging")
    calls.attach_mock(mock_run_workload, "run_workload")
    workload = mock.Mock(spec=ExecuteTask)

    execute_workload(workload)

    assert calls.mock_calls == [
        mock.call.ensure_initialized(orm=False),
        mock.call.configure_logging(output=mock.ANY, json_output=True),
        mock.call.run_workload(workload, subprocess_logs_to_stdout=True),
    ]
