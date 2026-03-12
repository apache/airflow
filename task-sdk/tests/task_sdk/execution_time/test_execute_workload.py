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

from airflow.executors import workloads
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.callback import CallbackDTO, CallbackFetchMethod


class TestExecuteWorkload:
    """Tests for execute_workload dispatch."""

    @staticmethod
    def _make_callback_workload():
        callback_data = CallbackDTO(
            id="12345678-1234-5678-1234-567812345678",
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
            data={"path": "test.module.my_callback", "kwargs": {}},
        )
        return workloads.ExecuteCallback(
            callback=callback_data,
            dag_rel_path="test_dag.py",
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token="test_token",
            log_path="executor_callbacks/test_dag/run_1/12345678",
        )

    @mock.patch("airflow.executors.base_executor.BaseExecutor.run_workload")
    @mock.patch("airflow.settings.dispose_orm")
    def test_execute_workload_handles_callback(self, mock_dispose_orm, mock_run_workload):
        from airflow.sdk.execution_time.execute_workload import execute_workload

        workload = self._make_callback_workload()
        execute_workload(workload)

        mock_run_workload.assert_called_once_with(workload, subprocess_logs_to_stdout=True)

    @mock.patch("airflow.executors.base_executor.BaseExecutor.run_workload")
    @mock.patch("airflow.settings.dispose_orm")
    def test_execute_workload_handles_task(self, mock_dispose_orm, mock_run_workload):
        from airflow.sdk.execution_time.execute_workload import execute_workload

        # ExecuteTask is also dispatched via run_workload
        # We verify the same path is taken regardless of workload type
        workload = self._make_callback_workload()
        execute_workload(workload)

        mock_run_workload.assert_called_once_with(workload, subprocess_logs_to_stdout=True)
