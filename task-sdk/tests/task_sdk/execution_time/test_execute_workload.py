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

from airflow.executors import workloads
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.callback import CallbackDTO, CallbackFetchMethod


class TestExecuteWorkloadCallback:
    """Tests for callback handling in execute_workload."""

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

    @mock.patch("airflow.executors.workloads.callback.execute_callback_workload")
    @mock.patch("airflow.settings.dispose_orm")
    def test_execute_workload_handles_callback(self, mock_dispose_orm, mock_execute_callback):
        from airflow.sdk.execution_time.execute_workload import execute_workload

        mock_execute_callback.return_value = (True, None)

        workload = self._make_callback_workload()
        execute_workload(workload)

        mock_execute_callback.assert_called_once_with(workload.callback, mock.ANY)

    @mock.patch("airflow.executors.workloads.callback.execute_callback_workload")
    @mock.patch("airflow.settings.dispose_orm")
    def test_execute_workload_callback_failure_raises(self, mock_dispose_orm, mock_execute_callback):
        from airflow.sdk.execution_time.execute_workload import execute_workload

        mock_execute_callback.return_value = (False, "Something went wrong")

        workload = self._make_callback_workload()
        with pytest.raises(RuntimeError, match="Callback execution failed"):
            execute_workload(workload)

    @mock.patch("airflow.settings.dispose_orm")
    def test_execute_workload_rejects_unknown_type(self, mock_dispose_orm):
        from airflow.sdk.execution_time.execute_workload import execute_workload

        with pytest.raises(ValueError, match="does not know how to handle"):
            execute_workload("not_a_workload")  # type: ignore[arg-type]
