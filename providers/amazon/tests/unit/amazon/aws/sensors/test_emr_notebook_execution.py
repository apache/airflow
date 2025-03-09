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

from typing import Any
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr import EmrNotebookExecutionSensor


class TestEmrNotebookExecutionSensor:
    def _generate_response(self, status: str, reason: str | None = None) -> dict[str, Any]:
        return {
            "NotebookExecution": {
                "Status": status,
                "LastStateChangeReason": reason,
            },
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_emr_notebook_execution_sensor_success_state(self, mock_conn):
        mock_conn.describe_notebook_execution.return_value = self._generate_response("FINISHED")
        sensor = EmrNotebookExecutionSensor(
            task_id="test_task",
            poke_interval=0,
            notebook_execution_id="test-execution-id",
        )
        sensor.poke(None)
        mock_conn.describe_notebook_execution.assert_called_once_with(NotebookExecutionId="test-execution-id")

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_emr_notebook_execution_sensor_failed_state(self, mock_conn):
        error_reason = "Test error"
        mock_conn.describe_notebook_execution.return_value = self._generate_response("FAILED", error_reason)
        sensor = EmrNotebookExecutionSensor(
            task_id="test_task",
            poke_interval=0,
            notebook_execution_id="test-execution-id",
        )
        with pytest.raises(AirflowException, match=rf"EMR job failed: {error_reason}"):
            sensor.poke(None)
        mock_conn.describe_notebook_execution.assert_called_once_with(NotebookExecutionId="test-execution-id")

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_emr_notebook_execution_sensor_success_state_multiple(self, mock_conn):
        return_values = [self._generate_response("PENDING") for i in range(2)]
        return_values.append(self._generate_response("FINISHED"))
        mock_conn.describe_notebook_execution.side_effect = return_values
        sensor = EmrNotebookExecutionSensor(
            task_id="test_task",
            poke_interval=0,
            notebook_execution_id="test-execution-id",
        )
        while not sensor.poke(None):
            pass
        assert mock_conn.describe_notebook_execution.call_count == 3
