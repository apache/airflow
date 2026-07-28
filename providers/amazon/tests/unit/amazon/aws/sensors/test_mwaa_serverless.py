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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.sensors.mwaa_serverless import MwaaServerlessWorkflowRunSensor

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

WORKFLOW_ARN = "arn:aws:mwaa-serverless:us-east-1:123456789012:workflow/test"
RUN_ID = "run-abc123"


class TestMwaaServerlessWorkflowRunSensor:
    def setup_method(self):
        self.sensor = MwaaServerlessWorkflowRunSensor(
            task_id="wait_for_run",
            workflow_arn=WORKFLOW_ARN,
            run_id=RUN_ID,
            poke_interval=5,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_poke_success(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.get_workflow_run.return_value = {"RunDetail": {"RunState": "SUCCESS", "ErrorMessage": ""}}
        mock_conn.return_value = mock_client

        assert self.sensor.poke({}) is True

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_poke_running(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.get_workflow_run.return_value = {"RunDetail": {"RunState": "RUNNING", "ErrorMessage": ""}}
        mock_conn.return_value = mock_client

        assert self.sensor.poke({}) is False

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_poke_failed(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.get_workflow_run.return_value = {
            "RunDetail": {"RunState": "FAILED", "ErrorMessage": "Task failed"}
        }
        mock_conn.return_value = mock_client

        with pytest.raises(RuntimeError, match="Task failed"):
            self.sensor.poke({})

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_poke_custom_states(self, mock_conn):
        sensor = MwaaServerlessWorkflowRunSensor(
            task_id="wait_for_run",
            workflow_arn=WORKFLOW_ARN,
            run_id=RUN_ID,
            success_states={"SUCCESS", "STOPPED"},
            failure_states={"FAILED"},
        )
        mock_client = mock.MagicMock()
        mock_client.get_workflow_run.return_value = {"RunDetail": {"RunState": "STOPPED", "ErrorMessage": ""}}
        mock_conn.return_value = mock_client

        assert sensor.poke({}) is True

    def test_template_fields(self):
        validate_template_fields(self.sensor)
