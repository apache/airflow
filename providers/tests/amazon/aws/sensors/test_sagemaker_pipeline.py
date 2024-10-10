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

import copy
from datetime import datetime
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerPipelineSensor

DESCRIBE_PIPELINE_EXECUTION_RESPONSE = {
    "PipelineArn": "string",
    "PipelineExecutionArn": "string",
    "PipelineExecutionDisplayName": "string",
    # Status can be: "Executing" | "Stopping" | "Stopped" | "Failed" | "Succeeded"
    "PipelineExecutionStatus": "-- to be set in test --",
    "PipelineExecutionDescription": "string",
    "PipelineExperimentConfig": {"ExperimentName": "string", "TrialName": "string"},
    "FailureReason": "string",
    "CreationTime": datetime(2015, 1, 1),
    "LastModifiedTime": datetime(2015, 1, 1),
    "CreatedBy": {"UserProfileArn": "string", "UserProfileName": "string", "DomainId": "string"},
    "LastModifiedBy": {"UserProfileArn": "string", "UserProfileName": "string", "DomainId": "string"},
    "ParallelismConfiguration": {"MaxParallelExecutionSteps": 123},
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}


class TestSageMakerPipelineSensor:
    @staticmethod
    def get_response_with_state(state: str):
        states = {"Executing", "Stopping", "Stopped", "Failed", "Succeeded"}
        assert state in states
        res_copy = copy.deepcopy(DESCRIBE_PIPELINE_EXECUTION_RESPONSE)
        res_copy["PipelineExecutionStatus"] = state
        return res_copy

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "describe_pipeline_exec")
    def test_sensor_with_failure(self, mock_describe, _):
        response_failure = self.get_response_with_state("Failed")
        mock_describe.return_value = response_failure
        sensor = SageMakerPipelineSensor(pipeline_exec_arn="ARN", task_id="test_task")

        with pytest.raises(AirflowException):
            sensor.execute(None)

        mock_describe.assert_called_once_with("ARN", True)

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "describe_pipeline_exec")
    def test_sensor(self, mock_describe, _):
        response_executing = self.get_response_with_state("Executing")
        response_stopping = self.get_response_with_state("Stopping")
        response_stopped = self.get_response_with_state("Stopped")
        mock_describe.side_effect = [
            response_executing,
            response_stopping,
            response_stopped,
        ]
        sensor = SageMakerPipelineSensor(pipeline_exec_arn="ARN", task_id="test_task", poke_interval=0)

        sensor.execute(None)

        assert mock_describe.call_count == 3
