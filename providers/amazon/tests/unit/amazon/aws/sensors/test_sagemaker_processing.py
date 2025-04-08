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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerProcessingSensor

DESCRIBE_PROCESSING_INPROGRESS_RESPONSE = {
    "ProcessingJobStatus": "InProgress",
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}

DESCRIBE_PROCESSING_COMPLETED_RESPONSE = {
    "ProcessingJobStatus": "Completed",
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}

DESCRIBE_PROCESSING_FAILED_RESPONSE = {
    "ProcessingJobStatus": "Failed",
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
    "FailureReason": "Unknown",
}

DESCRIBE_PROCESSING_STOPPING_RESPONSE = {
    "ProcessingJobStatus": "Stopping",
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}

DESCRIBE_PROCESSING_STOPPED_RESPONSE = {
    "ProcessingJobStatus": "Stopped",
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}


class TestSageMakerProcessingSensor:
    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "describe_processing_job")
    def test_sensor_with_failure(self, mock_describe_job, mock_client):
        mock_describe_job.side_effect = [DESCRIBE_PROCESSING_FAILED_RESPONSE]
        sensor = SageMakerProcessingSensor(
            task_id="test_task", poke_interval=2, aws_conn_id="aws_test", job_name="test_job_name"
        )
        with pytest.raises(AirflowException):
            sensor.execute(None)
        mock_describe_job.assert_called_once_with("test_job_name")

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "describe_processing_job")
    def test_sensor_with_stopped(self, mock_describe_job, mock_client):
        mock_describe_job.side_effect = [DESCRIBE_PROCESSING_STOPPED_RESPONSE]
        sensor = SageMakerProcessingSensor(
            task_id="test_task", poke_interval=2, aws_conn_id="aws_test", job_name="test_job_name"
        )
        with pytest.raises(AirflowException):
            sensor.execute(None)
        mock_describe_job.assert_called_once_with("test_job_name")

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "__init__")
    @mock.patch.object(SageMakerHook, "describe_processing_job")
    def test_sensor(self, mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [
            DESCRIBE_PROCESSING_INPROGRESS_RESPONSE,
            DESCRIBE_PROCESSING_STOPPING_RESPONSE,
            DESCRIBE_PROCESSING_COMPLETED_RESPONSE,
        ]
        sensor = SageMakerProcessingSensor(
            task_id="test_task", poke_interval=0, aws_conn_id="aws_test", job_name="test_job_name"
        )

        sensor.execute(None)

        # make sure we called 3 times(terminated when its completed)
        assert mock_describe_job.call_count == 3

        # make sure the hook was initialized with the specific params
        calls = [mock.call(aws_conn_id="aws_test")]
        hook_init.assert_has_calls(calls)
