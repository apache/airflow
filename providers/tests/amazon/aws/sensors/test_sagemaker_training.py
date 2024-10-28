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

from datetime import datetime
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.hooks.sagemaker import LogState, SageMakerHook
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerTrainingSensor

DESCRIBE_TRAINING_COMPLETED_RESPONSE = {
    "TrainingJobStatus": "Completed",
    "ResourceConfig": {
        "InstanceCount": 1,
        "InstanceType": "ml.c4.xlarge",
        "VolumeSizeInGB": 10,
    },
    "TrainingStartTime": datetime(2018, 2, 17, 7, 15, 0, 103000),
    "TrainingEndTime": datetime(2018, 2, 17, 7, 19, 34, 953000),
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}

DESCRIBE_TRAINING_INPROGRESS_RESPONSE = dict(DESCRIBE_TRAINING_COMPLETED_RESPONSE)
DESCRIBE_TRAINING_INPROGRESS_RESPONSE.update({"TrainingJobStatus": "InProgress"})

DESCRIBE_TRAINING_FAILED_RESPONSE = dict(DESCRIBE_TRAINING_COMPLETED_RESPONSE)
DESCRIBE_TRAINING_FAILED_RESPONSE.update(
    {"TrainingJobStatus": "Failed", "FailureReason": "Unknown"}
)

DESCRIBE_TRAINING_STOPPING_RESPONSE = dict(DESCRIBE_TRAINING_COMPLETED_RESPONSE)
DESCRIBE_TRAINING_STOPPING_RESPONSE.update({"TrainingJobStatus": "Stopping"})


class TestSageMakerTrainingSensor:
    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "__init__")
    @mock.patch.object(SageMakerHook, "describe_training_job")
    def test_sensor_with_failure(self, mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [DESCRIBE_TRAINING_FAILED_RESPONSE]
        sensor = SageMakerTrainingSensor(
            task_id="test_task",
            poke_interval=2,
            aws_conn_id="aws_test",
            job_name="test_job_name",
            print_log=False,
        )
        with pytest.raises(AirflowException):
            sensor.execute(None)
        mock_describe_job.assert_called_once_with("test_job_name")

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "__init__")
    @mock.patch.object(SageMakerHook, "describe_training_job")
    def test_sensor(self, mock_describe_job, hook_init, mock_client):
        hook_init.return_value = None

        mock_describe_job.side_effect = [
            DESCRIBE_TRAINING_INPROGRESS_RESPONSE,
            DESCRIBE_TRAINING_STOPPING_RESPONSE,
            DESCRIBE_TRAINING_COMPLETED_RESPONSE,
        ]
        sensor = SageMakerTrainingSensor(
            task_id="test_task",
            poke_interval=0,
            aws_conn_id="aws_test",
            job_name="test_job_name",
            print_log=False,
        )

        sensor.execute(None)

        # make sure we called 3 times(terminated when its completed)
        assert mock_describe_job.call_count == 3

        # make sure the hook was initialized with the specific params
        calls = [mock.call(aws_conn_id="aws_test")]
        hook_init.assert_has_calls(calls)

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(AwsLogsHook, "get_conn")
    @mock.patch.object(SageMakerHook, "__init__")
    @mock.patch.object(SageMakerHook, "describe_training_job_with_log")
    @mock.patch.object(SageMakerHook, "describe_training_job")
    def test_sensor_with_log(
        self,
        mock_describe_job,
        mock_describe_job_with_log,
        hook_init,
        mock_log_client,
        mock_client,
    ):
        hook_init.return_value = None

        mock_describe_job.return_value = DESCRIBE_TRAINING_COMPLETED_RESPONSE
        mock_describe_job_with_log.side_effect = [
            (LogState.WAIT_IN_PROGRESS, DESCRIBE_TRAINING_INPROGRESS_RESPONSE, 0),
            (LogState.JOB_COMPLETE, DESCRIBE_TRAINING_STOPPING_RESPONSE, 0),
            (LogState.COMPLETE, DESCRIBE_TRAINING_COMPLETED_RESPONSE, 0),
        ]
        sensor = SageMakerTrainingSensor(
            task_id="test_task",
            poke_interval=0,
            aws_conn_id="aws_test",
            job_name="test_job_name",
            print_log=True,
        )

        sensor.execute(None)

        assert mock_describe_job_with_log.call_count == 3
        assert mock_describe_job.call_count == 1

        calls = [mock.call(aws_conn_id="aws_test")]
        hook_init.assert_has_calls(calls)
