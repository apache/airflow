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

from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerAutoMLSensor
from airflow.providers.common.compat.sdk import AirflowException


class TestSageMakerAutoMLSensor:
    @staticmethod
    def get_response_with_state(state: str):
        states = {"Completed", "InProgress", "Failed", "Stopped", "Stopping"}
        assert state in states
        return {
            "AutoMLJobStatus": state,
            "AutoMLJobSecondaryStatus": "Starting",
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }

    @mock.patch.object(SageMakerHook, "_describe_auto_ml_job")
    def test_sensor_with_failure(self, mock_describe):
        mock_describe.return_value = self.get_response_with_state("Failed")
        sensor = SageMakerAutoMLSensor(job_name="job_job", task_id="test_task")

        with pytest.raises(AirflowException):
            sensor.execute(None)

        mock_describe.assert_called_once_with("job_job")

    @mock.patch.object(SageMakerHook, "_describe_auto_ml_job")
    def test_sensor(self, mock_describe):
        mock_describe.side_effect = [
            self.get_response_with_state("InProgress"),
            self.get_response_with_state("Stopping"),
            self.get_response_with_state("Stopped"),
        ]
        sensor = SageMakerAutoMLSensor(job_name="job_job", task_id="test_task", poke_interval=0)

        sensor.execute(None)

        assert mock_describe.call_count == 3
