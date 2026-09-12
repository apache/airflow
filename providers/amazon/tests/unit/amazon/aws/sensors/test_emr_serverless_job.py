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

from datetime import timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.providers.amazon.aws.triggers.emr import EmrServerlessJobSensorTrigger
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred


class TestEmrServerlessJobSensor:
    def setup_method(self):
        self.app_id = "vzwemreks"
        self.job_run_id = "job1234"
        self.sensor = EmrServerlessJobSensor(
            task_id="test_emrcontainer_sensor",
            application_id=self.app_id,
            job_run_id=self.job_run_id,
            aws_conn_id="aws_default",
        )

    def set_get_job_run_return_value(self, return_value: dict[str, str]):
        self.mock_hook = MagicMock()
        self.mock_hook.conn.get_job_run.return_value = return_value
        self.sensor.hook = self.mock_hook

    def assert_get_job_run_was_called_once_with_app_and_run_id(self):
        self.mock_hook.conn.get_job_run.assert_called_once_with(
            applicationId=self.app_id, jobRunId=self.job_run_id
        )


class TestPokeReturnValue(TestEmrServerlessJobSensor):
    @pytest.mark.parametrize(
        ("state", "expected_result"),
        [
            ("PENDING", False),
            ("RUNNING", False),
            ("SCHEDULED", False),
            ("SUBMITTED", False),
            ("SUCCESS", True),
        ],
    )
    def test_poke_returns_expected_result_for_states(self, state, expected_result):
        get_job_run_return_value = {"jobRun": {"state": state}}
        self.set_get_job_run_return_value(get_job_run_return_value)
        assert self.sensor.poke(None) == expected_result
        self.assert_get_job_run_was_called_once_with_app_and_run_id()


class TestPokeRaisesAirflowException(TestEmrServerlessJobSensor):
    @pytest.mark.parametrize("state", ["FAILED", "CANCELLING", "CANCELLED"])
    def test_poke_raises_airflow_exception_with_specified_states(self, state):
        state_details = f"mock {state}"
        exception_msg = f"EMR Serverless job failed: {state_details}"
        get_job_run_return_value = {"jobRun": {"state": state, "stateDetails": state_details}}
        self.set_get_job_run_return_value(get_job_run_return_value)

        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)

        assert exception_msg == str(ctx.value)
        self.assert_get_job_run_was_called_once_with_app_and_run_id()


class TestEmrServerlessJobSensorDeferrable(TestEmrServerlessJobSensor):
    def test_sensor_defer_trigger_parameters(self):
        sensor = EmrServerlessJobSensor(
            task_id="test_emr_serverless_job_sensor",
            application_id=self.app_id,
            job_run_id=self.job_run_id,
            target_states={"RUNNING"},
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
            deferrable=True,
            poke_interval=10,
            timeout=300,
        )

        with mock.patch.object(EmrServerlessJobSensor, "poke", autospec=True, return_value=False):
            with pytest.raises(TaskDeferred) as exc:
                sensor.execute(context=None)

        trigger = exc.value.trigger
        assert isinstance(trigger, EmrServerlessJobSensorTrigger)
        assert trigger.serialized_fields == {
            "application_id": self.app_id,
            "job_run_id": self.job_run_id,
            "target_states": {"RUNNING"},
        }
        assert trigger.waiter_delay == 10
        assert trigger.aws_conn_id == "aws_default"
        assert trigger.region_name == "eu-west-1"
        assert trigger.verify is False
        assert trigger.botocore_config == {"read_timeout": 42}
        assert exc.value.timeout == timedelta(seconds=300)

    @mock.patch("airflow.providers.amazon.aws.sensors.emr.EmrServerlessJobSensor.poke", autospec=True)
    def test_sensor_defer_skipped_when_poke_succeeds(self, mock_poke):
        self.sensor.deferrable = True
        mock_poke.return_value = True
        self.sensor.execute(context=None)
        mock_poke.assert_called_once()

    def test_execute_complete_success(self):
        self.sensor.execute_complete(context={}, event={"status": "success", "value": None})

    def test_execute_complete_failure(self):
        with pytest.raises(RuntimeError, match="Error while running job"):
            self.sensor.execute_complete(context={}, event={"status": "error", "message": "Job failed"})
