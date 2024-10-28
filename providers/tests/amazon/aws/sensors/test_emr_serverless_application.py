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

from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessApplicationSensor


class TestEmrServerlessApplicationSensor:
    def setup_method(self):
        self.app_id = "vzwemreks"
        self.job_run_id = "job1234"
        self.sensor = EmrServerlessApplicationSensor(
            task_id="test_emrcontainer_sensor",
            application_id=self.app_id,
            aws_conn_id="aws_default",
        )

    def set_get_application_return_value(self, return_value: dict[str, str]):
        self.mock_hook = MagicMock()
        self.mock_hook.conn.get_application.return_value = return_value
        self.sensor.hook = self.mock_hook

    def assert_get_application_was_called_once_with_app_id(self):
        self.mock_hook.conn.get_application.assert_called_once_with(
            applicationId=self.app_id
        )


class TestPokeReturnValue(TestEmrServerlessApplicationSensor):
    @pytest.mark.parametrize(
        "state, expected_result",
        [
            ("CREATING", False),
            ("STARTING", False),
            ("STOPPING", False),
            ("CREATED", True),
            ("STARTED", True),
        ],
    )
    def test_poke_returns_expected_result_for_states(self, state, expected_result):
        get_application_return_value = {"application": {"state": state}}
        self.set_get_application_return_value(get_application_return_value)
        assert self.sensor.poke(None) == expected_result
        self.assert_get_application_was_called_once_with_app_id()


class TestPokeRaisesAirflowException(TestEmrServerlessApplicationSensor):
    @pytest.mark.parametrize("state", ["STOPPED", "TERMINATED"])
    def test_poke_raises_airflow_exception_with_failure_states(self, state):
        state_details = f"mock {state}"
        exception_msg = f"EMR Serverless application failed: {state_details}"
        get_job_run_return_value = {
            "application": {"state": state, "stateDetails": state_details}
        }
        self.set_get_application_return_value(get_job_run_return_value)

        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke(None)

        assert exception_msg == str(ctx.value)
        self.assert_get_application_was_called_once_with_app_id()
