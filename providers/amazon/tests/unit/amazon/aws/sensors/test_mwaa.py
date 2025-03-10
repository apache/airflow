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
from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.sensors.mwaa import MwaaDagRunSensor
from airflow.utils.state import State

SENSOR_KWARGS = {
    "task_id": "test_mwaa_sensor",
    "external_env_name": "test_env",
    "external_dag_id": "test_dag",
    "external_dag_run_id": "test_run_id",
}


@pytest.fixture
def mock_invoke_rest_api():
    with mock.patch.object(MwaaHook, "invoke_rest_api") as m:
        yield m


class TestMwaaDagRunSuccessSensor:
    def test_init_success(self):
        success_states = {"state1", "state2"}
        failure_states = {"state3", "state4"}
        sensor = MwaaDagRunSensor(
            **SENSOR_KWARGS, success_states=success_states, failure_states=failure_states
        )
        assert sensor.external_env_name == SENSOR_KWARGS["external_env_name"]
        assert sensor.external_dag_id == SENSOR_KWARGS["external_dag_id"]
        assert sensor.external_dag_run_id == SENSOR_KWARGS["external_dag_run_id"]
        assert set(sensor.success_states) == success_states
        assert set(sensor.failure_states) == failure_states

    def test_init_failure(self):
        with pytest.raises(AirflowException):
            MwaaDagRunSensor(
                **SENSOR_KWARGS, success_states={"state1", "state2"}, failure_states={"state2", "state3"}
            )

    @pytest.mark.parametrize("status", sorted(State.success_states))
    def test_poke_completed(self, mock_invoke_rest_api, status):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": status}}
        assert MwaaDagRunSensor(**SENSOR_KWARGS).poke({})

    @pytest.mark.parametrize("status", ["running", "queued"])
    def test_poke_not_completed(self, mock_invoke_rest_api, status):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": status}}
        assert not MwaaDagRunSensor(**SENSOR_KWARGS).poke({})

    @pytest.mark.parametrize("status", sorted(State.failed_states))
    def test_poke_terminated(self, mock_invoke_rest_api, status):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": status}}
        with pytest.raises(AirflowException):
            MwaaDagRunSensor(**SENSOR_KWARGS).poke({})
