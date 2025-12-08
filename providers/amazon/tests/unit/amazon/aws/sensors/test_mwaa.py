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

from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.sensors.mwaa import MwaaDagRunSensor, MwaaTaskSensor
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.state import DagRunState, TaskInstanceState

SENSOR_DAG_RUN_KWARGS = {
    "task_id": "test_mwaa_sensor",
    "external_env_name": "test_env",
    "external_dag_id": "test_dag",
    "external_dag_run_id": "test_run_id",
    "deferrable": False,
    "poke_interval": 5,
    "max_retries": 100,
}

SENSOR_TASK_KWARGS = {
    "task_id": "test_mwaa_sensor",
    "external_env_name": "test_env",
    "external_dag_id": "test_dag",
    "external_dag_run_id": "test_run_id",
    "external_task_id": "test_task_id",
    "deferrable": True,
    "poke_interval": 5,
    "max_retries": 100,
}

SENSOR_STATE_KWARGS = {
    "success_states": ["a", "b"],
    "failure_states": ["c", "d"],
}


@pytest.fixture
def mock_invoke_rest_api():
    with mock.patch.object(MwaaHook, "invoke_rest_api") as m:
        yield m


class TestMwaaDagRunSuccessSensor:
    def test_init_success(self):
        sensor = MwaaDagRunSensor(**SENSOR_DAG_RUN_KWARGS, **SENSOR_STATE_KWARGS)
        assert sensor.external_env_name == SENSOR_DAG_RUN_KWARGS["external_env_name"]
        assert sensor.external_dag_id == SENSOR_DAG_RUN_KWARGS["external_dag_id"]
        assert sensor.external_dag_run_id == SENSOR_DAG_RUN_KWARGS["external_dag_run_id"]
        assert set(sensor.success_states) == set(SENSOR_STATE_KWARGS["success_states"])
        assert set(sensor.failure_states) == set(SENSOR_STATE_KWARGS["failure_states"])
        assert sensor.deferrable == SENSOR_DAG_RUN_KWARGS["deferrable"]
        assert sensor.poke_interval == SENSOR_DAG_RUN_KWARGS["poke_interval"]
        assert sensor.max_retries == SENSOR_DAG_RUN_KWARGS["max_retries"]

        sensor = MwaaDagRunSensor(**SENSOR_DAG_RUN_KWARGS)
        assert sensor.success_states == {DagRunState.SUCCESS.value}
        assert sensor.failure_states == {DagRunState.FAILED.value}

    def test_init_failure(self):
        with pytest.raises(ValueError, match=r".*success_states.*failure_states.*"):
            MwaaDagRunSensor(
                **SENSOR_DAG_RUN_KWARGS,
                success_states={"state1", "state2"},
                failure_states={"state2", "state3"},
            )

    @pytest.mark.parametrize("state", SENSOR_STATE_KWARGS["success_states"])
    def test_poke_completed(self, mock_invoke_rest_api, state):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": state}}
        assert MwaaDagRunSensor(**SENSOR_DAG_RUN_KWARGS, **SENSOR_STATE_KWARGS).poke({})

    @pytest.mark.parametrize("state", ["e", "f"])
    def test_poke_not_completed(self, mock_invoke_rest_api, state):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": state}}
        assert not MwaaDagRunSensor(**SENSOR_DAG_RUN_KWARGS, **SENSOR_STATE_KWARGS).poke({})

    @pytest.mark.parametrize("state", SENSOR_STATE_KWARGS["failure_states"])
    def test_poke_terminated(self, mock_invoke_rest_api, state):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": state}}
        with pytest.raises(AirflowException, match=f".*{state}.*"):
            MwaaDagRunSensor(**SENSOR_DAG_RUN_KWARGS, **SENSOR_STATE_KWARGS).poke({})


class TestMwaaTaskSuccessSensor:
    def test_init_success(self):
        sensor = MwaaTaskSensor(**SENSOR_TASK_KWARGS, **SENSOR_STATE_KWARGS)
        assert sensor.external_env_name == SENSOR_TASK_KWARGS["external_env_name"]
        assert sensor.external_dag_id == SENSOR_TASK_KWARGS["external_dag_id"]
        assert sensor.external_dag_run_id == SENSOR_TASK_KWARGS["external_dag_run_id"]
        assert sensor.external_task_id == SENSOR_TASK_KWARGS["external_task_id"]
        assert set(sensor.success_states) == set(SENSOR_STATE_KWARGS["success_states"])
        assert set(sensor.failure_states) == set(SENSOR_STATE_KWARGS["failure_states"])
        assert sensor.deferrable == SENSOR_TASK_KWARGS["deferrable"]
        assert sensor.poke_interval == SENSOR_TASK_KWARGS["poke_interval"]
        assert sensor.max_retries == SENSOR_TASK_KWARGS["max_retries"]

        sensor = MwaaTaskSensor(**SENSOR_TASK_KWARGS)
        assert sensor.success_states == {TaskInstanceState.SUCCESS.value}
        assert sensor.failure_states == {TaskInstanceState.FAILED.value}

    def test_init_failure(self):
        with pytest.raises(ValueError, match=r".*success_states.*failure_states.*"):
            MwaaTaskSensor(
                **SENSOR_TASK_KWARGS, success_states={"state1", "state2"}, failure_states={"state2", "state3"}
            )

    @pytest.mark.parametrize("state", SENSOR_STATE_KWARGS["success_states"])
    def test_poke_completed(self, mock_invoke_rest_api, state):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": state}}
        assert MwaaTaskSensor(**SENSOR_TASK_KWARGS, **SENSOR_STATE_KWARGS).poke({})

    @pytest.mark.parametrize("state", ["e", "f"])
    def test_poke_not_completed(self, mock_invoke_rest_api, state):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": state}}
        assert not MwaaTaskSensor(**SENSOR_TASK_KWARGS, **SENSOR_STATE_KWARGS).poke({})

    @pytest.mark.parametrize("state", SENSOR_STATE_KWARGS["failure_states"])
    def test_poke_terminated(self, mock_invoke_rest_api, state):
        mock_invoke_rest_api.return_value = {"RestApiResponse": {"state": state}}
        with pytest.raises(AirflowException, match=f".*{state}.*"):
            MwaaTaskSensor(**SENSOR_TASK_KWARGS, **SENSOR_STATE_KWARGS).poke({})
