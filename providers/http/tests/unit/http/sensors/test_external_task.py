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

from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("Waiting for a remote Airflow deployment needs Airflow 3+", allow_module_level=True)

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.http.sensors.external_task import HttpExternalTaskSensor
from airflow.providers.http.triggers.external_task import HttpExternalTaskTrigger
from airflow.providers.standard.exceptions import ExternalTaskFailedError
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

LOGICAL_DATE = datetime(2026, 1, 2, tzinfo=timezone.utc)


@pytest.fixture
def client():
    with mock.patch(
        "airflow.providers.http.sensors.external_task._AirflowApiClient", autospec=True
    ) as client_class:
        yield client_class.return_value


@pytest.fixture
def context():
    return {"logical_date": LOGICAL_DATE, "ti": mock.create_autospec(RuntimeTaskInstance, instance=True)}


def create_sensor(**kwargs) -> HttpExternalTaskSensor:
    return HttpExternalTaskSensor(
        task_id="wait", http_conn_id="remote_airflow", external_dag_id="remote_dag", **kwargs
    )


class TestHttpExternalTaskSensor:
    def test_attributes(self):
        sensor = create_sensor()

        assert sensor._client.http_conn_id == "remote_airflow"
        assert "http_conn_id" in sensor.template_fields
        assert not sensor.operator_extra_links

    @pytest.mark.parametrize(("count", "expected"), [(1, True), (0, False)])
    def test_poke_dag(self, client, context, count, expected):
        client.get_dr_count.return_value = count

        assert create_sensor(execution_delta=timedelta(days=1)).poke(context) is expected
        client.get_dr_count.assert_called_once_with(
            "remote_dag", [LOGICAL_DATE - timedelta(days=1)], ["success"]
        )
        context["ti"].get_dr_count.assert_not_called()

    def test_poke_tasks(self, client, context):
        client.get_ti_count.return_value = 1

        with pytest.raises(ExternalTaskFailedError):
            create_sensor(external_task_ids=["t1", "t2"], failed_states=["failed"]).poke(context)
        client.get_ti_count.assert_called_once_with("remote_dag", ["t1", "t2"], [LOGICAL_DATE], ["failed"])
        context["ti"].get_ti_count.assert_not_called()

    @pytest.mark.parametrize(
        ("task_states", "expected"),
        [
            ({"run_1": {"g.a": "success", "g.b_0": "success"}}, True),
            ({"run_1": {"g.a": "success", "g.b_0": "running"}}, False),
        ],
        ids=["all_allowed", "partially_allowed"],
    )
    def test_poke_task_group(self, client, context, task_states, expected):
        client.get_task_group_states.return_value = task_states

        assert create_sensor(external_task_group_id="g").poke(context) is expected
        client.get_task_group_states.assert_called_once_with("remote_dag", "g", [LOGICAL_DATE])
        context["ti"].get_task_states.assert_not_called()

    def test_execute_deferrable(self, context):
        sensor = create_sensor(
            external_task_ids=["t1"], failed_states=["failed"], deferrable=True, poke_interval=30
        )

        with pytest.raises(TaskDeferred) as deferred:
            sensor.execute(context)

        trigger = deferred.value.trigger
        assert isinstance(trigger, HttpExternalTaskTrigger)
        assert trigger.http_conn_id == "remote_airflow"
        assert trigger.external_dag_id == "remote_dag"
        assert trigger.external_task_ids == ["t1"]
        assert trigger.failed_states == ["failed"]
        assert trigger.logical_dates == [LOGICAL_DATE]
        assert trigger.poke_interval == 30
