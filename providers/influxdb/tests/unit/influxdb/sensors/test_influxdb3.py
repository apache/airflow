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

import pandas as pd
import pytest

from airflow.providers.common.compat.sdk import AirflowFailException, AirflowSensorTimeout, TaskDeferred
from airflow.providers.influxdb.sensors.influxdb3 import InfluxDB3Sensor
from airflow.providers.influxdb.triggers.influxdb3 import InfluxDB3SensorTrigger

SQL = """SELECT 1 FROM "events" WHERE time > now() - INTERVAL '1 hour' LIMIT 1"""
CONN_ID = "test_influxdb3_conn"
HOOK_PATH = "airflow.providers.influxdb.sensors.influxdb3.InfluxDB3Hook"


class TestInfluxDB3Sensor:
    def test_init(self):
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL)

        assert sensor.sql == SQL
        assert sensor.influxdb3_conn_id == "influxdb3_default"
        assert sensor.fail_on_empty is False
        assert sensor.deferrable is False
        assert sensor.template_fields == ("sql", "influxdb3_conn_id")
        assert sensor.template_ext == (".sql",)

    @pytest.mark.parametrize(
        ("dataframe", "expected"),
        [
            pytest.param(pd.DataFrame({"literal": [1]}), True, id="numeric-one"),
            pytest.param(pd.DataFrame({"literal": ["ready"]}), True, id="non-empty-string"),
            pytest.param(pd.DataFrame({"literal": []}), False, id="no-rows"),
            pytest.param(pd.DataFrame({"count": [0]}), False, id="numeric-zero"),
            pytest.param(pd.DataFrame({"count": ["0"]}), False, id="string-zero"),
            pytest.param(pd.DataFrame({"value": [None]}), False, id="none"),
        ],
    )
    @mock.patch(HOOK_PATH, autospec=True)
    def test_poke(self, mock_hook_class, dataframe, expected):
        mock_hook_class.return_value.query.return_value = dataframe
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL, influxdb3_conn_id=CONN_ID)

        assert sensor.poke(context={}) is expected
        mock_hook_class.assert_called_once_with(conn_id=CONN_ID)
        mock_hook_class.return_value.query.assert_called_once_with(SQL)

    @mock.patch(HOOK_PATH, autospec=True)
    def test_poke_fail_on_empty(self, mock_hook_class):
        mock_hook_class.return_value.query.return_value = pd.DataFrame({"literal": []})
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL, fail_on_empty=True)

        with pytest.raises(AirflowFailException, match="fail_on_empty"):
            sensor.poke(context={})

    @mock.patch.object(InfluxDB3Sensor, "defer", autospec=True)
    @mock.patch(HOOK_PATH, autospec=True)
    def test_execute_deferrable_fail_on_empty_before_deferral(self, mock_hook_class, mock_defer):
        mock_hook_class.return_value.query.return_value = pd.DataFrame({"literal": []})
        sensor = InfluxDB3Sensor(
            task_id="wait",
            sql=SQL,
            fail_on_empty=True,
            deferrable=True,
        )

        with pytest.raises(AirflowFailException, match="fail_on_empty"):
            sensor.execute(context={})

        mock_hook_class.return_value.query.assert_called_once_with(SQL)
        mock_defer.assert_not_called()

    @mock.patch(HOOK_PATH, autospec=True)
    def test_execute_times_out_when_condition_is_never_met(self, mock_hook_class):
        mock_hook_class.return_value.query.return_value = pd.DataFrame({"literal": []})
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL, poke_interval=0, timeout=0)

        with pytest.raises(AirflowSensorTimeout):
            sensor.execute(context={})

    @mock.patch(HOOK_PATH, autospec=True)
    def test_execute_deferrable_completes_after_initial_match(self, mock_hook_class):
        mock_hook_class.return_value.query.return_value = pd.DataFrame({"literal": [1]})
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL, deferrable=True)

        assert sensor.execute(context={}) is None
        mock_hook_class.return_value.query.assert_called_once_with(SQL)

    @mock.patch(HOOK_PATH, autospec=True)
    def test_execute_deferrable_defers_with_sensor_settings(self, mock_hook_class):
        mock_hook_class.return_value.query.return_value = pd.DataFrame({"literal": []})
        sensor = InfluxDB3Sensor(
            task_id="wait",
            sql=SQL,
            influxdb3_conn_id=CONN_ID,
            fail_on_empty=False,
            deferrable=True,
            poke_interval=30,
            timeout=600,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context={})

        trigger = exc.value.trigger
        assert isinstance(trigger, InfluxDB3SensorTrigger)
        assert trigger.sql == SQL
        assert trigger.influxdb3_conn_id == CONN_ID
        assert trigger.poll_interval == 30
        assert trigger.fail_on_empty is False
        assert exc.value.method_name == "execute_complete"
        assert exc.value.timeout == timedelta(minutes=10)

    def test_execute_complete_success(self):
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL, deferrable=True)

        assert sensor.execute_complete(context={}, event={"status": "success"}) is None

    @pytest.mark.parametrize(
        ("event", "match"),
        [
            pytest.param(
                {"status": "fail", "message": "No rows returned, raising as per fail_on_empty flag"},
                "fail_on_empty",
                id="fail-with-message",
            ),
            pytest.param(
                {"status": "fail"},
                "InfluxDB 3 sensor failed",
                id="fail-without-message",
            ),
        ],
    )
    def test_execute_complete_fail_on_empty(self, event, match):
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL, deferrable=True)

        with pytest.raises(AirflowFailException, match=match):
            sensor.execute_complete(context={}, event=event)

    @pytest.mark.parametrize(
        ("event", "match"),
        [
            pytest.param(None, "did not return an event", id="missing-event"),
            pytest.param({"status": "error", "message": "boom"}, "boom", id="error-with-message"),
            pytest.param({"status": "error"}, "InfluxDB 3 sensor failed", id="error-without-message"),
            pytest.param({"status": "cancelled"}, "unexpected status", id="unexpected-status"),
        ],
    )
    def test_execute_complete_failures(self, event, match):
        sensor = InfluxDB3Sensor(task_id="wait", sql=SQL, deferrable=True)

        with pytest.raises(RuntimeError, match=match):
            sensor.execute_complete(context={}, event=event)
