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

import json
from unittest.mock import patch

import pytest

from airflow.exceptions import TaskDeferred
from airflow.models import Connection
from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowSensorTimeout,
    AirflowSkipException,
)
from airflow.providers.datadog.sensors.datadog import (
    DatadogMonitorSensorAsync,
    DatadogSensor,
    TaskDeferralError,
)
from airflow.providers.datadog.triggers.datadog import DatadogMonitorTrigger

at_least_one_event = [
    {
        "alert_type": "info",
        "comments": [],
        "date_happened": 1419436860,
        "device_name": None,
        "host": None,
        "id": 2603387619536318140,
        "is_aggregate": False,
        "priority": "normal",
        "resource": "/api/v1/events/2603387619536318140",
        "source": "My Apps",
        "tags": ["application:web", "version:1"],
        "text": "And let me tell you all about it here!",
        "title": "Something big happened!",
        "url": "/event/jump_to?event_id=2603387619536318140",
    },
    {
        "alert_type": "info",
        "comments": [],
        "date_happened": 1419436865,
        "device_name": None,
        "host": None,
        "id": 2603387619536318141,
        "is_aggregate": False,
        "priority": "normal",
        "resource": "/api/v1/events/2603387619536318141",
        "source": "My Apps",
        "tags": ["application:web", "version:1"],
        "text": "And let me tell you all about it here!",
        "title": "Something big happened!",
        "url": "/event/jump_to?event_id=2603387619536318141",
    },
]

zero_events: list = []


class TestDatadogSensor:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="datadog_default",
                conn_type="datadog",
                login="login",
                password="password",
                extra=json.dumps({"api_key": "api_key", "app_key": "app_key"}),
            )
        )

    @patch("airflow.providers.datadog.hooks.datadog.api.Event.query")
    @patch("airflow.providers.datadog.sensors.datadog.api.Event.query")
    def test_sensor_ok(self, api1, api2):
        api1.return_value = at_least_one_event
        api2.return_value = at_least_one_event

        sensor = DatadogSensor(
            task_id="test_datadog",
            datadog_conn_id="datadog_default",
            from_seconds_ago=3600,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None,
        )

        assert sensor.poke({})

    @patch("airflow.providers.datadog.hooks.datadog.api.Event.query")
    @patch("airflow.providers.datadog.sensors.datadog.api.Event.query")
    def test_sensor_fail(self, api1, api2):
        api1.return_value = zero_events
        api2.return_value = zero_events

        sensor = DatadogSensor(
            task_id="test_datadog",
            datadog_conn_id="datadog_default",
            from_seconds_ago=0,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None,
        )

        assert not sensor.poke({})

    @patch("airflow.providers.datadog.hooks.datadog.api.Event.query")
    @patch("airflow.providers.datadog.sensors.datadog.api.Event.query")
    def test_sensor_fail_with_exception(self, api1, api2):
        api1.return_value = zero_events
        api2.return_value = {"status": "error"}

        sensor = DatadogSensor(
            task_id="test_datadog",
            datadog_conn_id="datadog_default",
            from_seconds_ago=0,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None,
        )
        with pytest.raises(AirflowException):
            sensor.poke({})


monitor_ok = {"overall_state": "OK"}
monitor_alert = {"overall_state": "Alert"}
monitor_missing = {"errors": ["Monitor not found"]}


class TestDatadogMonitorSensorAsync:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="datadog_default",
                conn_type="datadog",
                login="login",
                password="password",
                extra=json.dumps({"api_key": "api_key", "app_key": "app_key"}),
            )
        )

    def test_execute_defers_with_trigger(self):
        sensor = DatadogMonitorSensorAsync(task_id="t", monitor_id=1)
        with pytest.raises(TaskDeferred) as ctx:
            sensor.execute({})
        assert isinstance(ctx.value.trigger, DatadogMonitorTrigger)

    def test_execute_complete_raises_on_non_success(self):
        sensor = DatadogMonitorSensorAsync(task_id="t", monitor_id=1)
        with pytest.raises(AirflowException, match="DatadogMonitorTrigger failed"):
            sensor.execute_complete({}, {"status": "error"})

    def test_execute_complete_succeeds(self):
        sensor = DatadogMonitorSensorAsync(task_id="t", monitor_id=1)
        assert sensor.execute_complete({}, {"status": "success", "state": "OK"}) is None

    def test_resume_execution_trigger_failure_fails_despite_soft_fail(self):
        sensor = DatadogMonitorSensorAsync(task_id="t", monitor_id=1, soft_fail=True)
        with pytest.raises(TaskDeferralError):
            sensor.resume_execution(
                next_method="__fail__", next_kwargs={"error": "Trigger failure"}, context={}
            )

    def test_resume_execution_timeout_skips_with_soft_fail(self):
        sensor = DatadogMonitorSensorAsync(task_id="t", monitor_id=1, soft_fail=True)
        with pytest.raises(AirflowSkipException):
            sensor.resume_execution(
                next_method="__fail__", next_kwargs={"error": "Trigger/execution timeout"}, context={}
            )

    def test_resume_execution_timeout_fails_without_soft_fail(self):
        sensor = DatadogMonitorSensorAsync(task_id="t", monitor_id=1)
        with pytest.raises(AirflowSensorTimeout):
            sensor.resume_execution(
                next_method="__fail__", next_kwargs={"error": "Trigger/execution timeout"}, context={}
            )
