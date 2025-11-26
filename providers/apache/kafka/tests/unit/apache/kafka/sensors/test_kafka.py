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
import logging

import pytest

from airflow.models import Connection
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor, AwaitMessageTriggerFunctionSensor
from airflow.providers.common.compat.sdk import TaskDeferred

log = logging.getLogger(__name__)


def _return_true(message):
    return True


class TestSensors:
    """
    Test Sensors
    """

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="kafka_d",
                conn_type="kafka",
                extra=json.dumps(
                    {"socket.timeout.ms": 10, "bootstrap.servers": "localhost:9092", "group.id": "test_group"}
                ),
            )
        )

    def test_await_message_good(self):
        sensor = AwaitMessageSensor(
            kafka_config_id="kafka_d", topics=["test"], task_id="test", apply_function=_return_true
        )

        # execute marks the task as deferred
        with pytest.raises(TaskDeferred):
            sensor.execute(context={})

    def test_await_execute_complete(self):
        sensor = AwaitMessageSensor(
            kafka_config_id="kafka_d", topics=["test"], task_id="test", apply_function=_return_true
        )

        assert sensor.execute_complete(context={}, event="test") == "test"

    def test_await_message_trigger_event(self):
        sensor = AwaitMessageTriggerFunctionSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
            event_triggered_function=_return_true,
        )

        # task should immediately come out of deferred
        with pytest.raises(TaskDeferred):
            sensor.execute(context={})

    def test_await_message_trigger_event_execute_complete(self):
        sensor = AwaitMessageTriggerFunctionSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
            event_triggered_function=_return_true,
        )

        # task should immediately come out of deferred
        with pytest.raises(TaskDeferred):
            sensor.execute_complete(context={})

    def test_await_message_with_timeout_parameter(self):
        """Test that AwaitMessageSensor accepts timeout parameter."""
        sensor = AwaitMessageSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
            timeout=600,  # This should now work without errors
        )

        assert sensor.timeout == 600

    def test_await_message_with_soft_fail_parameter(self):
        """Test that AwaitMessageSensor accepts soft_fail parameter."""
        sensor = AwaitMessageSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
            soft_fail=True,  # This should now work without errors
        )

        assert sensor.soft_fail is True

    def test_await_message_trigger_function_with_timeout_parameter(self):
        """Test that AwaitMessageTriggerFunctionSensor accepts timeout parameter."""
        sensor = AwaitMessageTriggerFunctionSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
            event_triggered_function=_return_true,
            timeout=600,
        )

        assert sensor.timeout == 600

    def test_await_message_trigger_function_with_soft_fail_parameter(self):
        """Test that AwaitMessageTriggerFunctionSensor accepts soft_fail parameter."""
        sensor = AwaitMessageTriggerFunctionSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
            event_triggered_function=_return_true,
            soft_fail=True,
        )

        assert sensor.soft_fail is True
