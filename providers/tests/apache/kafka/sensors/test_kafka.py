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

from airflow.exceptions import TaskDeferred
from airflow.models import Connection
from airflow.providers.apache.kafka.sensors.kafka import (
    AwaitMessageSensor,
    AwaitMessageTriggerFunctionSensor,
)
from airflow.utils import db

pytestmark = pytest.mark.db_test


log = logging.getLogger(__name__)


def _return_true(message):
    return True


class TestSensors:
    """
    Test Sensors
    """

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kafka_d",
                conn_type="kafka",
                extra=json.dumps(
                    {
                        "socket.timeout.ms": 10,
                        "bootstrap.servers": "localhost:9092",
                        "group.id": "test_group",
                    }
                ),
            )
        )

    def test_await_message_good(self):
        sensor = AwaitMessageSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
        )

        # execute marks the task as deferred
        with pytest.raises(TaskDeferred):
            sensor.execute(context={})

    def test_await_execute_complete(self):
        sensor = AwaitMessageSensor(
            kafka_config_id="kafka_d",
            topics=["test"],
            task_id="test",
            apply_function=_return_true,
        )

        assert "test" == sensor.execute_complete(context={}, event="test")

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
