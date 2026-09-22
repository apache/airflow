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

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.datadog.triggers.datadog import DatadogMonitorTrigger
from airflow.triggers.base import TriggerEvent

pytestmark = pytest.mark.db_test


def make_trigger():
    return DatadogMonitorTrigger(
        monitor_id=42,
        target_states=["OK"],
        datadog_conn_id="datadog_default",
        poke_interval=0.01,
    )


class TestDatadogMonitorTrigger:
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

    def test_serialize(self):
        classpath, kwargs = make_trigger().serialize()
        assert classpath == "airflow.providers.datadog.triggers.datadog.DatadogMonitorTrigger"
        assert kwargs == {
            "monitor_id": 42,
            "target_states": ["OK"],
            "datadog_conn_id": "datadog_default",
            "poke_interval": 0.01,
        }

    @pytest.mark.asyncio
    @patch("airflow.providers.datadog.triggers.datadog.api.Monitor.get")
    async def test_run_yields_success_when_target_state_reached(self, monitor_get):
        monitor_get.side_effect = [{"overall_state": "Alert"}, {"overall_state": "OK"}]
        events = [event async for event in make_trigger().run()]
        assert events == [TriggerEvent({"status": "success", "state": "OK"})]

    @pytest.mark.asyncio
    @patch("airflow.providers.datadog.triggers.datadog.api.Monitor.get")
    async def test_run_raises_on_api_error(self, monitor_get):
        monitor_get.return_value = {"errors": ["Monitor not found"]}
        with pytest.raises(AirflowException, match="Monitor not found"):
            async for _ in make_trigger().run():
                pass
