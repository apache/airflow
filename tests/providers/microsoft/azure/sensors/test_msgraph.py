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

from airflow.providers.microsoft.azure.sensors.msgraph import MSGraphSensor
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import TaskInstanceState
from tests.providers.microsoft.azure.base import Base, MockedTaskInstance
from tests.providers.microsoft.conftest import load_json, mock_json_response


class TestMSGraphSensor(Base):
    def test_execute(self):
        status = load_json("resources", "status.json")
        response = mock_json_response(200, status)

        with self.patch_hook_and_request_adapter(response):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/0a1b1bf3-37de-48f7-9863-ed4cda97a9ef",
                timeout=350.0,
            )
            actual = sensor.execute(
                context=MockedTaskInstance(task=sensor, run_id="run_id", state=TaskInstanceState.RUNNING)
            )

            assert isinstance(actual, TriggerEvent)
            assert actual.payload["status"] == "success"
            assert actual.payload["type"] == "builtins.dict"
            assert actual.payload["response"] == json.dumps(status)
