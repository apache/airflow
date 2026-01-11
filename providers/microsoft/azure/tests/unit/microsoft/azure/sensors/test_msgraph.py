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
from datetime import datetime
from os.path import dirname

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.microsoft.azure.sensors.msgraph import MSGraphSensor
from airflow.triggers.base import TriggerEvent

from tests_common.test_utils.file_loading import load_json_from_resources
from tests_common.test_utils.operators.run_deferrable import execute_operator
from unit.microsoft.azure.test_utils import mock_json_response, patch_hook_and_request_adapter


class TestMSGraphSensor:
    def test_execute_with_result_processor_with_old_signature(self):
        status = load_json_from_resources(dirname(__file__), "..", "resources", "status.json")
        response = mock_json_response(200, *status)

        with patch_hook_and_request_adapter(response):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters={"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"},
                result_processor=lambda context, result: result["id"],
                retry_delay=5,
                timeout=5,
            )

            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="result_processor signature has changed, result parameter should be defined before context!",
            ):
                results, events = execute_operator(sensor)

            assert sensor.path_parameters == {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"}
            assert isinstance(results, str)
            assert results == "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"
            assert len(events) == 3
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(status[0])
            assert isinstance(events[1], TriggerEvent)
            assert isinstance(events[1].payload, datetime)
            assert isinstance(events[2], TriggerEvent)
            assert events[2].payload["status"] == "success"
            assert events[2].payload["type"] == "builtins.dict"
            assert events[2].payload["response"] == json.dumps(status[1])

    def test_execute_with_result_processor_with_new_signature(self):
        status = load_json_from_resources(dirname(__file__), "..", "resources", "status.json")
        response = mock_json_response(200, *status)

        with patch_hook_and_request_adapter(response):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters={"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"},
                result_processor=lambda result, **context: result["id"],
                retry_delay=5,
                timeout=5,
            )

            results, events = execute_operator(sensor)

            assert sensor.path_parameters == {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"}
            assert isinstance(results, str)
            assert results == "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"
            assert len(events) == 3
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(status[0])
            assert isinstance(events[1], TriggerEvent)
            assert isinstance(events[1].payload, datetime)
            assert isinstance(events[2], TriggerEvent)
            assert events[2].payload["status"] == "success"
            assert events[2].payload["type"] == "builtins.dict"
            assert events[2].payload["response"] == json.dumps(status[1])

    def test_execute_with_lambda_parameter_and_result_processor_with_new_signature(self):
        status = load_json_from_resources(dirname(__file__), "..", "resources", "status.json")
        response = mock_json_response(200, *status)

        with patch_hook_and_request_adapter(response):
            sensor = MSGraphSensor(
                task_id="check_workspaces_status",
                conn_id="powerbi",
                url="myorg/admin/workspaces/scanStatus/{scanId}",
                path_parameters=lambda context, jinja_env: {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"},
                result_processor=lambda result, **context: result["id"],
                retry_delay=5,
                timeout=5,
            )

            results, events = execute_operator(sensor)

            assert sensor.path_parameters == {"scanId": "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"}
            assert isinstance(results, str)
            assert results == "0a1b1bf3-37de-48f7-9863-ed4cda97a9ef"
            assert len(events) == 3
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(status[0])
            assert isinstance(events[1], TriggerEvent)
            assert isinstance(events[1].payload, datetime)
            assert isinstance(events[2], TriggerEvent)
            assert events[2].payload["status"] == "success"
            assert events[2].payload["type"] == "builtins.dict"
            assert events[2].payload["response"] == json.dumps(status[1])

    def test_template_fields(self):
        sensor = MSGraphSensor(
            task_id="check_workspaces_status",
            conn_id="powerbi",
            url="myorg/admin/workspaces/scanStatus/{scanId}",
        )

        for template_field in MSGraphSensor.template_fields:
            getattr(sensor, template_field)
