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
import locale
from base64 import b64encode
from unittest.mock import patch

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.triggers.msgraph import MSGraphTrigger
from airflow.triggers.base import TriggerEvent
from tests.providers.microsoft.azure.base import Base
from tests.providers.microsoft.conftest import (
    get_airflow_connection,
    load_file,
    load_json,
    mock_json_response,
    mock_response,
)


class TestMSGraphTrigger(Base):
    def test_run_when_valid_response(self):
        users = load_json("resources", "users.json")
        response = mock_json_response(200, users)

        with self.patch_hook_and_request_adapter(response):
            trigger = MSGraphTrigger("users/delta", conn_id="msgraph_api")
            actual = self.run_trigger(trigger)

            assert len(actual) == 1
            assert isinstance(actual[0], TriggerEvent)
            assert actual[0].payload["status"] == "success"
            assert actual[0].payload["type"] == "builtins.dict"
            assert actual[0].payload["response"] == json.dumps(users)

    def test_run_when_response_is_none(self):
        response = mock_json_response(200)

        with self.patch_hook_and_request_adapter(response):
            trigger = MSGraphTrigger("users/delta", conn_id="msgraph_api")
            actual = self.run_trigger(trigger)

            assert len(actual) == 1
            assert isinstance(actual[0], TriggerEvent)
            assert actual[0].payload["status"] == "success"
            assert actual[0].payload["type"] is None
            assert actual[0].payload["response"] is None

    def test_run_when_response_cannot_be_converted_to_json(self):
        with self.patch_hook_and_request_adapter(AirflowException()):
            trigger = MSGraphTrigger("users/delta", conn_id="msgraph_api")
            actual = next(iter(self.run_trigger(trigger)))

            assert isinstance(actual, TriggerEvent)
            assert actual.payload["status"] == "failure"
            assert actual.payload["message"] == ""

    def test_run_when_response_is_bytes(self):
        content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
        base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
        response = mock_response(200, content)

        with self.patch_hook_and_request_adapter(response):
            url = (
                "https://graph.microsoft.com/v1.0/me/drive/items/1b30fecf-4330-4899-b249-104c2afaf9ed/content"
            )
            trigger = MSGraphTrigger(url, response_type="bytes", conn_id="msgraph_api")
            actual = next(iter(self.run_trigger(trigger)))

            assert isinstance(actual, TriggerEvent)
            assert actual.payload["status"] == "success"
            assert actual.payload["type"] == "builtins.bytes"
            assert isinstance(actual.payload["response"], str)
            assert actual.payload["response"] == base64_encoded_content

    def test_serialize(self):
        with patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=get_airflow_connection,
        ):
            url = "https://graph.microsoft.com/v1.0/me/drive/items"
            trigger = MSGraphTrigger(url, response_type="bytes", conn_id="msgraph_api")

            actual = trigger.serialize()

            assert isinstance(actual, tuple)
            assert actual[0] == "airflow.providers.microsoft.azure.triggers.msgraph.MSGraphTrigger"
            assert actual[1] == {
                "url": "https://graph.microsoft.com/v1.0/me/drive/items",
                "path_parameters": None,
                "url_template": None,
                "method": "GET",
                "query_parameters": None,
                "headers": None,
                "content": None,
                "response_type": "bytes",
                "conn_id": "msgraph_api",
                "timeout": None,
                "proxies": None,
                "api_version": "v1.0",
                "serializer": "airflow.providers.microsoft.azure.serialization.serializer.ResponseSerializer"
            }
