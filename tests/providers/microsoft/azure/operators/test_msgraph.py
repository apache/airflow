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
import json
import locale
from base64 import b64encode
from unittest.mock import patch

import pytest
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator
from kiota_http.httpx_request_adapter import HttpxRequestAdapter

from airflow.exceptions import AirflowException
from airflow.triggers.base import TriggerEvent
from tests.providers.microsoft.azure.base import Base
from tests.providers.microsoft.conftest import load_json, mock_json_response, get_airflow_connection, \
    load_file, mock_response


class TestMSGraphAsyncOperator(Base):
    def test_run_when_expression_is_valid(self):
        users = load_json("resources", "users.json")
        next_users = load_json("resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message", return_value=response),
        ):

            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
                result_processor=lambda context, result: result.get("value")
            )

            results, events = self.execute_operator(operator)

            assert len(results) == 30
            assert results == users.get("value") + next_users.get("value")
            assert len(events) == 2
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(users)
            assert isinstance(events[1], TriggerEvent)
            assert events[1].payload["status"] == "success"
            assert events[1].payload["type"] == "builtins.dict"
            assert events[1].payload["response"] == json.dumps(next_users)

    def test_run_when_expression_is_valid_and_do_xcom_push_is_false(self):
        users = load_json("resources", "users.json")
        users.pop("@odata.nextLink")
        response = mock_json_response(200, users)

        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message", return_value=response),
        ):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                do_xcom_push=False,
            )

            results, events = self.execute_operator(operator)

            assert isinstance(results, dict)
            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(users)

    def test_run_when_an_exception_occurs(self):
        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter,"get_http_response_message", side_effect=AirflowException()),
        ):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                do_xcom_push=False,
            )

            with pytest.raises(AirflowException):
                self.execute_operator(operator)

    def test_run_when_url_which_returns_bytes(self):
        content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
        base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
        drive_id = "82f9d24d-6891-4790-8b6d-f1b2a1d0ca22"
        response = mock_response(200, content)

        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message", return_value=response),
        ):
            operator = MSGraphAsyncOperator(
                task_id="drive_item_content",
                conn_id="msgraph_api",
                response_type="bytes",
                url=f"/drives/{drive_id}/root/content",
            )

            results, events = self.execute_operator(operator)

            assert results == base64_encoded_content
            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.bytes"
            assert events[0].payload["response"] == base64_encoded_content
