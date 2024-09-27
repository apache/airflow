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
from typing import Any

import pytest
from airflow.utils.context import Context

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator
from airflow.triggers.base import TriggerEvent
from tests.providers.microsoft.azure.base import Base
from tests.providers.microsoft.conftest import (
    load_file,
    load_json,
    mock_context,
    mock_json_response,
    mock_response,
)


class TestMSGraphAsyncOperator(Base):
    @pytest.mark.db_test
    def test_execute(self):
        users = load_json("resources", "users.json")
        next_users = load_json("resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with self.patch_hook_and_request_adapter(response):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
                result_processor=lambda context, result: result.get("value"),
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

    @pytest.mark.db_test
    def test_execute_when_do_xcom_push_is_false(self):
        users = load_json("resources", "users.json")
        users.pop("@odata.nextLink")
        response = mock_json_response(200, users)

        with self.patch_hook_and_request_adapter(response):
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

    @pytest.mark.db_test
    def test_execute_when_an_exception_occurs(self):
        with self.patch_hook_and_request_adapter(AirflowException()):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                do_xcom_push=False,
            )

            with pytest.raises(AirflowException):
                self.execute_operator(operator)

    @pytest.mark.db_test
    def test_execute_when_an_exception_occurs_on_custom_event_handler(self):
        with self.patch_hook_and_request_adapter(AirflowException("An error occurred")):

            def custom_event_handler(context: Context, event: dict[Any, Any] | None = None):
                if event:
                    if event.get("status") == "failure":
                        return None

                    return event.get("response")

            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                event_handler=custom_event_handler,
            )

            results, events = self.execute_operator(operator)

            assert not results
            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "failure"
            assert events[0].payload["message"] == "An error occurred"

    @pytest.mark.db_test
    def test_execute_when_response_is_bytes(self):
        content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
        base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
        drive_id = "82f9d24d-6891-4790-8b6d-f1b2a1d0ca22"
        response = mock_response(200, content)

        with self.patch_hook_and_request_adapter(response):
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

    def test_template_fields(self):
        operator = MSGraphAsyncOperator(
            task_id="drive_item_content",
            conn_id="msgraph_api",
            url="users/delta",
        )

        for template_field in MSGraphAsyncOperator.template_fields:
            getattr(operator, template_field)

    def test_paginate_without_query_parameters(self):
        operator = MSGraphAsyncOperator(
            task_id="user_license_details",
            conn_id="msgraph_api",
            url="users",
        )
        context = mock_context(task=operator)
        response = load_json("resources", "users.json")
        next_link, query_parameters = MSGraphAsyncOperator.paginate(operator, response, context)

        assert next_link == response["@odata.nextLink"]
        assert query_parameters is None

    def test_paginate_with_context_query_parameters(self):
        operator = MSGraphAsyncOperator(
            task_id="user_license_details",
            conn_id="msgraph_api",
            url="users",
            query_parameters={"$top": 12},
        )
        context = mock_context(task=operator)
        response = load_json("resources", "users.json")
        response["@odata.count"] = 100
        url, query_parameters = MSGraphAsyncOperator.paginate(operator, response, context)

        assert url == "users"
        assert query_parameters == {"$skip": 12, "$top": 12}
