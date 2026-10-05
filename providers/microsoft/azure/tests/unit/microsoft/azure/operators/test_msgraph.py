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
import warnings
from base64 import b64encode
from io import BytesIO
from os.path import dirname
from typing import Any
from unittest import mock

import pytest
from msgraph_core import APIVersion

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.trigger import Trigger
from airflow.providers.common.compat.sdk import DAG, AirflowException, Context, TaskDeferred, timezone
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator, execute_callable
from airflow.providers.microsoft.azure.triggers.msgraph import MSGraphTrigger
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.file_loading import load_file_from_resources, load_json_from_resources
from tests_common.test_utils.mock_context import mock_context
from tests_common.test_utils.operators.run_deferrable import execute_operator, run_trigger
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS
from unit.microsoft.azure.test_utils import (
    mock_json_response,
    mock_response,
    patch_hook_and_request_adapter,
)


class TestMSGraphAsyncOperator:
    def test_execute_with_old_result_processor_signature(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_users = load_json_from_resources(dirname(__file__), "..", "resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with patch_hook_and_request_adapter(response):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
                result_processor=lambda context, result: result.get("value"),
            )

            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="result_processor signature has changed, result parameter should be defined before context!",
            ):
                results, events = execute_operator(operator)

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

    def test_execute_with_new_result_processor_signature(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_users = load_json_from_resources(dirname(__file__), "..", "resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with patch_hook_and_request_adapter(response):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
                result_processor=lambda result, **context: result.get("value"),
            )

            results, events = execute_operator(operator)

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

    def test_execute_with_old_paginate_function_signature(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_users = load_json_from_resources(dirname(__file__), "..", "resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with patch_hook_and_request_adapter(response):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
                result_processor=lambda result, **context: result.get("value"),
                pagination_function=lambda operator, response, context: MSGraphAsyncOperator.paginate(
                    operator, response, **context
                ),
            )

            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="pagination_function signature has changed, context parameter should be a kwargs argument!",
            ):
                results, events = execute_operator(operator)

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

    def test_execute_when_do_xcom_push_is_false(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        users.pop("@odata.nextLink")
        response = mock_json_response(200, users)

        with patch_hook_and_request_adapter(response):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                do_xcom_push=False,
            )

            results, events = execute_operator(operator)

            assert isinstance(results, dict)
            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.dict"
            assert events[0].payload["response"] == json.dumps(users)

    def test_execute_when_an_exception_occurs(self):
        with patch_hook_and_request_adapter(AirflowException()):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                do_xcom_push=False,
            )

            with pytest.raises(AirflowException):
                execute_operator(operator)

    def test_execute_when_an_exception_occurs_on_custom_event_handler_with_old_signature(self):
        with patch_hook_and_request_adapter(AirflowException("An error occurred")):

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

            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="event_handler signature has changed, event parameter should be defined before context!",
            ):
                results, events = execute_operator(operator)

            assert not results
            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "failure"
            assert events[0].payload["message"] == "An error occurred"

    def test_execute_when_an_exception_occurs_on_custom_event_handler_with_new_signature(self):
        with patch_hook_and_request_adapter(AirflowException("An error occurred")):

            def custom_event_handler(event: dict[Any, Any] | None = None, **context):
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

            results, events = execute_operator(operator)

            assert not results
            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "failure"
            assert events[0].payload["message"] == "An error occurred"

    def test_execute_when_response_is_bytes(self):
        content = load_file_from_resources(
            dirname(__file__), "..", "resources", "dummy.pdf", mode="rb", encoding=None
        )
        base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
        drive_id = "82f9d24d-6891-4790-8b6d-f1b2a1d0ca22"
        response = mock_response(200, content)

        with patch_hook_and_request_adapter(response):
            operator = MSGraphAsyncOperator(
                task_id="drive_item_content",
                conn_id="msgraph_api",
                response_type="bytes",
                url="/drives/{drive_id}/root/content",
                path_parameters={"drive_id": drive_id},
            )

            results, events = execute_operator(operator)

            assert operator.path_parameters == {"drive_id": drive_id}
            assert results == base64_encoded_content
            assert len(events) == 1
            assert isinstance(events[0], TriggerEvent)
            assert events[0].payload["status"] == "success"
            assert events[0].payload["type"] == "builtins.bytes"
            assert events[0].payload["response"] == base64_encoded_content

    def test_execute_with_lambda_parameter_when_response_is_bytes(self):
        content = load_file_from_resources(
            dirname(__file__), "..", "resources", "dummy.pdf", mode="rb", encoding=None
        )
        base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
        drive_id = "82f9d24d-6891-4790-8b6d-f1b2a1d0ca22"
        response = mock_response(200, content)

        with patch_hook_and_request_adapter(response):
            operator = MSGraphAsyncOperator(
                task_id="drive_item_content",
                conn_id="msgraph_api",
                response_type="bytes",
                url="/drives/{drive_id}/root/content",
                path_parameters=lambda context, jinja_env: {"drive_id": drive_id},
            )

            results, events = execute_operator(operator)

            assert operator.path_parameters == {"drive_id": drive_id}
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
        response = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_link, query_parameters = MSGraphAsyncOperator.paginate(operator, response, **context)

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
        response = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        response["@odata.count"] = 100
        url, query_parameters = MSGraphAsyncOperator.paginate(operator, response, **context)

        assert url == "users"
        assert query_parameters == {"$skip": 12, "$top": 12}

    def test_trigger_next_link_forwards_the_request_configuration(self):
        headers = {"ConsistencyLevel": "eventual"}
        data = {"requestBody": "value"}
        scopes = ["https://graph.microsoft.com/.default"]
        operator = MSGraphAsyncOperator(
            task_id="user_license_details",
            conn_id="msgraph_api",
            url="users",
            headers=headers,
            data=data,
            scopes=scopes,
        )
        context = mock_context(task=operator)
        response = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")

        with mock.patch.object(operator, "defer") as mock_defer:
            operator.trigger_next_link(response, method_name="execute_complete", context=context)

        trigger = mock_defer.call_args.kwargs["trigger"]
        assert trigger.headers == headers
        assert trigger.data == data
        assert trigger.scopes == scopes

    def test_trigger_next_link_forwards_the_path_parameters(self):
        path_parameters = {"user_id": "48d31887-5fad-4d73-a9f5-3c356e68a038", "mailFolder_id": "inbox"}
        operator = MSGraphAsyncOperator(
            task_id="messages",
            conn_id="msgraph_api",
            url="users/{user_id}/mailFolders/{mailFolder_id}/messages",
            path_parameters=path_parameters,
            query_parameters={"$top": 12, "$count": True},
        )
        context = mock_context(task=operator)
        messages = load_json_from_resources(dirname(__file__), "..", "resources", "messages.json")

        with mock.patch.object(operator, "defer") as mock_defer:
            operator.trigger_next_link(messages, method_name="execute_complete", context=context)

        trigger = mock_defer.call_args.kwargs["trigger"]
        assert trigger.url == "users/{user_id}/mailFolders/{mailFolder_id}/messages"
        assert trigger.path_parameters == path_parameters

    def test_execute_complete_advances_the_skip_offset_it_was_resumed_with(self):
        # The operator is rebuilt from the serialized Dag on every page, so its own query parameters
        # describe the first request; only the resume kwargs know where the completed page came from.
        operator = MSGraphAsyncOperator(
            task_id="messages",
            conn_id="msgraph_api",
            url="users/messages",
            query_parameters={"$top": 12, "$count": True},
        )
        context = mock_context(task=operator)
        second_messages = load_json_from_resources(
            dirname(__file__), "..", "resources", "second_messages.json"
        )
        event = {"status": "success", "type": "builtins.dict", "response": json.dumps(second_messages)}

        with mock.patch.object(operator, "defer") as mock_defer:
            operator.execute_complete(
                context=context,
                event=event,
                query_parameters={"$top": 12, "$count": True, "$skip": 12},
            )

        assert mock_defer.call_args.kwargs["trigger"].query_parameters == {
            "$top": 12,
            "$count": True,
            "$skip": 24,
        }
        assert mock_defer.call_args.kwargs["kwargs"] == {
            "query_parameters": {"$top": 12, "$count": True, "$skip": 24}
        }

    def test_skip_pagination_expands_the_url_template_on_every_page(self):
        messages = load_json_from_resources(dirname(__file__), "..", "resources", "messages.json")
        second_messages = load_json_from_resources(
            dirname(__file__), "..", "resources", "second_messages.json"
        )
        third_messages = load_json_from_resources(dirname(__file__), "..", "resources", "third_messages.json")
        response = mock_json_response(200, messages, second_messages, third_messages)

        with patch_hook_and_request_adapter(response) as (*_, mock_get_http_response):
            operator = MSGraphAsyncOperator(
                task_id="messages",
                conn_id="msgraph_api",
                url="users/{user_id}/mailFolders/{mailFolder_id}/messages",
                path_parameters={"user_id": "48d31887-5fad-4d73-a9f5-3c356e68a038", "mailFolder_id": "inbox"},
                query_parameters={"$top": 12, "$count": True},
                result_processor=lambda result, **context: result.get("value"),
            )

            execute_operator(operator)

        urls = [call.args[0].url for call in mock_get_http_response.call_args_list]

        assert urls == [
            "users/48d31887-5fad-4d73-a9f5-3c356e68a038/mailFolders/inbox/messages?%24top=12&%24count=true",
            "users/48d31887-5fad-4d73-a9f5-3c356e68a038/mailFolders/inbox/messages?%24top=12&%24count=true&%24skip=12",
            "users/48d31887-5fad-4d73-a9f5-3c356e68a038/mailFolders/inbox/messages?%24top=12&%24count=true&%24skip=24",
        ]

    def test_pagination_issues_every_page_with_the_configured_request(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_users = load_json_from_resources(dirname(__file__), "..", "resources", "next_users.json")
        response = mock_json_response(200, users, next_users)
        headers = {"ConsistencyLevel": "eventual"}
        data = {"requestBody": "value"}

        with patch_hook_and_request_adapter(response) as (*_, mock_get_http_response):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
                method="POST",
                headers=headers,
                data=data,
                result_processor=lambda result, **context: result.get("value"),
            )

            execute_operator(operator)

        requests = [call.args[0] for call in mock_get_http_response.call_args_list]

        assert len(requests) == 2
        for request in requests:
            assert request.headers.try_get("ConsistencyLevel") == {"eventual"}
            assert request.content == json.dumps(data).encode("utf-8")

    def test_pagination_refuses_cross_host_next_link(self):
        first_page = {
            "@odata.nextLink": "https://attacker.example/v1.0/users?$skiptoken=steal",
            "value": [{"id": "1"}],
        }
        second_page = {"value": [{"id": "2"}]}
        response = mock_json_response(200, first_page, second_page)

        with patch_hook_and_request_adapter(response) as (*_, mock_get_http_response):
            operator = MSGraphAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
            )

            with pytest.raises(AirflowException, match="attacker.example"):
                execute_operator(operator)

        # assert_allowed_host rejects the link before the request goes out, so the second page is never
        # fetched and the bearer token does not reach attacker.example.
        assert mock_get_http_response.call_count == 1

    def test_relative_pagination_link_is_not_treated_as_cross_host(self):
        pages = [{"next": "users?$skip=1", "value": [{"id": "1"}]}, {"value": [{"id": "2"}]}]
        response = mock_json_response(200, *pages)

        with patch_hook_and_request_adapter(response) as (*_, mock_get_http_response):
            operator = MSGraphAsyncOperator(
                task_id="users",
                conn_id="msgraph_api",
                url="users",
                pagination_function=lambda operator, response, **context: (response.get("next"), None),
            )

            results, _ = execute_operator(operator)

        # A pagination function may return a relative url, whose netloc is empty and never matches the
        # configured endpoint. The startswith("http") check in assert_allowed_host lets it pass.
        assert mock_get_http_response.call_count == 2
        assert results == pages

    def test_execute_callable(self):
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="result_processor signature has changed, result parameter should be defined before context!",
        ):
            assert (
                execute_callable(
                    lambda context, response: response,
                    "response",
                    Context({"execution_date": timezone.utcnow()}),
                    "result_processor signature has changed, result parameter should be defined before context!",
                )
                == "response"
            )

        with warnings.catch_warnings(record=True) as recorded_warnings:
            warnings.simplefilter("error")  # Treat warnings as errors
            assert (
                execute_callable(
                    lambda response, **context: response,
                    "response",
                    Context({"execution_date": timezone.utcnow()}),
                    "result_processor signature has changed, result parameter should be defined before context!",
                )
                == "response"
            )
            assert len(recorded_warnings) == 0


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The triggerer renders templated fields from Airflow 3.3")
class TestMSGraphAsyncOperatorStartFromTrigger:
    def test_start_from_trigger_builds_the_trigger_arguments(self):
        operator = MSGraphAsyncOperator(
            task_id="user_messages",
            conn_id="{{ params.conn_id }}",
            url="users/{user_id}/messages",
            response_type="bytes",
            path_parameters={"user_id": "{{ params.user_id }}"},
            url_template="{+baseurl}/users/{user_id}/messages{?%24top}",
            method="POST",
            query_parameters={"$top": 12},
            headers={"ConsistencyLevel": "eventual"},
            data={"subject": "{{ ds }}"},
            timeout=30,
            proxies={"https": "http://proxy:3128"},
            scopes=["https://graph.microsoft.com/.default"],
            api_version=APIVersion.beta,
            start_from_trigger=True,
        )

        assert operator.start_from_trigger is True
        assert operator.start_trigger_args.trigger_cls == (
            "airflow.providers.microsoft.azure.triggers.msgraph.MSGraphTrigger"
        )
        assert operator.start_trigger_args.next_method == "execute_complete"
        assert operator.start_trigger_args.next_kwargs is None
        assert operator.start_trigger_args.timeout is None
        assert operator.start_trigger_args.trigger_kwargs == {
            "url": "users/{user_id}/messages",
            "response_type": "bytes",
            "path_parameters": {"user_id": "{{ params.user_id }}"},
            "url_template": "{+baseurl}/users/{user_id}/messages{?%24top}",
            "method": "POST",
            "query_parameters": {"$top": 12},
            "headers": {"ConsistencyLevel": "eventual"},
            "data": {"subject": "{{ ds }}"},
            "conn_id": "{{ params.conn_id }}",
            "timeout": 30,
            "proxies": {"https": "http://proxy:3128"},
            "scopes": ["https://graph.microsoft.com/.default"],
            "api_version": "beta",
            "serializer": "airflow.providers.microsoft.azure.triggers.msgraph.ResponseSerializer",
        }
        # The enum itself would not survive the serialized Dag, its value does.
        assert type(operator.start_trigger_args.trigger_kwargs["api_version"]) is str

    def test_start_from_trigger_defers_the_same_trigger_as_execute(self):
        operator = MSGraphAsyncOperator(
            task_id="user_messages",
            conn_id="msgraph_api",
            url="users/{user_id}/messages",
            path_parameters={"user_id": "{{ params.user_id }}"},
            query_parameters={"$top": 12},
            api_version=APIVersion.beta,
            start_from_trigger=True,
        )

        with pytest.raises(TaskDeferred) as deferred:
            operator.execute(context=Context())

        trigger = MSGraphTrigger(**operator.start_trigger_args.trigger_kwargs)

        assert trigger.serialize() == deferred.value.trigger.serialize()
        assert operator.start_trigger_args.next_method == deferred.value.method_name

    @pytest.mark.parametrize(
        "arguments",
        [
            pytest.param(lambda upstream: {"url": upstream.output}, id="xcom-arg"),
            pytest.param(
                lambda upstream: {"path_parameters": {"user_id": upstream.output}}, id="nested-xcom-arg"
            ),
            pytest.param(
                lambda upstream: {"query_parameters": {"$select": [upstream.output]}},
                id="xcom-arg-in-list",
            ),
            pytest.param(
                lambda upstream: {"path_parameters": lambda context, jinja_env: {"user_id": "me"}},
                id="callable",
            ),
            pytest.param(lambda upstream: {"data": BytesIO(b"content")}, id="file-like-object"),
        ],
    )
    def test_start_from_trigger_falls_back_to_the_worker(self, arguments):
        with DAG(dag_id="msgraph_start_from_trigger", schedule=None):
            upstream = MSGraphAsyncOperator(task_id="upstream", conn_id="msgraph_api", url="users")
            operator = MSGraphAsyncOperator(
                **{
                    "task_id": "user_messages",
                    "conn_id": "msgraph_api",
                    "url": "users/{user_id}/messages",
                    "start_from_trigger": True,
                    **arguments(upstream),
                }
            )

        assert operator.start_from_trigger is False
        assert operator.start_trigger_args is None

    @mock.patch("airflow.providers.microsoft.azure.operators.msgraph.AIRFLOW_V_3_3_PLUS", False)
    def test_start_from_trigger_needs_airflow_3_3(self):
        operator = MSGraphAsyncOperator(
            task_id="users", conn_id="msgraph_api", url="users", start_from_trigger=True
        )

        assert operator.start_from_trigger is False
        assert operator.start_trigger_args is None

    def test_start_trigger_args_are_not_shared_between_tasks(self):
        users = MSGraphAsyncOperator(
            task_id="users", conn_id="msgraph_api", url="users", start_from_trigger=True
        )
        groups = MSGraphAsyncOperator(
            task_id="groups", conn_id="msgraph_api", url="groups", start_from_trigger=True
        )
        sites = MSGraphAsyncOperator(task_id="sites", conn_id="msgraph_api", url="sites")

        assert users.start_trigger_args is not groups.start_trigger_args
        assert users.start_trigger_args.trigger_kwargs["url"] == "users"
        assert groups.start_trigger_args.trigger_kwargs["url"] == "groups"
        assert sites.start_from_trigger is False
        assert sites.start_trigger_args is None
        assert MSGraphAsyncOperator.start_from_trigger is False
        assert MSGraphAsyncOperator.start_trigger_args is None

    def test_mapped_task_has_no_start_trigger_args(self):
        with DAG(dag_id="msgraph_start_from_trigger", schedule=None):
            mapped = MSGraphAsyncOperator.partial(
                task_id="users", conn_id="msgraph_api", start_from_trigger=True
            ).expand(url=["users", "groups"])

        # The scheduler cannot expand the arguments of a mapped task, and without them it leaves
        # the task to a worker.
        assert mapped.start_trigger_args is None

    @pytest.mark.db_test
    @pytest.mark.need_serialized_dag
    def test_scheduler_defers_the_task_to_the_triggerer(self, dag_maker, session):
        with dag_maker(session=session):
            users = MSGraphAsyncOperator(
                task_id="users",
                conn_id="msgraph_api",
                url="users/{{ params.user_id }}",
                query_parameters={"$top": 12},
                start_from_trigger=True,
            )
            MSGraphAsyncOperator(
                task_id="messages", conn_id="msgraph_api", url=users.output, start_from_trigger=True
            )

        dag_run = dag_maker.create_dagrun()
        task_instances = {}
        for task_id in ("users", "messages"):
            task_instances[task_id] = dag_run.get_task_instance(task_id, session=session)
            task_instances[task_id].task = dag_run.dag.get_task(task_id)

        dag_run.schedule_tis(task_instances.values(), session=session)

        assert task_instances["users"].state == TaskInstanceState.DEFERRED
        assert task_instances["users"].next_method == "execute_complete"
        assert task_instances["users"].trigger_timeout is None
        trigger = session.get(Trigger, task_instances["users"].trigger_id)
        assert trigger.classpath == "airflow.providers.microsoft.azure.triggers.msgraph.MSGraphTrigger"
        assert trigger.kwargs == users.start_trigger_args.trigger_kwargs
        # The task which takes its url from an XCom still starts on a worker.
        session.refresh(task_instances["messages"])
        assert task_instances["messages"].state == TaskInstanceState.SCHEDULED
        assert task_instances["messages"].trigger_id is None

    @pytest.mark.db_test
    def test_triggerer_renders_the_templated_fields(self, create_task_instance):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        users.pop("@odata.nextLink")
        operator = MSGraphAsyncOperator(
            task_id="user_messages",
            conn_id="{{ conn_id }}",
            url="users/{user_id}/messages",
            path_parameters={"user_id": "{{ user_id }}"},
            query_parameters={"$top": "{{ top }}"},
            headers={"ConsistencyLevel": "{{ consistency }}"},
            start_from_trigger=True,
        )
        task_instance = create_task_instance(
            task=operator, start_from_trigger=True, start_trigger_args=operator.start_trigger_args
        )
        trigger = MSGraphTrigger(**operator.start_trigger_args.trigger_kwargs)

        trigger.task_instance = task_instance

        assert trigger.template_fields == MSGraphAsyncOperator.template_fields

        trigger.render_template_fields(
            context={"conn_id": "msgraph_api", "user_id": "me", "top": 12, "consistency": "eventual"}
        )

        assert trigger.conn_id == "msgraph_api"
        assert trigger.path_parameters == {"user_id": "me"}
        assert trigger.query_parameters == {"$top": "12"}
        assert trigger.headers == {"ConsistencyLevel": "eventual"}

        with patch_hook_and_request_adapter(mock_json_response(200, users)) as (
            *_,
            mock_get_http_response,
        ):
            events = run_trigger(trigger)

        request = mock_get_http_response.call_args.args[0]

        assert request.url == "users/me/messages?%24top=12"
        assert request.headers.try_get("ConsistencyLevel") == {"eventual"}
        assert events[0].payload["status"] == "success"
        assert events[0].payload["response"] == json.dumps(users)

    def test_execute_when_started_from_the_trigger(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_users = load_json_from_resources(dirname(__file__), "..", "resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with (
            patch_hook_and_request_adapter(response) as (*_, mock_get_http_response),
            mock.patch.object(MSGraphAsyncOperator, "execute", autospec=True) as mock_execute,
        ):
            operator = MSGraphAsyncOperator(
                task_id="users",
                conn_id="msgraph_api",
                url="{{ ti.task_id }}",
                result_processor=lambda result, **context: result.get("value"),
                start_from_trigger=True,
            )

            results, events = execute_operator(operator)

        # The first page is requested by the trigger the scheduler created, with the url it rendered
        # itself, and the worker only takes over from there to follow the pagination.
        mock_execute.assert_not_called()
        assert mock_get_http_response.call_args_list[0].args[0].url == "users"
        assert mock_get_http_response.call_count == 2
        assert results == users.get("value") + next_users.get("value")
        assert [event.payload["response"] for event in events] == [
            json.dumps(users),
            json.dumps(next_users),
        ]
