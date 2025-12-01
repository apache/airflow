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
import pickle
from datetime import datetime
from typing import TYPE_CHECKING
from unittest import mock

import certifi
import httpx
import pytest
import time_machine
import uuid6
from task_sdk import make_client, make_client_w_dry_run, make_client_w_responses
from uuid6 import uuid7

from airflow.sdk import timezone
from airflow.sdk.api.client import Client, RemoteValidationError, ServerResponseError
from airflow.sdk.api.datamodels._generated import (
    AssetEventsResponse,
    AssetResponse,
    ConnectionResponse,
    DagRunState,
    DagRunStateResponse,
    HITLDetailRequest,
    HITLDetailResponse,
    HITLUser,
    TerminalTIState,
    VariableResponse,
    XComResponse,
)
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    DeferTask,
    ErrorResponse,
    OKResponse,
    PreviousDagRunResult,
    RescheduleTask,
    TaskRescheduleStartDate,
)

if TYPE_CHECKING:
    from time_machine import TimeMachineFixture


class TestClient:
    @pytest.mark.parametrize(
        ("path", "json_response"),
        [
            (
                "/task-instances/1/run",
                {
                    "dag_run": {
                        "dag_id": "test_dag",
                        "run_id": "test_run",
                        "logical_date": "2021-01-01T00:00:00Z",
                        "start_date": "2021-01-01T00:00:00Z",
                        "run_type": "manual",
                        "run_after": "2021-01-01T00:00:00Z",
                        "consumed_asset_events": [],
                    },
                    "max_tries": 0,
                    "should_retry": False,
                },
            ),
        ],
    )
    def test_dry_run(self, path, json_response):
        client = make_client_w_dry_run()
        assert client.base_url == "dry-run://server"

        resp = client.get(path)

        assert resp.status_code == 200
        assert resp.json() == json_response

    @mock.patch("airflow.sdk.api.client.API_SSL_CERT_PATH", "/capath/does/not/exist/")
    def test_add_capath(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200)

        with pytest.raises(FileNotFoundError) as err:
            make_client(httpx.MockTransport(handle_request))

        assert isinstance(err.value, FileNotFoundError)

    @mock.patch("airflow.sdk.api.client.API_TIMEOUT", 60.0)
    def test_timeout_configuration(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200)

        client = make_client(httpx.MockTransport(handle_request))
        assert client.timeout == httpx.Timeout(60.0)

    def test_timeout_can_be_overridden(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            return httpx.Response(status_code=200)

        client = Client(
            base_url="test://server", token="", transport=httpx.MockTransport(handle_request), timeout=120.0
        )
        assert client.timeout == httpx.Timeout(120.0)

    def test_error_parsing(self):
        responses = [
            httpx.Response(422, json={"detail": [{"loc": ["#0"], "msg": "err", "type": "required"}]})
        ]
        client = make_client_w_responses(responses)

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")

        assert isinstance(err.value, ServerResponseError)
        assert isinstance(err.value.detail, list)
        assert err.value.detail == [
            RemoteValidationError(loc=["#0"], msg="err", type="required"),
        ]

    def test_error_parsing_plain_text(self):
        responses = [httpx.Response(422, content=b"Internal Server Error")]
        client = make_client_w_responses(responses)

        with pytest.raises(httpx.HTTPStatusError) as err:
            client.get("http://error")
        assert not isinstance(err.value, ServerResponseError)

    def test_error_parsing_other_json(self):
        responses = [httpx.Response(404, json={"detail": "Not found"})]
        client = make_client_w_responses(responses)

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")
        assert err.value.args == ("Not found",)
        assert err.value.detail is None

    def test_server_response_error_pickling(self):
        responses = [httpx.Response(404, json={"detail": {"message": "Invalid input"}})]
        client = make_client_w_responses(responses)

        with pytest.raises(ServerResponseError) as exc_info:
            client.get("http://error")

        err = exc_info.value
        assert err.args == ("Server returned error",)
        assert err.detail == {"detail": {"message": "Invalid input"}}

        # Check that the error is picklable
        pickled = pickle.dumps(err)
        unpickled = pickle.loads(pickled)

        assert isinstance(unpickled, ServerResponseError)

        # Test that unpickled error has the same attributes as the original
        assert unpickled.response.json() == {"detail": {"message": "Invalid input"}}
        assert unpickled.detail == {"detail": {"message": "Invalid input"}}
        assert unpickled.response.status_code == 404
        assert unpickled.request.url == "http://error"

    def test_retry_handling_unrecoverable_error(self):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            responses: list[httpx.Response] = [
                *[httpx.Response(500, text="Internal Server Error")] * 6,
                httpx.Response(200, json={"detail": "Recovered from error - but will fail before"}),
                httpx.Response(400, json={"detail": "Should not get here"}),
            ]
            client = make_client_w_responses(responses)

            with pytest.raises(httpx.HTTPStatusError) as err:
                client.get("http://error")
            assert not isinstance(err.value, ServerResponseError)
            assert len(responses) == 3

    def test_retry_handling_recovered(self):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            responses: list[httpx.Response] = [
                *[httpx.Response(500, text="Internal Server Error")] * 2,
                httpx.Response(200, json={"detail": "Recovered from error"}),
                httpx.Response(400, json={"detail": "Should not get here"}),
            ]
            client = make_client_w_responses(responses)

            response = client.get("http://error")
            assert response.status_code == 200
            assert len(responses) == 1

    def test_retry_handling_non_retry_error(self):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            responses: list[httpx.Response] = [
                httpx.Response(422, json={"detail": "Somehow this is a bad request"}),
                httpx.Response(400, json={"detail": "Should not get here"}),
            ]
            client = make_client_w_responses(responses)

            with pytest.raises(ServerResponseError) as err:
                client.get("http://error")
            assert len(responses) == 1
            assert err.value.args == ("Somehow this is a bad request",)

    def test_retry_handling_ok(self):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            responses: list[httpx.Response] = [
                httpx.Response(200, json={"detail": "Recovered from error"}),
                httpx.Response(400, json={"detail": "Should not get here"}),
            ]
            client = make_client_w_responses(responses)

            response = client.get("http://error")
            assert response.status_code == 200
            assert len(responses) == 1

    def test_token_renewal(self):
        responses: list[httpx.Response] = [
            httpx.Response(200, json={"ok": "1"}),
            httpx.Response(404, json={"var": "not_found"}, headers={"Refreshed-API-Token": "abc"}),
            httpx.Response(200, json={"ok": "3"}),
        ]
        client = make_client_w_responses(responses)
        response = client.get("/")
        assert response.status_code == 200
        assert client.auth is not None
        assert not client.auth.token
        with pytest.raises(ServerResponseError):
            response = client.get("/")

        # Even thought it was Not Found, we should still respect the header
        assert client.auth is not None
        assert client.auth.token == "abc"

        # Test that the next request is made with that new auth token
        response = client.get("/")
        assert response.status_code == 200
        assert response.request.headers["Authorization"] == "Bearer abc"

    @pytest.mark.parametrize(
        ("status_code", "description"),
        [
            (399, "status code < 400"),
            (301, "3xx redirect status code"),
            (600, "status code >= 600"),
        ],
    )
    def test_server_response_error_invalid_status_codes(self, status_code, description):
        """Test that ServerResponseError.from_response returns None for invalid status codes."""
        response = httpx.Response(status_code, json={"detail": f"Test {description}"})
        assert ServerResponseError.from_response(response) is None


class TestTaskInstanceOperations:
    """
    Test that the TestTaskInstanceOperations class works as expected. While the operations are simple, it
    still catches the basic functionality of the client for task instances including endpoint and
    response parsing.
    """

    def test_task_instance_start(self, make_ti_context):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            # Simulate a successful response from the server that starts a task
            ti_id = uuid6.uuid7()
            start_date = "2024-10-31T12:00:00Z"
            ti_context = make_ti_context(
                start_date=start_date,
                logical_date="2024-10-31T12:00:00Z",
                run_type="manual",
            )

            # ...including a validation that retry really works
            call_count = 0

            def handle_request(request: httpx.Request) -> httpx.Response:
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    return httpx.Response(status_code=500, json={"detail": "Internal Server Error"})
                if request.url.path == f"/task-instances/{ti_id}/run":
                    actual_body = json.loads(request.read())
                    assert actual_body["pid"] == 100
                    assert actual_body["start_date"] == start_date
                    assert actual_body["state"] == "running"
                    return httpx.Response(
                        status_code=200,
                        json=ti_context.model_dump(mode="json"),
                    )
                return httpx.Response(status_code=400, json={"detail": "Bad Request"})

            client = make_client(transport=httpx.MockTransport(handle_request))
            resp = client.task_instances.start(ti_id, 100, start_date)
            assert resp == ti_context
            assert call_count == 3

    @pytest.mark.parametrize(
        "state", [state for state in TerminalTIState if state != TerminalTIState.SUCCESS]
    )
    def test_task_instance_finish(self, state):
        # Simulate a successful response from the server that finishes (moved to terminal state) a task
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                actual_body = json.loads(request.read())
                assert actual_body["end_date"] == "2024-10-31T12:00:00Z"
                assert actual_body["state"] == state
                assert actual_body["rendered_map_index"] == "test"
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        client.task_instances.finish(
            ti_id, state=state, when="2024-10-31T12:00:00Z", rendered_map_index="test"
        )

    def test_task_instance_heartbeat(self):
        # Simulate a successful response from the server that sends a heartbeat for a ti
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/heartbeat":
                actual_body = json.loads(request.read())
                assert actual_body["pid"] == 100
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        client.task_instances.heartbeat(ti_id, 100)

    def test_task_instance_defer(self):
        # Simulate a successful response from the server that defers a task
        ti_id = uuid6.uuid7()

        msg = DeferTask(
            classpath="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
            next_method="execute_complete",
            trigger_kwargs={
                "__type": "dict",
                "__var": {
                    "moment": {"__type": "datetime", "__var": 1730982899.0},
                    "end_from_trigger": False,
                },
            },
            next_kwargs={"__type": "dict", "__var": {}},
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                actual_body = json.loads(request.read())
                assert actual_body["state"] == "deferred"
                assert actual_body["trigger_kwargs"] == msg.trigger_kwargs
                assert (
                    actual_body["classpath"] == "airflow.providers.standard.triggers.temporal.DateTimeTrigger"
                )
                assert actual_body["next_method"] == "execute_complete"
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        client.task_instances.defer(ti_id, msg)

    def test_task_instance_reschedule(self):
        # Simulate a successful response from the server that reschedules a task
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                actual_body = json.loads(request.read())
                assert actual_body["state"] == "up_for_reschedule"
                assert actual_body["reschedule_date"] == "2024-10-31T12:00:00Z"
                assert actual_body["end_date"] == "2024-10-31T12:00:00Z"
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        msg = RescheduleTask(
            reschedule_date=timezone.parse("2024-10-31T12:00:00Z"),
            end_date=timezone.parse("2024-10-31T12:00:00Z"),
        )
        client.task_instances.reschedule(ti_id, msg)

    def test_task_instance_up_for_retry(self):
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                actual_body = json.loads(request.read())
                assert actual_body["state"] == "up_for_retry"
                assert actual_body["end_date"] == "2024-10-31T12:00:00Z"
                assert actual_body["rendered_map_index"] == "test"
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        client.task_instances.retry(
            ti_id, end_date=timezone.parse("2024-10-31T12:00:00Z"), rendered_map_index="test"
        )

    @pytest.mark.parametrize(
        "rendered_fields",
        [
            pytest.param({"field1": "rendered_value1", "field2": "rendered_value2"}, id="simple-rendering"),
            pytest.param(
                {
                    "field1": "ClassWithCustomAttributes({'nested1': ClassWithCustomAttributes("
                    "{'att1': 'test', 'att2': 'test2'), "
                    "'nested2': ClassWithCustomAttributes("
                    "{'att3': 'test3', 'att4': 'test4')"
                },
                id="complex-rendering",
            ),
        ],
    )
    def test_taskinstance_set_rtif_success(self, rendered_fields):
        TI_ID = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{TI_ID}/rtif":
                return httpx.Response(
                    status_code=201,
                    json={"message": "Rendered task instance fields successfully set"},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.task_instances.set_rtif(id=TI_ID, body=rendered_fields)

        assert result == OKResponse(ok=True)

    def test_taskinstance_set_rendered_map_index_success(self):
        TI_ID = uuid6.uuid7()
        rendered_map_index = "Label: task_1"

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{TI_ID}/rendered-map-index":
                actual_body = json.loads(request.read())
                assert request.method == "PATCH"
                # Body should be the string directly, not wrapped in JSON
                assert actual_body == rendered_map_index
                return httpx.Response(status_code=204)
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.task_instances.set_rendered_map_index(id=TI_ID, rendered_map_index=rendered_map_index)

        assert result == OKResponse(ok=True)

    def test_get_count_basic(self):
        """Test basic get_count functionality with just dag_id."""

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/task-instances/count"
            assert request.url.params.get("dag_id") == "test_dag"
            return httpx.Response(200, json=5)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.task_instances.get_count(dag_id="test_dag")
        assert result.count == 5

    def test_get_count_with_all_params(self):
        """Test get_count with all optional parameters."""

        logical_dates_str = ["2024-01-01T00:00:00+00:00", "2024-01-02T00:00:00+00:00"]
        logical_dates = [timezone.parse(d) for d in logical_dates_str]
        task_ids = ["task1", "task2"]
        states = ["success", "failed"]

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/task-instances/count"
            assert request.method == "GET"
            params = request.url.params
            assert params["dag_id"] == "test_dag"
            assert params.get_list("task_ids") == task_ids
            assert params["task_group_id"] == "group1"
            assert params.get_list("logical_dates") == logical_dates_str
            assert params.get_list("run_ids") == []
            assert params.get_list("states") == states
            assert params["map_index"] == "0"
            return httpx.Response(200, json=10)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.task_instances.get_count(
            dag_id="test_dag",
            map_index=0,
            task_ids=task_ids,
            task_group_id="group1",
            logical_dates=logical_dates,
            states=states,
        )
        assert result.count == 10

    def test_get_task_states_basic(self):
        """Test basic get_task_states functionality with just dag_id."""

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/task-instances/states"
            assert request.url.params.get("dag_id") == "test_dag"
            assert request.url.params.get("task_group_id") == "group1"
            return httpx.Response(
                200, json={"task_states": {"run_id": {"group1.task1": "success", "group1.task2": "failed"}}}
            )

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.task_instances.get_task_states(dag_id="test_dag", task_group_id="group1")
        assert result.task_states == {"run_id": {"group1.task1": "success", "group1.task2": "failed"}}

    def test_get_task_states_with_all_params(self):
        """Test get_task_states with all optional parameters."""

        logical_dates_str = ["2024-01-01T00:00:00+00:00", "2024-01-02T00:00:00+00:00"]
        logical_dates = [timezone.parse(d) for d in logical_dates_str]

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/task-instances/states"
            assert request.method == "GET"
            params = request.url.params
            assert params["dag_id"] == "test_dag"
            assert params["task_group_id"] == "group1"
            assert params.get_list("logical_dates") == logical_dates_str
            assert params.get_list("task_ids") == []
            assert params.get_list("run_ids") == []
            assert params.get("map_index") == "0"
            return httpx.Response(
                200, json={"task_states": {"run_id": {"group1.task1": "success", "group1.task2": "failed"}}}
            )

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.task_instances.get_task_states(
            dag_id="test_dag",
            map_index=0,
            task_group_id="group1",
            logical_dates=logical_dates,
        )
        assert result.task_states == {"run_id": {"group1.task1": "success", "group1.task2": "failed"}}


class TestVariableOperations:
    """
    Test that the VariableOperations class works as expected. While the operations are simple, it
    still catches the basic functionality of the client for variables including endpoint and
    response parsing.
    """

    def test_variable_get_success(self):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            # Simulate a successful response from the server with a variable
            # ...including a validation that retry really works
            call_count = 0

            def handle_request(request: httpx.Request) -> httpx.Response:
                nonlocal call_count
                call_count += 1
                if call_count < 2:
                    return httpx.Response(status_code=500, json={"detail": "Internal Server Error"})
                if request.url.path == "/variables/test_key":
                    return httpx.Response(
                        status_code=200,
                        json={"key": "test_key", "value": "test_value"},
                    )
                return httpx.Response(status_code=400, json={"detail": "Bad Request"})

            client = make_client(transport=httpx.MockTransport(handle_request))
            result = client.variables.get(key="test_key")

            assert isinstance(result, VariableResponse)
            assert result.key == "test_key"
            assert result.value == "test_value"
            assert call_count == 2

    def test_variable_not_found(self):
        # Simulate a 404 response from the server
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/variables/non_existent_var":
                return httpx.Response(
                    status_code=404,
                    json={
                        "detail": {
                            "message": "Variable with key 'non_existent_var' not found",
                            "reason": "not_found",
                        }
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))

        resp = client.variables.get(key="non_existent_var")

        assert isinstance(resp, ErrorResponse)
        assert resp.error == ErrorType.VARIABLE_NOT_FOUND
        assert resp.detail == {"key": "non_existent_var"}

    def test_variable_get_500_error(self):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            # Simulate a response from the server returning a 500 error
            def handle_request(request: httpx.Request) -> httpx.Response:
                if request.url.path == "/variables/test_key":
                    return httpx.Response(
                        status_code=500,
                        headers=[("content-Type", "application/json")],
                        json={
                            "reason": "internal_server_error",
                            "message": "Internal Server Error",
                        },
                    )
                return httpx.Response(status_code=400, json={"detail": "Bad Request"})

            client = make_client(transport=httpx.MockTransport(handle_request))
            with pytest.raises(ServerResponseError):
                client.variables.get(
                    key="test_key",
                )

    def test_variable_set_success(self):
        # Simulate a successful response from the server when putting a variable
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/variables/test_key":
                return httpx.Response(
                    status_code=201,
                    json={"message": "Variable successfully set"},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))

        result = client.variables.set(key="test_key", value="test_value", description="test_description")
        assert result == OKResponse(ok=True)

    def test_variable_delete_success(self):
        # Simulate a successful response from the server when deleting a variable
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.method == "DELETE" and request.url.path == "/variables/test_key":
                return httpx.Response(
                    status_code=200,
                    json={"count": 1},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))

        result = client.variables.delete(key="test_key")
        assert result == OKResponse(ok=True)


class TestXCOMOperations:
    """
    Test that the XComOperations class works as expected. While the operations are simple, it
    still catches the basic functionality of the client for xcoms including endpoint and
    response parsing.
    """

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("value1", id="string-value"),
            pytest.param({"key1": "value1"}, id="dict-value"),
            pytest.param('{"key1": "value1"}', id="dict-str-value"),
            pytest.param(["value1", "value2"], id="list-value"),
            pytest.param({"key": "test_key", "value": {"key2": "value2"}}, id="nested-dict-value"),
        ],
    )
    def test_xcom_get_success(self, value):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            # Simulate a successful response from the server when getting an xcom
            # ...including a validation that retry really works
            call_count = 0

            def handle_request(request: httpx.Request) -> httpx.Response:
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    return httpx.Response(status_code=500, json={"detail": "Internal Server Error"})
                if request.url.path == "/xcoms/dag_id/run_id/task_id/key":
                    return httpx.Response(
                        status_code=201,
                        json={"key": "test_key", "value": value},
                    )
                return httpx.Response(status_code=400, json={"detail": "Bad Request"})

            client = make_client(transport=httpx.MockTransport(handle_request))
            result = client.xcoms.get(
                dag_id="dag_id",
                run_id="run_id",
                task_id="task_id",
                key="key",
            )
            assert isinstance(result, XComResponse)
            assert result.key == "test_key"
            assert result.value == value
            assert call_count == 3

    def test_xcom_get_success_with_map_index(self):
        # Simulate a successful response from the server when getting an xcom with map_index passed
        def handle_request(request: httpx.Request) -> httpx.Response:
            if (
                request.url.path == "/xcoms/dag_id/run_id/task_id/key"
                and request.url.params.get("map_index") == "2"
            ):
                return httpx.Response(
                    status_code=201,
                    json={"key": "test_key", "value": "test_value"},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.xcoms.get(
            dag_id="dag_id",
            run_id="run_id",
            task_id="task_id",
            key="key",
            map_index=2,
        )
        assert isinstance(result, XComResponse)
        assert result.key == "test_key"
        assert result.value == "test_value"

    def test_xcom_get_success_with_include_prior_dates(self):
        # Simulate a successful response from the server when getting an xcom with include_prior_dates passed
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/xcoms/dag_id/run_id/task_id/key" and request.url.params.get(
                "include_prior_dates"
            ):
                return httpx.Response(
                    status_code=201,
                    json={"key": "test_key", "value": "test_value"},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.xcoms.get(
            dag_id="dag_id",
            run_id="run_id",
            task_id="task_id",
            key="key",
            include_prior_dates=True,
        )
        assert isinstance(result, XComResponse)
        assert result.key == "test_key"
        assert result.value == "test_value"

    def test_xcom_get_500_error(self):
        with time_machine.travel("2023-01-01T00:00:00Z", tick=False):
            # Simulate a successful response from the server returning a 500 error
            def handle_request(request: httpx.Request) -> httpx.Response:
                if request.url.path == "/xcoms/dag_id/run_id/task_id/key":
                    return httpx.Response(
                        status_code=500,
                        headers=[("content-Type", "application/json")],
                        json={
                            "reason": "invalid_format",
                            "message": "XCom value is not a valid JSON",
                        },
                    )
                return httpx.Response(status_code=400, json={"detail": "Bad Request"})

            client = make_client(transport=httpx.MockTransport(handle_request))
            with pytest.raises(ServerResponseError):
                client.xcoms.get(
                    dag_id="dag_id",
                    run_id="run_id",
                    task_id="task_id",
                    key="key",
                )

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param("value1", id="string-value"),
            pytest.param({"key1": "value1"}, id="dict-value"),
            pytest.param(["value1", "value2"], id="list-value"),
            pytest.param({"key": "test_key", "value": {"key2": "value2"}}, id="nested-dict-value"),
        ],
    )
    def test_xcom_set_success(self, values):
        # Simulate a successful response from the server when setting an xcom
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/xcoms/dag_id/run_id/task_id/key":
                assert json.loads(request.read()) == values
                return httpx.Response(
                    status_code=201,
                    json={"message": "XCom successfully set"},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.xcoms.set(
            dag_id="dag_id",
            run_id="run_id",
            task_id="task_id",
            key="key",
            value=values,
        )
        assert result == OKResponse(ok=True)

    def test_xcom_set_with_map_index(self):
        # Simulate a successful response from the server when setting an xcom with map_index passed
        def handle_request(request: httpx.Request) -> httpx.Response:
            if (
                request.url.path == "/xcoms/dag_id/run_id/task_id/key"
                and request.url.params.get("map_index") == "2"
            ):
                assert json.loads(request.read()) == "value1"
                return httpx.Response(
                    status_code=201,
                    json={"message": "XCom successfully set"},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.xcoms.set(
            dag_id="dag_id",
            run_id="run_id",
            task_id="task_id",
            key="key",
            value="value1",
            map_index=2,
        )
        assert result == OKResponse(ok=True)

    def test_xcom_set_with_mapped_length(self):
        # Simulate a successful response from the server when setting an xcom with mapped_length
        def handle_request(request: httpx.Request) -> httpx.Response:
            if (
                request.url.path == "/xcoms/dag_id/run_id/task_id/key"
                and request.url.params.get("map_index") == "2"
                and request.url.params.get("mapped_length") == "3"
            ):
                assert json.loads(request.read()) == "value1"
                return httpx.Response(
                    status_code=201,
                    json={"message": "XCom successfully set"},
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.xcoms.set(
            dag_id="dag_id",
            run_id="run_id",
            task_id="task_id",
            key="key",
            value="value1",
            map_index=2,
            mapped_length=3,
        )
        assert result == OKResponse(ok=True)


class TestConnectionOperations:
    """
    Test that the TestConnectionOperations class works as expected. While the operations are simple, it
    still catches the basic functionality of the client for connections including endpoint and
    response parsing.
    """

    def test_connection_get_success(self):
        # Simulate a successful response from the server with a connection
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/connections/test_conn":
                return httpx.Response(
                    status_code=200,
                    json={
                        "conn_id": "test_conn",
                        "conn_type": "mysql",
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.connections.get(conn_id="test_conn")

        assert isinstance(result, ConnectionResponse)
        assert result.conn_id == "test_conn"
        assert result.conn_type == "mysql"

    def test_connection_get_404_not_found(self):
        # Simulate a successful response from the server with a connection
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/connections/test_conn":
                return httpx.Response(
                    status_code=404,
                    json={
                        "reason": "not_found",
                        "message": "Connection with ID test_conn not found",
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.connections.get(conn_id="test_conn")

        assert isinstance(result, ErrorResponse)
        assert result.error == ErrorType.CONNECTION_NOT_FOUND


class TestAssetEventOperations:
    @pytest.mark.parametrize(
        "request_params",
        [
            ({"name": "this_asset", "uri": "s3://bucket/key"}),
            ({"alias_name": "this_asset_alias"}),
        ],
    )
    def test_by_name_get_success(self, request_params):
        def handle_request(request: httpx.Request) -> httpx.Response:
            params = request.url.params
            if request.url.path == "/asset-events/by-asset":
                assert params.get("name") == request_params.get("name")
                assert params.get("uri") == request_params.get("uri")
            elif request.url.path == "/asset-events/by-asset-alias":
                assert params.get("name") == request_params.get("alias_name")
            else:
                return httpx.Response(status_code=400, json={"detail": "Bad Request"})

            return httpx.Response(
                status_code=200,
                json={
                    "asset_events": [
                        {
                            "id": 1,
                            "asset": {
                                "name": "this_asset",
                                "uri": "s3://bucket/key",
                                "group": "asset",
                            },
                            "created_dagruns": [],
                            "timestamp": "2023-01-01T00:00:00Z",
                        }
                    ]
                },
            )

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.asset_events.get(**request_params)

        assert isinstance(result, AssetEventsResponse)
        assert len(result.asset_events) == 1
        assert result.asset_events[0].asset.name == "this_asset"
        assert result.asset_events[0].asset.uri == "s3://bucket/key"


class TestAssetOperations:
    @pytest.mark.parametrize(
        "request_params",
        [
            ({"name": "this_asset"}),
            ({"uri": "s3://bucket/key"}),
        ],
    )
    def test_by_name_get_success(self, request_params):
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in ("/assets/by-name", "/assets/by-uri"):
                return httpx.Response(
                    status_code=200,
                    json={
                        "name": "this_asset",
                        "uri": "s3://bucket/key",
                        "group": "asset",
                        "extra": {"foo": "bar"},
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.assets.get(**request_params)

        assert isinstance(result, AssetResponse)
        assert result.name == "this_asset"
        assert result.uri == "s3://bucket/key"

    @pytest.mark.parametrize(
        "request_params",
        [
            ({"name": "this_asset"}),
            ({"uri": "s3://bucket/key"}),
        ],
    )
    def test_by_name_get_404_not_found(self, request_params):
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in ("/assets/by-name", "/assets/by-uri"):
                return httpx.Response(
                    status_code=404,
                    json={
                        "detail": {
                            "message": "Asset with name non_existent not found",
                            "reason": "not_found",
                        }
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.assets.get(**request_params)

        assert isinstance(result, ErrorResponse)
        assert result.error == ErrorType.ASSET_NOT_FOUND


class TestDagRunOperations:
    def test_trigger(self):
        # Simulate a successful response from the server when triggering a dag run
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_trigger/test_run_id":
                actual_body = json.loads(request.read())
                assert actual_body["logical_date"] == "2025-01-01T00:00:00Z"
                assert actual_body["conf"] == {}

                # Since the value for `reset_dag_run` is default, it should not be present in the request body
                assert "reset_dag_run" not in actual_body
                return httpx.Response(status_code=204)
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.trigger(
            dag_id="test_trigger", run_id="test_run_id", logical_date=timezone.datetime(2025, 1, 1)
        )

        assert result == OKResponse(ok=True)

    def test_trigger_conflict(self):
        """Test that if the dag run already exists, the client returns an error when default reset_dag_run=False"""

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_trigger_conflict/test_run_id":
                return httpx.Response(
                    status_code=409,
                    json={
                        "detail": {
                            "reason": "already_exists",
                            "message": "A Dag Run already exists for Dag test_trigger_conflict with run id test_run_id",
                        }
                    },
                )
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.trigger(dag_id="test_trigger_conflict", run_id="test_run_id")

        assert result == ErrorResponse(error=ErrorType.DAGRUN_ALREADY_EXISTS)

    def test_trigger_conflict_reset_dag_run(self):
        """Test that if dag run already exists and reset_dag_run=True, the client clears the dag run"""

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_trigger_conflict_reset/test_run_id":
                return httpx.Response(
                    status_code=409,
                    json={
                        "detail": {
                            "reason": "already_exists",
                            "message": "A Dag Run already exists for Dag test_trigger_conflict with run id test_run_id",
                        }
                    },
                )
            if request.url.path == "/dag-runs/test_trigger_conflict_reset/test_run_id/clear":
                return httpx.Response(status_code=204)
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.trigger(
            dag_id="test_trigger_conflict_reset",
            run_id="test_run_id",
            reset_dag_run=True,
        )

        assert result == OKResponse(ok=True)

    def test_clear(self):
        """Test that the client can clear a dag run"""

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_clear/test_run_id/clear":
                return httpx.Response(status_code=204)
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.clear(dag_id="test_clear", run_id="test_run_id")

        assert result == OKResponse(ok=True)

    def test_get_state(self):
        """Test that the client can get the state of a dag run"""

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_state/test_run_id/state":
                return httpx.Response(
                    status_code=200,
                    json={"state": "running"},
                )
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_state(dag_id="test_state", run_id="test_run_id")

        assert result == DagRunStateResponse(state=DagRunState.RUNNING)

    def test_get_count_basic(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/count":
                assert request.url.params["dag_id"] == "test_dag"
                return httpx.Response(status_code=200, json=1)
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_count(dag_id="test_dag")
        assert result.count == 1

    def test_get_count_with_states(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/count":
                assert request.url.params["dag_id"] == "test_dag"
                assert request.url.params.get_list("states") == ["success", "failed"]
                return httpx.Response(status_code=200, json=2)
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_count(dag_id="test_dag", states=["success", "failed"])
        assert result.count == 2

    def test_get_count_with_logical_dates(self):
        logical_dates = [timezone.datetime(2025, 1, 1), timezone.datetime(2025, 1, 2)]
        logical_dates_str = [d.isoformat() for d in logical_dates]

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/count":
                assert request.url.params["dag_id"] == "test_dag"
                assert request.url.params.get_list("logical_dates") == logical_dates_str
                return httpx.Response(status_code=200, json=2)
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_count(
            dag_id="test_dag", logical_dates=[timezone.datetime(2025, 1, 1), timezone.datetime(2025, 1, 2)]
        )
        assert result.count == 2

    def test_get_count_with_run_ids(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/count":
                assert request.url.params["dag_id"] == "test_dag"
                assert request.url.params.get_list("run_ids") == ["run1", "run2"]
                return httpx.Response(status_code=200, json=2)
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_count(dag_id="test_dag", run_ids=["run1", "run2"])
        assert result.count == 2

    def test_get_previous_basic(self):
        """Test basic get_previous functionality with dag_id and logical_date."""
        logical_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_dag/previous":
                assert request.url.params["logical_date"] == logical_date.isoformat()
                # Return complete DagRun data
                return httpx.Response(
                    status_code=200,
                    json={
                        "dag_id": "test_dag",
                        "run_id": "prev_run",
                        "logical_date": "2024-01-14T12:00:00+00:00",
                        "start_date": "2024-01-14T12:05:00+00:00",
                        "run_after": "2024-01-14T12:00:00+00:00",
                        "run_type": "scheduled",
                        "state": "success",
                        "consumed_asset_events": [],
                    },
                )
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_previous(dag_id="test_dag", logical_date=logical_date)

        assert isinstance(result, PreviousDagRunResult)
        assert result.dag_run.dag_id == "test_dag"
        assert result.dag_run.run_id == "prev_run"
        assert result.dag_run.state == "success"

    def test_get_previous_with_state_filter(self):
        """Test get_previous functionality with state filtering."""
        logical_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_dag/previous":
                assert request.url.params["logical_date"] == logical_date.isoformat()
                assert request.url.params["state"] == "success"
                # Return complete DagRun data
                return httpx.Response(
                    status_code=200,
                    json={
                        "dag_id": "test_dag",
                        "run_id": "prev_success_run",
                        "logical_date": "2024-01-14T12:00:00+00:00",
                        "start_date": "2024-01-14T12:05:00+00:00",
                        "run_after": "2024-01-14T12:00:00+00:00",
                        "run_type": "scheduled",
                        "state": "success",
                        "consumed_asset_events": [],
                    },
                )
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_previous(dag_id="test_dag", logical_date=logical_date, state="success")

        assert isinstance(result, PreviousDagRunResult)
        assert result.dag_run.dag_id == "test_dag"
        assert result.dag_run.run_id == "prev_success_run"
        assert result.dag_run.state == "success"

    def test_get_previous_not_found(self):
        """Test get_previous when no previous Dag run exists returns None."""
        logical_date = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/dag-runs/test_dag/previous":
                assert request.url.params["logical_date"] == logical_date.isoformat()
                # Return None (null) when no previous Dag run found
                return httpx.Response(status_code=200, content="null")
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.dag_runs.get_previous(dag_id="test_dag", logical_date=logical_date)

        assert isinstance(result, PreviousDagRunResult)
        assert result.dag_run is None


class TestTaskRescheduleOperations:
    def test_get_start_date(self):
        """Test that the client can get the start date of a task reschedule"""
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-reschedules/{ti_id}/start_date":
                assert request.url.params.get("try_number") == "1"

                return httpx.Response(
                    status_code=200,
                    json="2024-01-01T00:00:00Z",
                )
            return httpx.Response(status_code=422)

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.task_instances.get_reschedule_start_date(id=ti_id, try_number=1)

        assert isinstance(result, TaskRescheduleStartDate)
        assert result.start_date == "2024-01-01T00:00:00Z"


class TestHITLOperations:
    def test_add_response(self) -> None:
        ti_id = uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in (f"/hitlDetails/{ti_id}"):
                return httpx.Response(
                    status_code=201,
                    json={
                        "ti_id": str(ti_id),
                        "options": ["Approval", "Reject"],
                        "subject": "This is subject",
                        "body": "This is body",
                        "defaults": ["Approval"],
                        "params": None,
                        "multiple": False,
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.hitl.add_response(
            ti_id=ti_id,
            options=["Approval", "Reject"],
            subject="This is subject",
            body="This is body",
            defaults=["Approval"],
            params=None,
            multiple=False,
        )
        assert isinstance(result, HITLDetailRequest)
        assert result.ti_id == ti_id
        assert result.options == ["Approval", "Reject"]
        assert result.subject == "This is subject"
        assert result.body == "This is body"
        assert result.defaults == ["Approval"]
        assert result.params is None
        assert result.multiple is False
        assert result.assigned_users is None

    def test_update_response(self, time_machine: TimeMachineFixture) -> None:
        time_machine.move_to(datetime(2025, 7, 3, 0, 0, 0))
        ti_id = uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in (f"/hitlDetails/{ti_id}"):
                return httpx.Response(
                    status_code=200,
                    json={
                        "chosen_options": ["Approval"],
                        "params_input": {},
                        "responded_by_user": {"id": "admin", "name": "admin"},
                        "response_received": True,
                        "responded_at": "2025-07-03T00:00:00Z",
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.hitl.update_response(
            ti_id=ti_id,
            chosen_options=["Approve"],
            params_input={},
        )
        assert isinstance(result, HITLDetailResponse)
        assert result.response_received is True
        assert result.chosen_options == ["Approval"]
        assert result.params_input == {}
        assert result.responded_by_user == HITLUser(id="admin", name="admin")
        assert result.responded_at == timezone.datetime(2025, 7, 3, 0, 0, 0)

    def test_get_detail_response(self, time_machine: TimeMachineFixture) -> None:
        time_machine.move_to(datetime(2025, 7, 3, 0, 0, 0))
        ti_id = uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in (f"/hitlDetails/{ti_id}"):
                return httpx.Response(
                    status_code=200,
                    json={
                        "chosen_options": ["Approval"],
                        "params_input": {},
                        "responded_by_user": {"id": "admin", "name": "admin"},
                        "response_received": True,
                        "responded_at": "2025-07-03T00:00:00Z",
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.hitl.get_detail_response(ti_id=ti_id)
        assert isinstance(result, HITLDetailResponse)
        assert result.response_received is True
        assert result.chosen_options == ["Approval"]
        assert result.params_input == {}
        assert result.responded_by_user == HITLUser(id="admin", name="admin")
        assert result.responded_at == timezone.datetime(2025, 7, 3, 0, 0, 0)


class TestSSLContextCaching:
    @pytest.fixture(autouse=True)
    def clear_ssl_context_cache(self):
        Client._get_ssl_context_cached.cache_clear()
        yield
        Client._get_ssl_context_cached.cache_clear()

    def test_cache_hit_on_same_parameters(self):
        ca_file = certifi.where()
        ctx1 = Client._get_ssl_context_cached(ca_file, None)
        ctx2 = Client._get_ssl_context_cached(ca_file, None)
        assert ctx1 is ctx2

    def test_cache_miss_if_cache_cleared(self):
        ca_file = certifi.where()
        ctx1 = Client._get_ssl_context_cached(ca_file, None)
        Client._get_ssl_context_cached.cache_clear()
        ctx2 = Client._get_ssl_context_cached(ca_file, None)
        assert ctx1 is not ctx2

    def test_cache_miss_on_different_parameters(self):
        ca_file = certifi.where()

        ctx1 = Client._get_ssl_context_cached(ca_file, None)
        ctx2 = Client._get_ssl_context_cached(ca_file, ca_file)

        info = Client._get_ssl_context_cached.cache_info()

        assert ctx1 is not ctx2
        assert info.misses == 2
        assert info.currsize == 2
