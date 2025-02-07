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
from unittest import mock

import httpx
import pytest
import uuid6

from airflow.sdk.api.client import Client, RemoteValidationError, ServerResponseError
from airflow.sdk.api.datamodels._generated import ConnectionResponse, VariableResponse, XComResponse
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import DeferTask, ErrorResponse, RescheduleTask
from airflow.utils import timezone
from airflow.utils.state import TerminalTIState


def make_client(transport: httpx.MockTransport) -> Client:
    """Get a client with a custom transport"""
    return Client(base_url="test://server", token="", transport=transport)


def make_client_w_dry_run() -> Client:
    """Get a client with dry_run enabled"""
    return Client(base_url=None, dry_run=True, token="")


def make_client_w_responses(responses: list[httpx.Response]) -> Client:
    """Helper fixture to create a mock client with custom responses."""

    def handle_request(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    return Client(
        base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
    )


class TestClient:
    @pytest.mark.parametrize(
        ["path", "json_response"],
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
                    },
                    "max_tries": 0,
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

    @mock.patch("time.sleep", return_value=None)
    def test_retry_handling_unrecoverable_error(self, mock_sleep):
        responses: list[httpx.Response] = [
            *[httpx.Response(500, text="Internal Server Error")] * 11,
            httpx.Response(200, json={"detail": "Recovered from error - but will fail before"}),
            httpx.Response(400, json={"detail": "Should not get here"}),
        ]
        client = make_client_w_responses(responses)

        with pytest.raises(httpx.HTTPStatusError) as err:
            client.get("http://error")
        assert not isinstance(err.value, ServerResponseError)
        assert len(responses) == 3
        assert mock_sleep.call_count == 9

    @mock.patch("time.sleep", return_value=None)
    def test_retry_handling_recovered(self, mock_sleep):
        responses: list[httpx.Response] = [
            *[httpx.Response(500, text="Internal Server Error")] * 3,
            httpx.Response(200, json={"detail": "Recovered from error"}),
            httpx.Response(400, json={"detail": "Should not get here"}),
        ]
        client = make_client_w_responses(responses)

        response = client.get("http://error")
        assert response.status_code == 200
        assert len(responses) == 1
        assert mock_sleep.call_count == 3

    @mock.patch("time.sleep", return_value=None)
    def test_retry_handling_overload(self, mock_sleep):
        responses: list[httpx.Response] = [
            httpx.Response(429, text="I am really busy atm, please back-off", headers={"Retry-After": "37"}),
            httpx.Response(200, json={"detail": "Recovered from error"}),
            httpx.Response(400, json={"detail": "Should not get here"}),
        ]
        client = make_client_w_responses(responses)

        response = client.get("http://error")
        assert response.status_code == 200
        assert len(responses) == 1
        assert mock_sleep.call_count == 1
        assert mock_sleep.call_args[0][0] == 37

    @mock.patch("time.sleep", return_value=None)
    def test_retry_handling_non_retry_error(self, mock_sleep):
        responses: list[httpx.Response] = [
            httpx.Response(422, json={"detail": "Somehow this is a bad request"}),
            httpx.Response(400, json={"detail": "Should not get here"}),
        ]
        client = make_client_w_responses(responses)

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")
        assert len(responses) == 1
        assert mock_sleep.call_count == 0
        assert err.value.args == ("Somehow this is a bad request",)

    @mock.patch("time.sleep", return_value=None)
    def test_retry_handling_ok(self, mock_sleep):
        responses: list[httpx.Response] = [
            httpx.Response(200, json={"detail": "Recovered from error"}),
            httpx.Response(400, json={"detail": "Should not get here"}),
        ]
        client = make_client_w_responses(responses)

        response = client.get("http://error")
        assert response.status_code == 200
        assert len(responses) == 1
        assert mock_sleep.call_count == 0


class TestTaskInstanceOperations:
    """
    Test that the TestVariableOperations class works as expected. While the operations are simple, it
    still catches the basic functionality of the client for task instances including endpoint and
    response parsing.
    """

    @mock.patch("time.sleep", return_value=None)  # To have retries not slowing down tests
    def test_task_instance_start(self, mock_sleep, make_ti_context):
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
            if call_count < 4:
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
        assert call_count == 4

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
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        client.task_instances.finish(ti_id, state=state, when="2024-10-31T12:00:00Z")

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

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                actual_body = json.loads(request.read())
                assert actual_body["state"] == "deferred"
                assert actual_body["trigger_kwargs"] == {
                    "moment": "2024-11-07T12:34:59Z",
                    "end_from_trigger": False,
                }
                assert (
                    actual_body["classpath"] == "airflow.providers.standard.triggers.temporal.DateTimeTrigger"
                )
                assert actual_body["next_method"] == "execute_complete"
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        msg = DeferTask(
            classpath="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
            trigger_kwargs={"moment": "2024-11-07T12:34:59Z", "end_from_trigger": False},
            next_method="execute_complete",
        )
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

        assert result == {"ok": True}


class TestVariableOperations:
    """
    Test that the VariableOperations class works as expected. While the operations are simple, it
    still catches the basic functionality of the client for variables including endpoint and
    response parsing.
    """

    @mock.patch("time.sleep", return_value=None)  # To have retries not slowing down tests
    def test_variable_get_success(self, mock_sleep):
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

    @mock.patch("time.sleep", return_value=None)
    def test_variable_get_500_error(self, mock_sleep):
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
        assert result == {"ok": True}


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
    @mock.patch("time.sleep", return_value=None)  # To have retries not slowing down tests
    def test_xcom_get_success(self, mock_sleep, value):
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

    @mock.patch("time.sleep", return_value=None)
    def test_xcom_get_500_error(self, mock_sleep):
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
        assert result == {"ok": True}

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
        assert result == {"ok": True}


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
