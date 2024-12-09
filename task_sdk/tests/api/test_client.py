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

import httpx
import pytest
import uuid6

from airflow.sdk.api.client import Client, RemoteValidationError, ServerResponseError
from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.execution_time.comms import DeferTask
from airflow.utils.state import TerminalTIState


class TestClient:
    def test_error_parsing(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            """
            A transport handle that always returns errors
            """

            return httpx.Response(422, json={"detail": [{"loc": ["#0"], "msg": "err", "type": "required"}]})

        client = Client(
            base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
        )

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")

        assert isinstance(err.value, ServerResponseError)
        assert isinstance(err.value.detail, list)
        assert err.value.detail == [
            RemoteValidationError(loc=["#0"], msg="err", type="required"),
        ]

    def test_error_parsing_plain_text(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            """
            A transport handle that always returns errors
            """

            return httpx.Response(422, content=b"Internal Server Error")

        client = Client(
            base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
        )

        with pytest.raises(httpx.HTTPStatusError) as err:
            client.get("http://error")
        assert not isinstance(err.value, ServerResponseError)

    def test_error_parsing_other_json(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            # Some other json than an error body.
            return httpx.Response(404, json={"detail": "Not found"})

        client = Client(
            base_url=None, dry_run=True, token="", mounts={"'http://": httpx.MockTransport(handle_request)}
        )

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")
        assert err.value.args == ("Not found",)
        assert err.value.detail is None


def make_client(transport: httpx.MockTransport) -> Client:
    """Get a client with a custom transport"""
    return Client(base_url="test://server", token="", transport=transport)


class TestTaskInstanceOperations:
    """
    Test that the TestVariableOperations class works as expected. While the operations are simple, it
    still catches the basic functionality of the client for task instances including endpoint and
    response parsing.
    """

    def test_task_instance_start(self):
        # Simulate a successful response from the server that starts a task
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        try:
            client.task_instances.start(ti_id, 100, "2024-10-31T12:00:00Z")
        except Exception as e:
            pytest.fail(f"Unexpected error occurred: {e}")

    @pytest.mark.parametrize("state", [state for state in TerminalTIState])
    def test_task_instance_finish(self, state):
        # Simulate a successful response from the server that finishes a task
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        try:
            client.task_instances.finish(ti_id, state=state, when="2024-10-31T12:00:00Z")
        except Exception as e:
            pytest.fail(f"Unexpected error occurred: {e}")

    def test_task_instance_heartbeat(self):
        # Simulate a successful response from the server is used to heartbeat
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/heartbeat":
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        try:
            client.task_instances.heartbeat(ti_id, 100)
        except Exception as e:
            pytest.fail(f"Unexpected error occurred: {e}")

    def test_task_instance_defer(self):
        # Simulate a successful response from the server that defers a task
        ti_id = uuid6.uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/state":
                return httpx.Response(
                    status_code=204,
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        try:
            msg = DeferTask(
                classpath="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
                trigger_kwargs={"moment": "2024-11-07T12:34:59Z", "end_from_trigger": False},
                next_method="execute_complete",
            )
            client.task_instances.defer(ti_id, msg)
        except Exception as e:
            pytest.fail(f"Unexpected error occurred: {e}")

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

    def test_variable_get_success(self):
        # Simulate a successful response from the server with a variable
        def handle_request(request: httpx.Request) -> httpx.Response:
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

        with pytest.raises(ServerResponseError) as err:
            client.variables.get(key="non_existent_var")

        assert err.value.response.status_code == 404
        assert err.value.detail == {
            "detail": {
                "message": "Variable with key 'non_existent_var' not found",
                "reason": "not_found",
            }
        }

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
