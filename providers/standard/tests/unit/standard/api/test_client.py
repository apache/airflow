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

from datetime import datetime
from typing import TYPE_CHECKING

import httpx
from uuid6 import uuid7

from airflow.providers.standard.api_fastapi.execution_api.datamodels.hitl import HITLResponseContentDetail
from airflow.providers.standard.execution_time.comms import HITLInputRequestResponseResult
from airflow.sdk.api.client import Client
from airflow.utils import timezone

if TYPE_CHECKING:
    from time_machine import TimeMachineFixture


def make_client(transport: httpx.MockTransport) -> Client:
    """Get a client with a custom transport."""
    return Client(base_url="test://server", token="", transport=transport)


class TestHITLOperations:
    def test_add_response(self) -> None:
        ti_id = uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in (f"/hitl-responses/{ti_id}"):
                return httpx.Response(
                    status_code=201,
                    json={
                        "ti_id": str(ti_id),
                        "options": ["Approval", "Reject"],
                        "subject": "This is subject",
                        "body": "This is body",
                        "default": ["Approval"],
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
            default=["Approval"],
            params=None,
            multiple=False,
        )
        assert isinstance(result, HITLInputRequestResponseResult)
        assert result.ti_id == ti_id
        assert result.options == ["Approval", "Reject"]
        assert result.subject == "This is subject"
        assert result.body == "This is body"
        assert result.default == ["Approval"]
        assert result.params is None
        assert result.multiple is False

    def test_update_response(self, time_machine: TimeMachineFixture) -> None:
        time_machine.move_to(datetime(2025, 7, 3, 0, 0, 0))
        ti_id = uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in (f"/hitl-responses/{ti_id}"):
                return httpx.Response(
                    status_code=200,
                    json={
                        "response_content": "Approval",
                        "params_input": None,
                        "user_id": "admin",
                        "response_received": True,
                        "response_at": "2025-07-03T00:00:00Z",
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.hitl.update_response(
            ti_id=ti_id,
            response_content="Approve",
            params_input=None,
        )
        assert isinstance(result, HITLResponseContentDetail)
        assert result.response_received is True
        assert result.response_content == "Approval"
        assert result.params_input is None
        assert result.user_id == "admin"
        assert result.response_at == timezone.datetime(2025, 7, 3, 0, 0, 0)

    def test_get_response_content_detail(self, time_machine: TimeMachineFixture) -> None:
        time_machine.move_to(datetime(2025, 7, 3, 0, 0, 0))
        ti_id = uuid7()

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path in (f"/hitl-responses/{ti_id}"):
                return httpx.Response(
                    status_code=200,
                    json={
                        "response_content": "Approval",
                        "params_input": None,
                        "user_id": "admin",
                        "response_received": True,
                        "response_at": "2025-07-03T00:00:00Z",
                    },
                )
            return httpx.Response(status_code=400, json={"detail": "Bad Request"})

        client = make_client(transport=httpx.MockTransport(handle_request))
        result = client.hitl.get_response_content_detail(
            ti_id=ti_id,
        )
        assert isinstance(result, HITLResponseContentDetail)
        assert result.response_received is True
        assert result.response_content == "Approval"
        assert result.params_input is None
        assert result.user_id == "admin"
        assert result.response_at == timezone.datetime(2025, 7, 3, 0, 0, 0)
