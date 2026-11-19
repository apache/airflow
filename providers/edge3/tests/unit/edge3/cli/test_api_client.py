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
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from unittest.mock import patch
from uuid import uuid4

import pytest
from aiohttp import ClientResponseError, ConnectionTimeoutError
from fastapi import Request

from airflow.providers.common.compat.sdk import TaskInstanceKey
from airflow.providers.edge3.cli.api_client import (
    _make_generic_request,
    jobs_fetch,
    jobs_set_state,
    jwt_generator,
    logs_logfile_path,
    logs_push,
)
from airflow.providers.edge3.worker_api import auth
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.aiohttp import MockAiohttpClientResponse
from tests_common.test_utils.config import conf_vars

pytestmark = [pytest.mark.asyncio]

MOCK_ENDPOINT = "https://mock-api-test-endpoint"
MOCK_OK_PAYLOAD = {"test": "ok"}


@dataclass
class _ResponseSpec:
    status: int
    payload: dict | None = None


class _MockRequestContext:
    def __init__(self, *, response: MockAiohttpClientResponse | None = None, exc: Exception | None = None):
        self._response = response
        self._exc = exc

    async def __aenter__(self) -> MockAiohttpClientResponse:
        if self._exc:
            raise self._exc
        return self._response  # type: ignore[return-value]

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


def _build_request_side_effect(
    sequence: list[_ResponseSpec | Exception],
    calls: list[tuple[str, str, str | None]],
):
    iterator = iter(sequence)

    def _request(method: str, *, url: str, data: str | None = None, headers=None):
        calls.append((method, url, data))
        current = next(iterator)
        if isinstance(current, Exception):
            return _MockRequestContext(exc=current)
        return _MockRequestContext(
            response=MockAiohttpClientResponse(
                status=current.status,
                payload=current.payload,
                method=method,
                url=url,
                reason=f"HTTP {current.status}",
            )
        )

    return _request


@pytest.mark.parametrize("uuid_capability", [None, False, True])
async def test_jobs_fetch_sends_worker_capability(mocker, uuid_capability):
    request = mocker.patch(
        "airflow.providers.edge3.cli.api_client._make_generic_request", autospec=True, return_value=None
    )
    kwargs = {} if uuid_capability is None else {"supports_task_instance_uuid": uuid_capability}

    assert await jobs_fetch("worker", ["queue"], 2, **kwargs) is None

    request.assert_awaited_once()
    assert request.call_args.args[:2] == ("POST", "jobs/fetch/worker")
    assert json.loads(request.call_args.args[2]) == {
        "queues": ["queue"],
        "free_concurrency": 2,
        "team_name": None,
        "supports_task_instance_uuid": bool(uuid_capability),
    }


class TestApiClient:
    @patch.dict("os.environ", {"AIRFLOW__EDGE__API_RETRIES": "10"}, clear=False)
    async def test_make_generic_request_success(self):
        calls: list[tuple[str, str, str | None]] = []
        request_side_effect = _build_request_side_effect(
            [
                _ResponseSpec(status=HTTPStatus.OK, payload=MOCK_OK_PAYLOAD),
                _ResponseSpec(status=HTTPStatus.NO_CONTENT),
            ],
            calls,
        )

        with (
            conf_vars({("edge", "api_url"): MOCK_ENDPOINT}),
            patch("airflow.providers.edge3.cli.api_client.request", side_effect=request_side_effect),
        ):
            result1 = await _make_generic_request("GET", f"{MOCK_ENDPOINT}/mock_service")
            result2 = await _make_generic_request("POST", f"{MOCK_ENDPOINT}/service_no_content", "test")

        assert result1 == MOCK_OK_PAYLOAD
        assert result2 is None
        assert len(calls) == 2

    @patch("asyncio.sleep", return_value=None)
    async def test_make_generic_request_retry(self, mock_sleep):
        flaky_service = f"{MOCK_ENDPOINT}/flaky_service"
        calls: list[tuple[str, str, str | None]] = []
        request_side_effect = _build_request_side_effect(
            [
                _ResponseSpec(status=HTTPStatus.SERVICE_UNAVAILABLE),
                _ResponseSpec(status=HTTPStatus.SERVICE_UNAVAILABLE),
                _ResponseSpec(status=HTTPStatus.SERVICE_UNAVAILABLE),
                ConnectionTimeoutError(),
                _ResponseSpec(status=HTTPStatus.OK, payload=MOCK_OK_PAYLOAD),
            ],
            calls,
        )
        with (
            conf_vars({("edge", "api_url"): MOCK_ENDPOINT}),
            patch("airflow.providers.edge3.cli.api_client.request", side_effect=request_side_effect),
        ):
            result = await _make_generic_request("GET", flaky_service)

        assert result == MOCK_OK_PAYLOAD
        assert len(calls) == 5
        assert all(call[0] == "GET" and call[1] == flaky_service for call in calls)

    @patch("asyncio.sleep", return_value=None)
    async def test_make_generic_request_unrecoverable_error(self, mock_sleep):
        unreliable_service = f"{MOCK_ENDPOINT}/bad_service"
        calls: list[tuple[str, str, str | None]] = []
        request_side_effect = _build_request_side_effect(
            [_ResponseSpec(status=HTTPStatus.INTERNAL_SERVER_ERROR) for _ in range(10)],
            calls,
        )

        with (
            conf_vars({("edge", "api_url"): MOCK_ENDPOINT}),
            patch("airflow.providers.edge3.cli.api_client.request", side_effect=request_side_effect),
        ):
            with pytest.raises(ClientResponseError) as err:
                await _make_generic_request("POST", unreliable_service, "test")

        mock_sleep.assert_called()
        assert err.value.status == HTTPStatus.INTERNAL_SERVER_ERROR
        assert len(calls) == 10
        assert all(call[0] == "POST" and call[1] == unreliable_service for call in calls)


@pytest.mark.parametrize("uuid_job", [False, True])
async def test_job_state_identity_preserves_signed_request_path(uuid_job):
    task_id = uuid4() if uuid_job else None
    captured = {}
    key = TaskInstanceKey("dag", "task", "run", 1, -1)

    def send_request(method, *, url, data, headers):
        captured.update(url=url, data=data, headers=headers)
        return _MockRequestContext(
            response=MockAiohttpClientResponse(
                status=HTTPStatus.NO_CONTENT, method=method, url=url, reason="No Content"
            )
        )

    with (
        conf_vars(
            {
                ("edge", "api_url"): "https://worker/edge_worker/v1/",
                ("api_auth", "jwt_secret"): "uuid-test-secret",
            }
        ),
        patch("airflow.providers.edge3.cli.api_client.request", autospec=True, side_effect=send_request),
    ):
        jwt_generator.cache_clear()
        auth.jwt_validator.cache_clear()
        try:
            await jobs_set_state(key, TaskInstanceState.SUCCESS, task_instance_id=task_id)
            request = Request(
                {
                    "type": "http",
                    "path": "/edge_worker/v1/jobs/state/dag/task/run/1/-1/success",
                    "headers": [],
                }
            )
            await auth.jwt_token_authorization_rest(request, captured["headers"]["Authorization"])
        finally:
            jwt_generator.cache_clear()
            auth.jwt_validator.cache_clear()
    assert captured["url"] == "https://worker/edge_worker/v1/jobs/state/dag/task/run/1/-1/success"
    assert captured["data"] == (json.dumps({"task_instance_id": str(task_id)}) if task_id else None)


@pytest.mark.parametrize("uuid_job", [False, True])
async def test_log_requests_identify_the_task_instance_without_changing_the_signed_path(uuid_job):
    task_id = uuid4() if uuid_job else None
    captured = []
    key = TaskInstanceKey("dag", "task", "run", 1, -1)

    def send_request(method, *, url, data, headers):
        captured.append({"url": url, "data": data, "headers": headers})
        return _MockRequestContext(
            response=MockAiohttpClientResponse(
                status=HTTPStatus.OK, method=method, url=url, reason="OK", payload='"log/path"'
            )
        )

    with (
        conf_vars(
            {
                ("edge", "api_url"): "https://worker/edge_worker/v1/",
                ("api_auth", "jwt_secret"): "uuid-test-secret",
            }
        ),
        patch("airflow.providers.edge3.cli.api_client.request", autospec=True, side_effect=send_request),
    ):
        jwt_generator.cache_clear()
        auth.jwt_validator.cache_clear()
        try:
            await logs_logfile_path(key, task_instance_id=task_id)
            await logs_push(key, datetime(2026, 1, 1, tzinfo=UTC), "chunk", task_instance_id=task_id)
            for sent, path in zip(
                captured,
                [
                    "/edge_worker/v1/logs/logfile_path/dag/task/run/1/-1",
                    "/edge_worker/v1/logs/push/dag/task/run/1/-1",
                ],
            ):
                request = Request({"type": "http", "path": path, "headers": []})
                await auth.jwt_token_authorization_rest(request, sent["headers"]["Authorization"])
        finally:
            jwt_generator.cache_clear()
            auth.jwt_validator.cache_clear()

    path_url, push_url = (sent["url"] for sent in captured)
    assert path_url == "https://worker/edge_worker/v1/logs/logfile_path/dag/task/run/1/-1" + (
        f"?task_instance_id={task_id}" if task_id else ""
    )
    assert push_url == "https://worker/edge_worker/v1/logs/push/dag/task/run/1/-1"
    assert json.loads(captured[1]["data"]).get("task_instance_id") == (str(task_id) if task_id else None)
