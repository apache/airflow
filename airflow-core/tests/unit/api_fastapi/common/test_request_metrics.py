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

import asyncio
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from starlette.testclient import TestClient

from airflow.api_fastapi.common.request_metrics import (
    _UNMATCHED_ROUTE,
    REQUEST_COUNT_METRIC,
    REQUEST_DURATION_METRIC,
    RequestMetricsMiddleware,
)

from tests_common.test_utils.config import conf_vars

STATS_TARGET = "airflow.api_fastapi.common.request_metrics.Stats"


def _make_app(*, raise_exc: bool = False, status_code: int = 200) -> FastAPI:
    app = FastAPI()

    @app.get("/items/{item_id}")
    def items(item_id: str):
        if raise_exc:
            raise RuntimeError("boom")
        return PlainTextResponse("ok", status_code=status_code)

    app.add_middleware(RequestMetricsMiddleware)
    return app


@mock.patch(STATS_TARGET, autospec=True)
def test_records_count_and_duration_with_templated_route(mock_stats):
    client = TestClient(_make_app(), raise_server_exceptions=False)
    client.get("/items/42")

    expected_tags = {"method": "GET", "status": "200", "route": "/items/{item_id}"}
    mock_stats.incr.assert_called_once_with(REQUEST_COUNT_METRIC, tags=expected_tags)

    mock_stats.timing.assert_called_once()
    name, duration = mock_stats.timing.call_args.args
    assert name == REQUEST_DURATION_METRIC
    assert duration >= 0
    assert mock_stats.timing.call_args.kwargs["tags"] == expected_tags


@mock.patch(STATS_TARGET, autospec=True)
def test_records_response_status(mock_stats):
    client = TestClient(_make_app(status_code=418), raise_server_exceptions=False)
    client.get("/items/1")

    assert mock_stats.incr.call_args.kwargs["tags"]["status"] == "418"


@mock.patch(STATS_TARGET, autospec=True)
def test_unmatched_route_uses_constant_tag(mock_stats):
    client = TestClient(_make_app(), raise_server_exceptions=False)
    client.get("/does-not-exist")

    tags = mock_stats.incr.call_args.kwargs["tags"]
    assert tags["route"] == _UNMATCHED_ROUTE
    assert tags["status"] == "404"


@mock.patch(STATS_TARGET, autospec=True)
def test_records_status_500_on_app_exception(mock_stats):
    client = TestClient(_make_app(raise_exc=True), raise_server_exceptions=False)
    client.get("/items/1")

    assert mock_stats.incr.call_args.kwargs["tags"]["status"] == "500"


@mock.patch(STATS_TARGET, autospec=True)
def test_non_http_scope_is_passed_through(mock_stats):
    called = False

    async def downstream(scope, receive, send):
        nonlocal called
        called = True

    middleware = RequestMetricsMiddleware(downstream)
    asyncio.run(middleware({"type": "lifespan"}, None, None))

    assert called
    mock_stats.incr.assert_not_called()
    mock_stats.timing.assert_not_called()


@mock.patch(STATS_TARGET, autospec=True)
def test_stats_failure_does_not_mask_app_exception(mock_stats):
    mock_stats.incr.side_effect = RuntimeError("stats broken")

    async def raising_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 503, "headers": []})
        raise RuntimeError("app exception")

    middleware = RequestMetricsMiddleware(raising_app)
    scope = {"type": "http", "method": "GET", "path": "/boom", "headers": []}

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(_message):
        return None

    with pytest.raises(RuntimeError, match="app exception"):
        asyncio.run(middleware(scope, receive, send))


@mock.patch(STATS_TARGET, autospec=True)
def test_stats_failure_swallowed_on_clean_request(mock_stats):
    mock_stats.incr.side_effect = RuntimeError("stats broken")

    client = TestClient(_make_app(), raise_server_exceptions=False)
    response = client.get("/items/1")

    assert response.status_code == 200


@pytest.mark.parametrize(
    ("flag", "enabled", "should_register"),
    [
        ("otel_on", "True", True),
        ("statsd_on", "True", True),
        ("statsd_datadog_enabled", "True", True),
        ("otel_on", "False", False),
    ],
)
@mock.patch("airflow.api_fastapi.app.get_auth_manager")
def test_middleware_registered_only_when_metrics_backend_enabled(
    mock_get_auth_manager, flag, enabled, should_register
):
    from airflow.api_fastapi.core_api.app import init_middlewares

    mock_get_auth_manager.return_value.get_fastapi_middlewares.return_value = []

    app = FastAPI()
    with conf_vars(
        {
            ("metrics", "otel_on"): "False",
            ("metrics", "statsd_on"): "False",
            ("metrics", "statsd_datadog_enabled"): "False",
            ("metrics", flag): enabled,
        }
    ):
        init_middlewares(app)

    registered = {m.cls for m in app.user_middleware}
    assert (RequestMetricsMiddleware in registered) is should_register
