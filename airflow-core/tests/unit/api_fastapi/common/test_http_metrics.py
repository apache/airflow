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
import structlog.testing
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from airflow.api_fastapi.common.http_metrics import (
    _HEALTH_PATHS,
    HttpMetricsMiddleware,
    _get_status_family,
)


def _make_app(raise_exc: bool = False) -> Starlette:
    async def homepage(request):
        if raise_exc:
            raise RuntimeError("boom")
        return PlainTextResponse("ok")

    async def api_item(request):
        return PlainTextResponse("ok")

    async def ui_item(request):
        return PlainTextResponse("ok")

    async def api_fail(request):
        raise RuntimeError("boom")

    async def health(request):
        return PlainTextResponse("healthy")

    app = Starlette(
        routes=[
            Route("/", homepage),
            Route("/api/v2/items/{item_id}", api_item),
            Route("/ui/items/{item_id}", ui_item),
            Route("/api/v2/fail", api_fail),
            Route("/api/v2/monitor/health", health),
        ]
    )
    app.add_middleware(HttpMetricsMiddleware)
    return app


def test_metric_emission_failure_is_logged():
    with (
        mock.patch(
            "airflow.api_fastapi.common.http_metrics._emit_api_metrics",
            side_effect=RuntimeError("metrics boom"),
        ),
        structlog.testing.capture_logs() as logs,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/api/v2/items/42")

    assert response.status_code == 200
    assert [record["event"] for record in logs] == ["failed to emit API metrics"]
    assert logs[0]["method"] == "GET"
    assert logs[0]["path"] == "/api/v2/items/42"
    assert logs[0]["status_code"] == 200


def test_non_http_scope_does_not_emit_metrics():
    """Non-HTTP scopes (e.g. lifespan) are passed through without metrics emission."""

    async def lifespan_app(scope, receive, send):
        pass

    middleware = HttpMetricsMiddleware(lifespan_app)

    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        asyncio.run(middleware({"type": "lifespan"}, None, None))

    mock_incr.assert_not_called()
    mock_timing.assert_not_called()


def test_health_paths_constant():
    assert "/api/v2/monitor/health" in _HEALTH_PATHS


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        pytest.param(0, "0xx", id="no-response"),
        pytest.param(200, "2xx", id="success"),
        pytest.param(404, "4xx", id="client-error"),
        pytest.param(500, "5xx", id="server-error"),
    ],
)
def test_get_status_family(status_code, expected):
    assert _get_status_family(status_code) == expected


@pytest.mark.parametrize(
    ("request_path", "route_tag", "api_surface"),
    [
        pytest.param("/api/v2/items/42", "/api/v2/items/{item_id}", "public", id="public"),
        pytest.param("/ui/items/42", "/ui/items/{item_id}", "ui", id="ui"),
    ],
)
def test_api_requests_emit_metrics(request_path, route_tag, api_surface):
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get(request_path)

    assert response.status_code == 200
    expected_request_tags = {
        "api_surface": api_surface,
        "method": "GET",
        "route": route_tag,
        "status_family": "2xx",
    }
    expected_duration_tags = {
        "api_surface": api_surface,
        "method": "GET",
        "route": route_tag,
    }
    mock_incr.assert_called_once_with("api.requests", tags=expected_request_tags)
    mock_timing.assert_called_once_with("api.request.duration", mock.ANY, tags=expected_duration_tags)


def test_non_api_paths_do_not_emit_metrics():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/")

    assert response.status_code == 200
    mock_incr.assert_not_called()
    mock_timing.assert_not_called()


def test_health_path_does_not_emit_metrics():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/api/v2/monitor/health")

    assert response.status_code == 200
    mock_incr.assert_not_called()
    mock_timing.assert_not_called()


def test_failed_api_requests_emit_error_metric():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/api/v2/fail")

    assert response.status_code == 500
    expected_request_tags = {
        "api_surface": "public",
        "method": "GET",
        "route": "/api/v2/fail",
        "status_family": "5xx",
    }
    expected_error_tags = {
        "api_surface": "public",
        "method": "GET",
        "route": "/api/v2/fail",
    }
    assert mock_incr.call_args_list == [
        mock.call("api.requests", tags=expected_request_tags),
        mock.call("api.request.errors", tags=expected_error_tags),
    ]
    mock_timing.assert_called_once_with("api.request.duration", mock.ANY, tags=expected_error_tags)


def test_unmatched_api_requests_use_unmatched_route_tag():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/api/v2/missing")

    assert response.status_code == 404
    expected_request_tags = {
        "api_surface": "public",
        "method": "GET",
        "route": "unmatched",
        "status_family": "4xx",
    }
    expected_duration_tags = {
        "api_surface": "public",
        "method": "GET",
        "route": "unmatched",
    }
    mock_incr.assert_called_once_with("api.requests", tags=expected_request_tags)
    mock_timing.assert_called_once_with("api.request.duration", mock.ANY, tags=expected_duration_tags)
