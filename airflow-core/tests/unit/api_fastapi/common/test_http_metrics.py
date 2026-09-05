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
from fastapi import FastAPI
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from airflow.api_fastapi.common.http_metrics import (
    HttpMetricsMiddleware,
    _emit_api_metrics,
    _get_status_family,
)


def _make_app() -> FastAPI:
    """Build a FastAPI app so route resolution behaves as it does in the real API server."""
    app = FastAPI()

    @app.get("/")
    async def homepage():
        return PlainTextResponse("ok")

    @app.get("/api/v2/items/{item_id}")
    async def api_item(item_id: str):
        return PlainTextResponse("ok")

    @app.get("/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}")
    async def task_instance(dag_id: str, dag_run_id: str, task_id: str):
        return PlainTextResponse("ok")

    @app.get("/ui/items/{item_id}")
    async def ui_item(item_id: str):
        return PlainTextResponse("ok")

    @app.get("/plugin/items/{item_id}")
    async def plugin_item(item_id: str):
        return PlainTextResponse("ok")

    @app.get("/api/v2/fail")
    async def api_fail():
        raise RuntimeError("boom")

    @app.get("/api/v2/monitor/health")
    async def health():
        return PlainTextResponse("healthy")

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


def test_request_duration_is_emitted_in_milliseconds():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr"),
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        _emit_api_metrics(
            scope={},
            path="/api/v2/items/42",
            method="GET",
            status_code=200,
            duration_us=1_500_000,
        )

    mock_timing.assert_called_once_with("http_request_duration_milliseconds", 1500.0, tags=mock.ANY)


@pytest.mark.parametrize(
    ("request_path", "route_tag"),
    [
        pytest.param("/api/v2/items/42", "/api/v2/items/{item_id}", id="public"),
        pytest.param(
            "/api/v2/dags/example/dagRuns/manual__2026-07-13/taskInstances/extract",
            "/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}",
            id="route-template-with-parameters",
        ),
        pytest.param("/ui/items/42", "/ui/items/{item_id}", id="ui"),
    ],
)
def test_api_requests_emit_metrics(request_path, route_tag):
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get(request_path)

    assert response.status_code == 200
    expected_tags = {
        "method": "GET",
        "route": route_tag,
        "status_family": "2xx",
    }
    mock_incr.assert_called_once_with("http_requests_total", tags=expected_tags)
    mock_timing.assert_called_once_with("http_request_duration_milliseconds", mock.ANY, tags=expected_tags)


@pytest.mark.parametrize(
    ("request_path", "expected_status_code"),
    [
        pytest.param("/plugin/items/42", 200, id="plugin"),
        pytest.param("/api/v20/items/42", 404, id="similar-prefix"),
    ],
)
def test_paths_outside_api_metrics_surfaces_do_not_emit_metrics(request_path, expected_status_code):
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get(request_path)

    assert response.status_code == expected_status_code
    mock_incr.assert_not_called()
    mock_timing.assert_not_called()


def test_health_path_emits_metrics():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/api/v2/monitor/health")

    assert response.status_code == 200
    expected_tags = {
        "method": "GET",
        "route": "/api/v2/monitor/health",
        "status_family": "2xx",
    }
    mock_incr.assert_called_once_with("http_requests_total", tags=expected_tags)
    mock_timing.assert_called_once_with("http_request_duration_milliseconds", mock.ANY, tags=expected_tags)


def test_failed_api_requests_emit_metrics_with_server_error_status():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/api/v2/fail")

    assert response.status_code == 500
    expected_tags = {
        "method": "GET",
        "route": "/api/v2/fail",
        "status_family": "5xx",
    }
    mock_incr.assert_called_once_with("http_requests_total", tags=expected_tags)
    mock_timing.assert_called_once_with("http_request_duration_milliseconds", mock.ANY, tags=expected_tags)


@pytest.mark.parametrize(
    ("method", "expected_status_code", "expected_tags"),
    [
        pytest.param(
            "GET",
            200,
            {"method": "GET", "route": "/api/v2/items/{item_id}", "status_family": "2xx"},
            id="served-method",
        ),
        pytest.param(
            "POST",
            405,
            {"method": "POST", "route": "/api/v2/items/{item_id}", "status_family": "4xx"},
            id="served-method-not-allowed-on-route",
        ),
        pytest.param(
            "HEAD",
            405,
            {"method": "OTHER", "route": "/api/v2/items/{item_id}", "status_family": "4xx"},
            id="standard-method-never-served",
        ),
        pytest.param(
            "METHOD-0001",
            405,
            {"method": "OTHER", "route": "/api/v2/items/{item_id}", "status_family": "4xx"},
            id="made-up-method",
        ),
    ],
)
def test_method_tag_is_bounded_to_served_methods(method, expected_status_code, expected_tags):
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.request(method, "/api/v2/items/42")

    assert response.status_code == expected_status_code
    mock_incr.assert_called_once_with("http_requests_total", tags=expected_tags)
    mock_timing.assert_called_once_with("http_request_duration_milliseconds", mock.ANY, tags=expected_tags)


def test_unmatched_api_requests_use_unmatched_route_tag():
    with (
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.incr") as mock_incr,
        mock.patch("airflow.api_fastapi.common.http_metrics.Stats.timing") as mock_timing,
    ):
        client = TestClient(_make_app(), raise_server_exceptions=False)
        response = client.get("/api/v2/missing")

    assert response.status_code == 404
    expected_tags = {
        "method": "GET",
        "route": "unmatched",
        "status_family": "4xx",
    }
    mock_incr.assert_called_once_with("http_requests_total", tags=expected_tags)
    mock_timing.assert_called_once_with("http_request_duration_milliseconds", mock.ANY, tags=expected_tags)
