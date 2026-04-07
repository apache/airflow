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

import structlog
import structlog.testing
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from airflow.api_fastapi.common.http_access_log import _HEALTH_PATHS, HttpAccessLogMiddleware


def _make_app(raise_exc: bool = False) -> Starlette:
    async def homepage(request):
        if raise_exc:
            raise RuntimeError("boom")
        return PlainTextResponse("ok")

    async def health(request):
        return PlainTextResponse("healthy")

    app = Starlette(
        routes=[
            Route("/", homepage),
            Route("/api/v2/monitor/health", health),
        ]
    )
    app.add_middleware(HttpAccessLogMiddleware)
    return app


def test_logs_request_fields():
    with structlog.testing.capture_logs() as logs:
        client = TestClient(_make_app(), raise_server_exceptions=False)
        client.get("/?foo=bar")

    assert len(logs) == 1
    record = logs[0]
    assert record["event"] == "request finished"
    assert record["method"] == "GET"
    assert record["path"] == "/"
    assert record["query"] == "foo=bar"
    assert record["status_code"] == 200
    assert "duration_us" in record
    assert isinstance(record["duration_us"], int)
    assert record["duration_us"] >= 0
    assert "client_addr" in record


def test_health_path_not_logged():
    with structlog.testing.capture_logs() as logs:
        client = TestClient(_make_app(), raise_server_exceptions=False)
        client.get("/api/v2/monitor/health")

    assert logs == []


def test_request_id_bound_to_context():
    """request_id header is bound to structlog contextvars during the request."""
    captured_context: dict = {}

    async def homepage(request):
        captured_context.update(structlog.contextvars.get_contextvars())
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/", homepage)])
    app.add_middleware(HttpAccessLogMiddleware)

    TestClient(app).get("/", headers={"x-request-id": "test-id-123"})

    assert captured_context.get("request_id") == "test-id-123"


def test_no_request_id_when_header_absent():
    """No request_id is bound when the header is absent."""
    captured_context: dict = {}

    async def homepage(request):
        captured_context.update(structlog.contextvars.get_contextvars())
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/", homepage)])
    app.add_middleware(HttpAccessLogMiddleware)

    TestClient(app).get("/")

    assert "request_id" not in captured_context


def test_exception_logs_500_status():
    with structlog.testing.capture_logs() as logs:
        client = TestClient(_make_app(raise_exc=True), raise_server_exceptions=False)
        client.get("/")

    assert len(logs) == 1
    assert logs[0]["status_code"] == 500


def test_non_http_scope_not_logged():
    """Non-HTTP scopes (e.g. lifespan) are passed through without logging."""

    async def lifespan_app(scope, receive, send):
        pass

    middleware = HttpAccessLogMiddleware(lifespan_app)

    import asyncio

    with structlog.testing.capture_logs() as logs:
        asyncio.run(middleware({"type": "lifespan"}, None, None))

    assert logs == []


def test_health_paths_constant():
    assert "/api/v2/monitor/health" in _HEALTH_PATHS
