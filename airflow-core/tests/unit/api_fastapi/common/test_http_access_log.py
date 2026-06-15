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

import pytest
import structlog
import structlog.testing
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from airflow._shared.secrets_masker import _secrets_masker
from airflow.api_fastapi.common.http_access_log import (
    _HEALTH_PATHS,
    HttpAccessLogMiddleware,
    _redact_query_string,
)


@pytest.fixture
def _password_sensitive_field():
    """Register ``password`` as a sensitive field name on the module-level masker.

    Production initialises this list from ``DEFAULT_SENSITIVE_FIELDS`` via
    ``settings.mask_secret``; unit tests run without that initialisation, so we
    populate the field explicitly for the redaction tests.
    """
    masker = _secrets_masker()
    original = masker.sensitive_variables_fields
    masker.sensitive_variables_fields = list(set(original) | {"password"})
    try:
        yield
    finally:
        masker.sensitive_variables_fields = original


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


@pytest.mark.enable_redact
def test_redact_query_string_masks_value_by_sensitive_key_name(_password_sensitive_field):
    """A key flagged sensitive by ``secrets_masker`` has its value replaced with ``***``."""
    redacted = _redact_query_string("password=topsecret&safe=value")
    assert "topsecret" not in redacted
    assert "safe=value" in redacted


def test_redact_query_string_leaves_safe_pairs_untouched():
    assert _redact_query_string("page=2&limit=50") == "page=2&limit=50"


def test_redact_query_string_handles_empty_and_blank_values():
    assert _redact_query_string("") == ""
    # Blank values should be preserved so log readers still see the key was present.
    assert _redact_query_string("flag=&other=x") == "flag=&other=x"


@pytest.mark.enable_redact
def test_logs_redact_sensitive_query_param(_password_sensitive_field):
    """Integration: a request with `?password=secret` is logged with the value masked."""
    with structlog.testing.capture_logs() as logs:
        client = TestClient(_make_app(), raise_server_exceptions=False)
        client.get("/?password=topsecret&keep=ok")

    assert len(logs) == 1
    query = logs[0]["query"]
    assert "topsecret" not in query
    assert "keep=ok" in query


def test_logger_failure_does_not_mask_app_exception(monkeypatch):
    """
    If ``logger.info`` raises while the app already raised, the original app exception must
    still propagate (rather than being replaced by the logger's exception).
    """
    import airflow.api_fastapi.common.http_access_log as mod

    def broken_info(*_args, **_kwargs):
        raise RuntimeError("logger broken")

    monkeypatch.setattr(mod.logger, "info", broken_info)

    import asyncio

    async def raising_app(scope, receive, send):
        # Send response.start so the middleware's response variable is populated, then raise.
        await send({"type": "http.response.start", "status": 503, "headers": []})
        raise RuntimeError("app exception")

    middleware = HttpAccessLogMiddleware(raising_app)
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/boom",
        "query_string": b"",
        "headers": [],
        "client": ("test", 1),
    }

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(_message):
        return None

    with pytest.raises(RuntimeError, match="app exception"):
        asyncio.run(middleware(scope, receive, send))


def test_logger_failure_swallowed_on_clean_request(monkeypatch):
    """No app exception + a broken logger must not break the request."""
    import airflow.api_fastapi.common.http_access_log as mod

    monkeypatch.setattr(
        mod.logger, "info", lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("logger broken"))
    )

    client = TestClient(_make_app(), raise_server_exceptions=False)
    response = client.get("/")
    assert response.status_code == 200
