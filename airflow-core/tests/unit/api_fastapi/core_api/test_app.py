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

import inspect
import typing

import pytest
from fastapi.params import Depends as DependsClass
from fastapi.responses import StreamingResponse
from starlette.routing import Mount

from airflow.api_fastapi.app import create_app

from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test


def _get_all_api_routes(app):
    """Recursively yield all APIRoutes from the app and its mounted sub-apps."""
    for route in getattr(app, "routes", []):
        if isinstance(route, Mount) and hasattr(route, "app"):
            yield from _get_all_api_routes(route.app)
        if hasattr(route, "endpoint"):
            yield route


class TestStreamingEndpointSessionScope:
    def test_no_streaming_endpoint_uses_function_scoped_depends(self):
        """Streaming endpoints must not use function-scoped generator dependencies.

        FastAPI's ``function_stack`` (used for ``scope="function"`` dependencies)
        is torn down after the route handler returns but *before* the response body
        is sent.  For ``StreamingResponse`` endpoints the response body is produced
        by a generator that runs during sending, so any generator dependency with
        ``scope="function"`` will have its cleanup run before the generator
        executes.  This causes the generator to silently reopen the session via
        autobegin, and the resulting connection is never returned to the pool.
        """
        # These endpoints mention StreamingResponse but only use the session
        # *before* streaming begins — the generator does not capture it.
        # Function scope is correct for them: close the session early rather
        # than hold it open for the entire (potentially long) stream.
        allowed = {
            "airflow.api_fastapi.core_api.routes.public.log.get_log",
            "airflow.api_fastapi.core_api.routes.public.dag_run.wait_dag_run_until_finished",
        }

        app = create_app()
        violations = []
        for route in _get_all_api_routes(app):
            try:
                hints = typing.get_type_hints(route.endpoint, include_extras=True)
            except Exception:
                continue
            returns_streaming = hints.get("return") is StreamingResponse
            if not returns_streaming:
                try:
                    returns_streaming = "StreamingResponse" in inspect.getsource(route.endpoint)
                except (OSError, TypeError):
                    pass
            if not returns_streaming:
                continue
            fqn = f"{route.endpoint.__module__}.{route.endpoint.__qualname__}"
            if fqn in allowed:
                continue
            for param_name, hint in hints.items():
                if param_name == "return":
                    continue
                if typing.get_origin(hint) is not typing.Annotated:
                    continue
                for metadata in typing.get_args(hint)[1:]:
                    if isinstance(metadata, DependsClass) and metadata.scope == "function":
                        violations.append(
                            f"{route.endpoint.__module__}.{route.endpoint.__qualname__}"
                            f" parameter '{param_name}'"
                        )

        assert not violations, (
            "Streaming endpoints must not use function-scoped dependencies like "
            "SessionDep.  Use Annotated[Session, Depends(_get_session)] (default "
            "request scope) instead — function-scoped cleanup runs before the "
            "response body is streamed, leaking database connections.\n"
            + "\n".join(f"  - {v}" for v in violations)
        )


class TestGzipMiddleware:
    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_jobs()
        yield
        clear_db_jobs()

    def test_gzip_middleware_should_not_be_chunked(self, test_client) -> None:
        response = test_client.get("/api/v2/monitor/health")
        headers = {k.lower(): v for k, v in response.headers.items()}

        # Ensure we do not reintroduce Transfer-Encoding: chunked
        assert "transfer-encoding" not in headers
