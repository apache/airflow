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
"""In-process Execution API hosting, authentication overrides, and transport lifecycle."""

from __future__ import annotations

import asyncio
import threading
import weakref
from contextlib import AsyncExitStack
from functools import cached_property
from typing import TYPE_CHECKING

import attrs
import structlog
from starlette.requests import Request

if TYPE_CHECKING:
    import httpx
    from fastapi import FastAPI

logger = structlog.get_logger(logger_name=__name__)


# Finalizer arguments must not reference the transport, or it cannot be garbage collected.
def _shutdown_loop(
    loop: asyncio.AbstractEventLoop,
    thread: threading.Thread,
    cm: AsyncExitStack,
) -> None:
    """Close the FastAPI lifespan and stop the background event loop + thread."""
    try:
        asyncio.run_coroutine_threadsafe(cm.aclose(), loop).result(timeout=5)
    except Exception:
        logger.exception("Error while closing in-process execution API lifespan")
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)


def _configure_in_process_auth(app: FastAPI) -> None:
    from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
    from airflow.api_fastapi.execution_api.routes.connections import has_connection_access
    from airflow.api_fastapi.execution_api.routes.variables import has_variable_access
    from airflow.api_fastapi.execution_api.routes.xcoms import has_xcom_access
    from airflow.api_fastapi.execution_api.security import _jwt_bearer

    async def always_allow(request: Request):
        from uuid import UUID

        ti_id = UUID(request.path_params.get("task_instance_id", "00000000-0000-0000-0000-000000000000"))
        claims = TIClaims(scope="execution")
        return TIToken(id=ti_id, claims=claims)

    app.dependency_overrides[_jwt_bearer] = always_allow
    app.dependency_overrides[has_connection_access] = always_allow
    app.dependency_overrides[has_variable_access] = always_allow
    app.dependency_overrides[has_xcom_access] = always_allow


@attrs.define()
class InProcessExecutionAPI:
    """
    A helper class to make it possible to run the ExecutionAPI "in-process".

    The sync version of this makes use of a2wsgi which runs the async loop in a separate thread. This is
    needed so that we can use the sync httpx client
    """

    _app: FastAPI | None = None

    @cached_property
    def app(self):
        import svcs

        from airflow.api_fastapi.auth.tokens import JWTValidator
        from airflow.api_fastapi.execution_api.app import create_task_execution_api_app, lifespan

        if not self._app:
            # Keep the stub private: a shared None validator would prevent lifespan() from
            # registering the real API server's validator.
            registry = svcs.Registry()
            self._app = create_task_execution_api_app(lifespan=attrs.evolve(lifespan, registry=registry))
            # Auth dependency overrides bypass validation, so the stub skips validator setup in lifespan().
            registry.register_value(JWTValidator, None)
            _configure_in_process_auth(self._app)

        return self._app

    @cached_property
    def transport(self) -> httpx.WSGITransport:
        import httpx
        from a2wsgi import ASGIMiddleware

        # Own the event loop and thread so the transport controls their lifecycle.
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, name="InProcessExecutionAPI-loop", daemon=True)
        thread.start()

        middleware = ASGIMiddleware(self.app, loop=loop)

        # https://github.com/abersheeran/a2wsgi/discussions/64
        async def start_lifespan(cm: AsyncExitStack, app: FastAPI):
            await cm.enter_async_context(app.router.lifespan_context(app))

        cm = AsyncExitStack()

        # Wait for startup so callers see a ready app and the finalizer can close an entered context.
        asyncio.run_coroutine_threadsafe(start_lifespan(cm, self.app), loop).result()

        transport = httpx.WSGITransport(app=middleware)  # type: ignore[arg-type]

        # Callers can retain the transport after dropping this instance; finalizing on self would
        # stop the loop while requests still need it.
        weakref.finalize(transport, _shutdown_loop, loop, thread, cm)

        return transport

    @cached_property
    def atransport(self) -> httpx.ASGITransport:
        import httpx

        return httpx.ASGITransport(app=self.app)
