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

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from fastapi import Body, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.providers.common.compat.sdk import conf
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginResponse
from airflow.providers.fab.auth_manager.api_fastapi.routes.router import auth_router
from airflow.providers.fab.auth_manager.api_fastapi.services.login import FABAuthManagerLogin
from airflow.providers.fab.version_compat import AIRFLOW_V_3_1_8_PLUS

if AIRFLOW_V_3_1_8_PLUS:
    from airflow.api_fastapi.app import get_cookie_path
else:
    get_cookie_path = lambda: "/"

log = logging.getLogger(__name__)


def _get_flask_app():
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

    auth_manager = get_auth_manager()
    if not isinstance(auth_manager, FabAuthManager):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "FabAuthManager is not configured as the auth manager. "
                "Ensure AUTH_MANAGER is set to FabAuthManager in your Airflow configuration."
            ),
        )
    if not auth_manager.flask_app:
        with auth_manager._flask_app_lock:
            if not auth_manager.flask_app:
                try:
                    from airflow.providers.fab.www.app import create_app

                    auth_manager.flask_app = create_app(enable_plugins=False)
                except Exception:
                    log.exception("Failed to lazily initialize Flask app context for FabAuthManager")
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Failed to initialize Flask app context. Check logs for details.",
                    )
    return auth_manager.flask_app


@contextmanager
def _release_scoped_session() -> Generator[None, None, None]:
    """
    Remove the thread-local ``settings.Session`` when a sync FAB route returns.

    FastAPI runs sync route handlers on a threadpool thread. The FAB security manager reads and
    writes through ``settings.Session``, a ``scoped_session`` keyed by thread, so the session it
    opens stays bound to that worker thread. ``cleanup_session_middleware`` calls
    ``Session.remove()`` on the event-loop thread and never reaches it: the transaction opened by
    a read such as ``find_user`` or ``find_role`` is never ended, the backend stays
    ``idle in transaction`` and the connection is never returned to the pool. Once every worker
    thread holds one, the pool is exhausted and the API server stops answering.

    Removing the session here runs in the handler's own thread, after the response model is built.
    """
    try:
        yield
    finally:
        from airflow import settings

        if settings.Session is not None:
            try:
                settings.Session.remove()
            except Exception:
                log.warning("Failed to remove the scoped session after a FAB API call", exc_info=True)


@auth_router.post(
    "/token",
    response_model=LoginResponse,
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token(request: Request, body: dict[str, Any] = Body(...)) -> LoginResponse:
    """Generate a new API token."""
    with _get_flask_app().app_context(), _release_scoped_session():
        return FABAuthManagerLogin.create_token(headers=dict(request.headers), body=body)


@auth_router.post(
    "/token/cli",
    response_model=LoginResponse,
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token_cli(request: Request, body: dict[str, Any] = Body(...)) -> LoginResponse:
    """Generate a new CLI API token."""
    with _get_flask_app().app_context(), _release_scoped_session():
        return FABAuthManagerLogin.create_token(
            headers=dict(request.headers),
            body=body,
            expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time"),
        )


@auth_router.get(
    "/logout",
    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
)
def logout(request: Request) -> RedirectResponse:
    """Clear session cookies and redirect to the login page."""
    with _get_flask_app().app_context(), _release_scoped_session():
        login_url = get_auth_manager().get_url_login()
        secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
        cookie_path = get_cookie_path()
        response = RedirectResponse(login_url)
        response.delete_cookie(
            key="session",
            path=cookie_path,
            secure=secure,
            httponly=True,
        )
        response.delete_cookie(
            key=COOKIE_NAME_JWT_TOKEN,
            path=cookie_path,
            secure=secure,
            httponly=True,
        )
        return response
