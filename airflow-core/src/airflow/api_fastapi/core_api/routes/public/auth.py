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

import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import structlog
from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.exc import IntegrityError

from airflow.api_fastapi.app import get_cookie_path
from airflow.api_fastapi.auth.cli_authorization_code import (
    CliAuthorizationCodeUnavailableError,
    InvalidCliAuthorizationCodeError,
    build_loopback_redirect_uri,
    validate_cli_authorization_code,
)
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.auth import CliTokenBody, CliTokenResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    AuthManagerDep,
    bearer_scheme,
    collect_request_tokens,
    is_safe_url,
    oauth2_scheme,
)
from airflow.configuration import conf
from airflow.models.revoked_token import RevokedToken

log = structlog.get_logger(logger_name=__name__)

auth_router = AirflowRouter(tags=["Login"], prefix="/auth")


@auth_router.get(
    "/login",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_307_TEMPORARY_REDIRECT, status.HTTP_400_BAD_REQUEST]
    ),
)
def login(request: Request, auth_manager: AuthManagerDep, next: None | str = None) -> RedirectResponse:
    """Redirect to the login URL depending on the AuthManager configured."""
    login_url = auth_manager.get_url_login()

    if next and not is_safe_url(next, request=request):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Invalid or unsafe next URL")

    if next:
        login_url += f"?{urlencode({'next': next})}"

    return RedirectResponse(login_url)


@auth_router.get(
    "/logout",
    responses=create_openapi_http_exception_doc([status.HTTP_307_TEMPORARY_REDIRECT]),
)
def logout(
    request: Request,
    auth_manager: AuthManagerDep,
    # Kept for the OpenAPI security spec so ``/docs`` still renders the OAuth2 password
    # login form. It resolves to the same ``Authorization: Bearer`` header
    # ``bearer_scheme`` reads, so the value is unused at runtime.
    _oauth_token: str | None = Depends(oauth2_scheme),
    bearer_credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> RedirectResponse:
    """Logout the user."""
    # Invalidate both tokens from the Authorization header and the _token cookie, if present.
    for token_str in collect_request_tokens(request, bearer_credentials):
        auth_manager.revoke_token(token_str)

    # The local session cookie is cleared on every path, including the redirect to an external
    # logout URL: revocation above is recorded server side and can fail, so it must not be the
    # only thing that ends the browser's session.
    secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
    cookie_path = get_cookie_path()
    response = RedirectResponse(auth_manager.get_url_logout() or auth_manager.get_url_login())
    response.delete_cookie(
        key=COOKIE_NAME_JWT_TOKEN,
        path=cookie_path,
        secure=secure,
        httponly=True,
    )
    # Clear any stale _token cookie at root path "/" left by
    # older Airflow instances to prevent redirect loops.
    if cookie_path != "/":
        response.delete_cookie(
            key=COOKIE_NAME_JWT_TOKEN,
            path="/",
            secure=secure,
            httponly=True,
        )

    return response


@auth_router.post(
    "/cli/token",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]),
)
def create_cli_token(
    body: CliTokenBody,
    session: SessionDep,
) -> CliTokenResponse:
    """
    Exchange a code approved in the UI (``airflowctl auth login --web``) for the CLI token it carries.

    Unauthenticated, like an OAuth token endpoint: the code and the PKCE verifier are the credential.
    """
    try:
        claims = validate_cli_authorization_code(
            body.code,
            code_verifier=body.code_verifier,
            redirect_uri=build_loopback_redirect_uri(body.port),
        )
    except (InvalidCliAuthorizationCodeError, CliAuthorizationCodeUnavailableError) as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=str(e))

    # The code's jti goes into the table logout uses, so a replay fails on the primary key even when two
    # API server replicas receive it at the same time. The row must outlive the code: validation accepts
    # the code for jwt_leeway past exp, while the table's cleanup drops rows whose exp has passed.
    try:
        session.add(
            RevokedToken(
                jti=claims["jti"],
                exp=datetime.fromtimestamp(claims["exp"], tz=UTC)
                + timedelta(seconds=conf.getint("api_auth", "jwt_leeway")),
            )
        )
        session.flush()
    except IntegrityError:
        session.rollback()
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, detail="The authorization code has already been used"
        )

    expires_in_seconds = max(claims["token_expires_at"] - int(time.time()), 1)
    log.info(
        "Handed out the CLI token approved in the browser",
        user=claims.get("name"),
        expires_in_seconds=expires_in_seconds,
    )
    return CliTokenResponse(access_token=claims["token"], expires_in_seconds=expires_in_seconds)
