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
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import anyio
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse, RedirectResponse

from airflow.api_fastapi.app import (
    AUTH_MANAGER_FASTAPI_APP_PREFIX,
    get_auth_manager,
)
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.providers.amazon.aws.auth_manager.constants import CONF_SAML_METADATA_URL_KEY, CONF_SECTION_NAME
from airflow.providers.amazon.aws.auth_manager.datamodels.login import LoginResponse
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_1_1_PLUS, AIRFLOW_V_3_1_8_PLUS
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from starlette.datastructures import FormData

if AIRFLOW_V_3_1_8_PLUS:
    from airflow.api_fastapi.app import get_cookie_path
else:
    get_cookie_path = lambda: "/"

try:
    from onelogin.saml2.auth import OneLogin_Saml2_Auth
    from onelogin.saml2.errors import OneLogin_Saml2_Error
    from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser
except ImportError:
    raise ImportError(
        "AWS auth manager requires the python3-saml library but it is not installed by default. "
        "Please install the python3-saml library by running: "
        "pip install apache-airflow-providers-amazon[python3-saml]"
    )

log = logging.getLogger(__name__)
login_router = AirflowRouter(tags=["AWSAuthManagerLogin"])

# Name of the short-lived cookie that ties a SAML response back to the browser that
# started the flow.
COOKIE_NAME_LOGIN_STATE = "_awsam_login_state"

# The login flow is a redirect to the IdP and back. Ten minutes is generous for that and
# keeps a stale request id from lingering.
LOGIN_STATE_MAX_AGE = 600


def _is_secure_request(request: Request) -> bool:
    return request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))


def _set_login_state(request: Request, response: Any, request_id: str, relay_state: str) -> None:
    """
    Remember the AuthnRequest this browser started, so the response can be tied back to it.

    A SAML assertion is signed by the identity provider, which authenticates *the identity in
    the response* -- it says nothing about *which browser asked*. Without this binding, an
    assertion obtained by an attacker can be replayed into a victim's browser, logging the
    victim into the attacker's account. ``RelayState`` only selects the return mode and is
    attacker-controlled, so it is remembered here too rather than trusted from the form.
    """
    response.set_cookie(
        COOKIE_NAME_LOGIN_STATE,
        f"{request_id}:{relay_state}",
        max_age=LOGIN_STATE_MAX_AGE,
        path=get_cookie_path(),
        secure=_is_secure_request(request),
        httponly=True,
        samesite="lax",
    )


def _pop_login_state(request: Request) -> tuple[str, str]:
    """Return the ``(request id, relay state)`` this browser started the flow with."""
    raw = request.cookies.get(COOKIE_NAME_LOGIN_STATE)
    if not raw or ":" not in raw:
        log.error("SAML response received without a login state cookie for this browser.")
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "No login in progress for this browser. Start the login from Airflow and try again.",
        )
    request_id, _, relay_state = raw.partition(":")
    if not request_id:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "No login in progress for this browser. Start the login from Airflow and try again.",
        )
    return request_id, relay_state


def _read_form(request: Request) -> FormData:
    """Read the request form from a synchronous context running in a worker thread."""

    async def _form() -> FormData:
        return await request.form()

    return anyio.from_thread.run(_form)


@login_router.get("/login")
def login(request: Request):
    """Initiate the authentication."""
    saml_auth = _init_saml_auth(request)
    callback_url = saml_auth.login("login-redirect")
    response = RedirectResponse(url=callback_url)
    _set_login_state(request, response, saml_auth.get_last_request_id(), "login-redirect")
    return response


@login_router.get("/login/token")
def login_token(request: Request) -> RedirectResponse:
    """Initiate the authentication to create a token."""
    saml_auth = _init_saml_auth(request)
    callback_url = saml_auth.login("login-token")
    response = RedirectResponse(url=callback_url)
    _set_login_state(request, response, saml_auth.get_last_request_id(), "login-token")
    return response


@login_router.post("/login_callback")
def login_callback(request: Request):
    """Authenticate the user."""
    expected_request_id, expected_relay_state = _pop_login_state(request)
    saml_auth = _init_saml_auth(request)
    try:
        # Passing the request id makes python3-saml enforce InResponseTo. Without it the
        # check is skipped entirely and any valid assertion is accepted.
        saml_auth.process_response(request_id=expected_request_id)
    except OneLogin_Saml2_Error as e:
        log.exception(e)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to authenticate")
    errors = saml_auth.get_errors()
    is_authenticated = saml_auth.is_authenticated()
    if not is_authenticated:
        error_reason = saml_auth.get_last_error_reason()
        log.error("Failed to authenticate")
        log.error("Errors: %s", errors)
        log.error("Error reason: %s", error_reason)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Failed to authenticate: {error_reason}")

    attributes = saml_auth.get_attributes()
    user = AwsAuthManagerUser(
        user_id=attributes["id"][0],
        groups=attributes["groups"] or [],
        username=saml_auth.get_nameid(),
        email=attributes["email"][0] if "email" in attributes else None,
    )
    url = conf.get("api", "base_url", fallback="/")
    token = get_auth_manager().generate_jwt(user)

    form_data = _read_form(request)
    relay_state = form_data["RelayState"]
    if relay_state != expected_relay_state:
        # The return mode is decided when the flow starts. Honouring the form value would
        # let the response select a different one than the browser asked for.
        log.error("RelayState %r does not match the login this browser started.", relay_state)
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid relay state")

    if relay_state == "login-redirect":
        response = RedirectResponse(url=url, status_code=303)
        secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
        # In Airflow 3.1.1 authentication changes, front-end no longer handle the token
        # See https://github.com/apache/airflow/pull/55506
        cookie_path = get_cookie_path()
        if AIRFLOW_V_3_1_1_PLUS:
            response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, path=cookie_path, secure=secure, httponly=True)
        else:
            response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, path=cookie_path, secure=secure)
        response.delete_cookie(COOKIE_NAME_LOGIN_STATE, path=cookie_path)
        return response
    if relay_state == "login-token":
        # Returned as a JSONResponse rather than the bare model so the consumed login
        # state is cleared here too. Leaving it set would keep the request id acceptable
        # until the cookie expired, so the same assertion could mint further tokens.
        token_response = JSONResponse(content=LoginResponse(access_token=token).model_dump())
        token_response.delete_cookie(COOKIE_NAME_LOGIN_STATE, path=get_cookie_path())
        return token_response
    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Invalid relay state: {relay_state}")


def _init_saml_auth(request: Request) -> OneLogin_Saml2_Auth:
    request_data = _prepare_request(request)
    base_url = conf.get(section="api", key="base_url")
    settings = {
        # We want to keep this flag on in case of errors.
        # It provides an error reasons, if turned off, it does not
        "debug": True,
        "sp": {
            "entityId": "aws-auth-manager-saml-client",
            "assertionConsumerService": {
                "url": f"{base_url.rstrip('/')}{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login_callback",
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
        },
    }
    merged_settings = OneLogin_Saml2_IdPMetadataParser.merge_settings(_get_idp_data(), settings)
    return OneLogin_Saml2_Auth(request_data, merged_settings)


def _prepare_request(request: Request) -> dict:
    parsed = urlparse(conf.get("api", "base_url", fallback="http://localhost"))
    host = parsed.hostname

    data: dict[str, Any] = {
        "https": "on" if request.url.scheme == "https" else "off",
        "http_host": host,
        "server_port": request.url.port,
        "script_name": request.url.path,
        "get_data": request.query_params,
        "post_data": {},
    }
    form_data = _read_form(request)
    if "SAMLResponse" in form_data:
        data["post_data"]["SAMLResponse"] = form_data["SAMLResponse"]
    if "RelayState" in form_data:
        data["post_data"]["RelayState"] = form_data["RelayState"]
    return data


def _get_idp_data() -> dict:
    saml_metadata_url = conf.get_mandatory_value(CONF_SECTION_NAME, CONF_SAML_METADATA_URL_KEY)
    return OneLogin_Saml2_IdPMetadataParser.parse_remote(saml_metadata_url)
