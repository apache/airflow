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

import base64
import binascii
import hmac
import json
import logging
import secrets
import time
from hashlib import sha256
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
from airflow.providers.amazon.aws.auth_manager.constants import (
    CONF_ALLOW_IDP_INITIATED_LOGIN_KEY,
    CONF_SAML_METADATA_URL_KEY,
    CONF_SECTION_NAME,
)
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

# Name of the short-lived cookie recording which logins this browser has started.
COOKIE_NAME_LOGIN_STATE = "_awsam_login_state"

# The login flow is a redirect to the IdP and back. Ten minutes is generous for that and
# keeps a stale request id from lingering.
LOGIN_STATE_MAX_AGE = 600

# One entry per login started and not yet completed, so opening a second tab does not
# invalidate the first. The cap bounds the cookie; the oldest pending login is dropped.
MAX_PENDING_LOGINS = 5

LOGIN_MODE_REDIRECT = "login-redirect"
LOGIN_MODE_TOKEN = "login-token"
LOGIN_MODES = (LOGIN_MODE_REDIRECT, LOGIN_MODE_TOKEN)

NO_LOGIN_IN_PROGRESS = "No login in progress for this browser. Start the login from Airflow and try again."


def _is_secure_request(request: Request) -> bool:
    return request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))


def _allows_idp_initiated_login() -> bool:
    return conf.getboolean(CONF_SECTION_NAME, CONF_ALLOW_IDP_INITIATED_LOGIN_KEY, fallback=False)


def _sign_login_state(payload: str) -> str:
    # The API server secret key is already required to be identical across API servers, so a
    # login may start on one instance and finish on another.
    secret = conf.get("api", "secret_key", fallback="")
    return hmac.new(secret.encode(), payload.encode(), sha256).hexdigest()


def _read_pending_logins(request: Request) -> list[dict[str, Any]]:
    """
    Return the logins this browser started and has not yet completed.

    This cookie is the browser's half of the binding: it states that *this* browser asked for
    these AuthnRequests. A SAML assertion is signed by the identity provider, which
    authenticates *the identity in the response* -- it says nothing about *which browser
    asked*. Without this, an assertion issued for one login would be accepted in a browser
    that never started it, signing that browser in as the assertion's subject.

    Entries are signed so a response cannot contribute one of its own, and each carries its
    own deadline so an abandoned tab expires without affecting the others.
    """
    raw = request.cookies.get(COOKIE_NAME_LOGIN_STATE)
    if not raw:
        return []
    payload, _, signature = raw.rpartition(".")
    if not payload or not hmac.compare_digest(signature, _sign_login_state(payload)):
        log.warning("Ignoring a login state cookie that this deployment did not sign.")
        return []
    try:
        entries = json.loads(base64.urlsafe_b64decode(payload))
    except (ValueError, binascii.Error):
        log.warning("Ignoring a login state cookie that could not be decoded.")
        return []
    if not isinstance(entries, list):
        return []
    now = time.time()
    return [
        entry
        for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("exp"), (int, float)) and entry["exp"] > now
    ]


def _write_pending_logins(request: Request, response: Any, entries: list[dict[str, Any]]) -> None:
    cookie_path = get_cookie_path()
    if not entries:
        response.delete_cookie(COOKIE_NAME_LOGIN_STATE, path=cookie_path)
        return
    payload = base64.urlsafe_b64encode(json.dumps(entries, separators=(",", ":")).encode()).decode()
    response.set_cookie(
        COOKIE_NAME_LOGIN_STATE,
        f"{payload}.{_sign_login_state(payload)}",
        max_age=LOGIN_STATE_MAX_AGE,
        path=cookie_path,
        secure=_is_secure_request(request),
        httponly=True,
        samesite="lax",
    )


def _match_pending_login(pending: list[dict[str, Any]], relay_state: str) -> dict[str, Any] | None:
    """Find which of this browser's pending logins a response claims to answer."""
    mode, _, nonce = relay_state.partition(":")
    if not nonce or mode not in LOGIN_MODES:
        return None
    for entry in pending:
        if entry.get("mode") == mode and hmac.compare_digest(str(entry.get("nonce", "")), nonce):
            return entry
    return None


def _read_form(request: Request) -> FormData:
    """Read the request form from a synchronous context running in a worker thread."""

    async def _form() -> FormData:
        return await request.form()

    return anyio.from_thread.run(_form)


def _start_login(request: Request, mode: str) -> RedirectResponse:
    """
    Begin a login, remembering enough about it to recognise its response later.

    The nonce travels to the IdP in ``RelayState`` and returns with the response, naming
    which of this browser's pending logins that response answers. It is only an index into
    the signed cookie; the binding itself is the AuthnRequest id, which the IdP echoes in
    ``InResponseTo`` and which whoever posts the response cannot choose. The return mode is
    read back from the matched entry rather than from the form, so a response cannot select
    a mode the browser did not ask for.
    """
    saml_auth = _init_saml_auth(request)
    nonce = secrets.token_urlsafe(16)
    callback_url = saml_auth.login(f"{mode}:{nonce}")
    response = RedirectResponse(url=callback_url)
    pending = _read_pending_logins(request)[-(MAX_PENDING_LOGINS - 1) :]
    pending.append(
        {
            "nonce": nonce,
            "request_id": saml_auth.get_last_request_id(),
            "mode": mode,
            "exp": time.time() + LOGIN_STATE_MAX_AGE,
        }
    )
    _write_pending_logins(request, response, pending)
    return response


@login_router.get("/login")
def login(request: Request):
    """Initiate the authentication."""
    return _start_login(request, LOGIN_MODE_REDIRECT)


@login_router.get("/login/token")
def login_token(request: Request) -> RedirectResponse:
    """Initiate the authentication to create a token."""
    return _start_login(request, LOGIN_MODE_TOKEN)


@login_router.post("/login_callback")
def login_callback(request: Request):
    """Authenticate the user."""
    form_data = _read_form(request)
    pending = _read_pending_logins(request)
    relay_state = form_data.get("RelayState")
    # A multipart upload posted under this name is not a relay state, so it matches nothing.
    matched = _match_pending_login(pending, relay_state) if isinstance(relay_state, str) else None

    if matched is not None:
        mode = matched["mode"]
        expected_request_id = matched["request_id"]
    elif _allows_idp_initiated_login():
        # Opted in: this deployment accepts assertions that no login from this browser asked
        # for, so the Identity Center access portal tile keeps working. Nothing ties such a
        # response to the browser receiving it -- that is the trade the option names.
        mode = LOGIN_MODE_REDIRECT
        expected_request_id = None
    else:
        log.error("SAML response received that answers no login started by this browser.")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, NO_LOGIN_IN_PROGRESS)

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

    if expected_request_id is None and saml_auth.get_last_response_in_response_to() is not None:
        # An unsolicited assertion answers no request, so one carrying InResponseTo is a
        # solicited assertion being replayed here. python3-saml skips the comparison entirely
        # when it is given no request id, so this is checked rather than assumed.
        log.error("Unsolicited SAML response carries InResponseTo; refusing it as a replay.")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid SAML response")

    attributes = saml_auth.get_attributes()
    user = AwsAuthManagerUser(
        user_id=attributes["id"][0],
        groups=attributes["groups"] or [],
        username=saml_auth.get_nameid(),
        email=attributes["email"][0] if "email" in attributes else None,
    )
    url = conf.get("api", "base_url", fallback="/")
    token = get_auth_manager().generate_jwt(user)

    response: Any
    if mode == LOGIN_MODE_REDIRECT:
        response = RedirectResponse(url=url, status_code=303)
        cookie_path = get_cookie_path()
        secure = _is_secure_request(request)
        # In Airflow 3.1.1 authentication changes, front-end no longer handle the token
        # See https://github.com/apache/airflow/pull/55506
        if AIRFLOW_V_3_1_1_PLUS:
            response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, path=cookie_path, secure=secure, httponly=True)
        else:
            response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, path=cookie_path, secure=secure)
    else:
        # Returned as a JSONResponse rather than the bare model so the consumed login state
        # can be cleared on this path too. Left in place, the same assertion could be reposted
        # to mint further tokens until the cookie expired.
        response = JSONResponse(content=LoginResponse(access_token=token).model_dump())

    if matched is not None:
        # One response per request. Logins started in other tabs keep theirs.
        _write_pending_logins(request, response, [entry for entry in pending if entry is not matched])
    return response


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
