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
from typing import Any

import anyio
from fastapi import HTTPException, Request
from starlette import status
from starlette.responses import RedirectResponse

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.configuration import conf
from airflow.providers.amazon.aws.auth_manager.constants import CONF_SAML_METADATA_URL_KEY, CONF_SECTION_NAME
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser

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


@login_router.get("/login")
def login(request: Request):
    """Authenticate the user."""
    saml_auth = _init_saml_auth(request)
    callback_url = saml_auth.login()
    return RedirectResponse(url=callback_url)


@login_router.post("/login_callback")
def login_callback(request: Request):
    """Authenticate the user."""
    saml_auth = _init_saml_auth(request)
    try:
        saml_auth.process_response()
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
        groups=attributes["groups"],
        username=saml_auth.get_nameid(),
        email=attributes["email"][0] if "email" in attributes else None,
    )
    return RedirectResponse(url=f"/webapp?token={get_auth_manager().get_jwt_token(user)}", status_code=303)


def _init_saml_auth(request: Request) -> OneLogin_Saml2_Auth:
    request_data = _prepare_request(request)
    base_url = conf.get(section="fastapi", key="base_url")
    settings = {
        # We want to keep this flag on in case of errors.
        # It provides an error reasons, if turned off, it does not
        "debug": True,
        "sp": {
            "entityId": "aws-auth-manager-saml-client",
            "assertionConsumerService": {
                "url": f"{base_url}/auth/login_callback",
                "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
            },
        },
    }
    merged_settings = OneLogin_Saml2_IdPMetadataParser.merge_settings(_get_idp_data(), settings)
    return OneLogin_Saml2_Auth(request_data, merged_settings)


def _prepare_request(request: Request) -> dict:
    host = request.headers.get("host", request.client.host if request.client else "localhost")
    data: dict[str, Any] = {
        "https": "on" if request.url.scheme == "https" else "off",
        "http_host": host,
        "server_port": request.url.port,
        "script_name": request.url.path,
        "get_data": request.query_params,
        "post_data": {},
    }
    form_data = anyio.from_thread.run(request.form)
    if "SAMLResponse" in form_data:
        data["post_data"]["SAMLResponse"] = form_data["SAMLResponse"]
    if "RelayState" in form_data:
        data["post_data"]["RelayState"] = form_data["RelayState"]
    return data


def _get_idp_data() -> dict:
    saml_metadata_url = conf.get_mandatory_value(CONF_SECTION_NAME, CONF_SAML_METADATA_URL_KEY)
    return OneLogin_Saml2_IdPMetadataParser.parse_remote(saml_metadata_url)
