#
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
import os
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar, cast

import kerberos
from flask import Response, current_app, g, make_response, request
from requests_kerberos import HTTPKerberosAuth

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.common.compat.sdk import conf
from airflow.utils.net import getfqdn

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

log = logging.getLogger(__name__)

CLIENT_AUTH: tuple[str, str] | Any | None = HTTPKerberosAuth(service="airflow")


class KerberosService:
    """Class to keep information about the Kerberos Service initialized."""

    def __init__(self):
        self.service_name = None


class _KerberosAuth(NamedTuple):
    return_code: int | None
    user: str = ""
    token: str | None = None


# Stores currently initialized Kerberos Service
_KERBEROS_SERVICE = KerberosService()


def init_app(app):
    """Initialize application with kerberos."""
    hostname = app.config.get("SERVER_NAME")
    if not hostname:
        hostname = getfqdn()
    log.info("Kerberos: hostname %s", hostname)

    service = "airflow"

    _KERBEROS_SERVICE.service_name = f"{service}@{hostname}"

    if "KRB5_KTNAME" not in os.environ:
        os.environ["KRB5_KTNAME"] = conf.get("kerberos", "keytab")

    try:
        log.info("Kerberos init: %s %s", service, hostname)
        principal = kerberos.getServerPrincipalDetails(service, hostname)
    except kerberos.KrbError as err:
        log.warning("Kerberos: %s", err)
    else:
        log.info("Kerberos API: server is %s", principal)


def _unauthorized():
    """Indicate that authorization is required."""
    return Response("Unauthorized", 401, {"WWW-Authenticate": "Negotiate"})


def _forbidden():
    return Response("Forbidden", 403)


def _gssapi_authenticate(token) -> _KerberosAuth | None:
    state = None
    try:
        return_code, state = kerberos.authGSSServerInit(_KERBEROS_SERVICE.service_name)
        if return_code != kerberos.AUTH_GSS_COMPLETE:
            return _KerberosAuth(return_code=None)

        if (return_code := kerberos.authGSSServerStep(state, token)) == kerberos.AUTH_GSS_COMPLETE:
            return _KerberosAuth(
                return_code=return_code,
                user=kerberos.authGSSServerUserName(state),
                token=kerberos.authGSSServerResponse(state),
            )
        if return_code == kerberos.AUTH_GSS_CONTINUE:
            return _KerberosAuth(return_code=return_code)
        return _KerberosAuth(return_code=return_code)
    except kerberos.GSSError:
        return _KerberosAuth(return_code=None)
    finally:
        if state:
            kerberos.authGSSServerClean(state)


T = TypeVar("T", bound=Callable)


def find_user(username=None, email=None):
    security_manager = cast("FabAuthManager", get_auth_manager()).security_manager
    return security_manager.find_user(username=username, email=email)


def requires_authentication(function: T, find_user: Callable[[str], BaseUser] | None = find_user):
    """Decorate functions that require authentication with Kerberos."""

    @wraps(function)
    def decorated(*args, **kwargs):
        if current_app.config.get("AUTH_ROLE_PUBLIC", None):
            response = function(*args, **kwargs)
            return make_response(response)

        header = request.headers.get("Authorization")
        if header:
            token = "".join(header.split()[1:])
            auth = _gssapi_authenticate(token)
            if auth.return_code == kerberos.AUTH_GSS_COMPLETE:
                g.user = find_user(auth.user)
                response = function(*args, **kwargs)
                response = make_response(response)
                if auth.token is not None:
                    response.headers["WWW-Authenticate"] = f"negotiate {auth.token}"
                return response
            if auth.return_code != kerberos.AUTH_GSS_CONTINUE:
                return _forbidden()
        return _unauthorized()

    return cast("T", decorated)
