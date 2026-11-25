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

from keycloak import KeycloakConnectionError, KeycloakError

from airflow.providers.common.compat.sdk import AirflowException

try:
    from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
except ImportError:

    class AuthManagerRefreshTokenExpiredException(Exception):  # type: ignore[no-redef]
        """In case it is using a version of Airflow without ``AuthManagerRefreshTokenExpiredException``."""


class KeycloakAuthorizationError(AirflowException):
    """Keycloak could not answer an authorization request."""


class KeycloakAuthorizationRequestError(KeycloakAuthorizationError):
    """Keycloak rejected an authorization request as invalid."""


class KeycloakUnexpectedResponseError(KeycloakAuthorizationError):
    """Keycloak answered an authorization request with an unexpected HTTP status."""


def is_keycloak_outage(error: KeycloakError) -> bool:
    """Return whether ``error`` means Keycloak could not be reached or failed on its side."""
    return isinstance(error, KeycloakConnectionError) or (error.response_code or 0) >= 500


__all__ = [
    "AuthManagerRefreshTokenExpiredException",
    "KeycloakAuthorizationError",
    "KeycloakAuthorizationRequestError",
    "KeycloakUnexpectedResponseError",
    "is_keycloak_outage",
]
