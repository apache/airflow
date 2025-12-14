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

from fastapi import HTTPException
from keycloak import KeycloakAuthenticationError
from starlette import status

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser


def create_token_for(
    username: str,
    password: str,
    expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
) -> str:
    client = KeycloakAuthManager.get_keycloak_client()

    try:
        tokens = client.token(username, password)
    except KeycloakAuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    userinfo = client.userinfo(tokens["access_token"])
    user = KeycloakAuthManagerUser(
        user_id=userinfo["sub"],
        name=userinfo["preferred_username"],
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
    )

    return get_auth_manager().generate_jwt(user, expiration_time_in_seconds=expiration_time_in_seconds)


def create_token_for_client_credentials(
    client_id: str,
    client_secret: str,
    expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
) -> str:
    """
    Create token using OAuth2 client_credentials grant type.

    This authentication flow uses the provided client_id and client_secret
    to obtain a token for a service account. The Keycloak client must have:
    - Service accounts roles: ON
    - Client Authentication: ON (confidential client)

    The service account must be configured with the appropriate roles/permissions.
    """
    # Get Keycloak client with service account credentials
    client = KeycloakAuthManager.get_keycloak_client(
        client_id=client_id,
        client_secret=client_secret,
    )

    try:
        tokens = client.token(grant_type="client_credentials")
    except KeycloakAuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Client credentials authentication failed",
        )

    # For client_credentials, get the service account user info
    # The token represents the service account associated with the client
    userinfo = client.userinfo(tokens["access_token"])
    user = KeycloakAuthManagerUser(
        user_id=userinfo["sub"],
        name=userinfo.get("preferred_username", userinfo.get("clientId", "service-account")),
        access_token=tokens["access_token"],
        refresh_token=tokens.get(
            "refresh_token", ""
        ),  # client_credentials may not return refresh_token (RFC6749 section 4.4.3)
    )

    return get_auth_manager().generate_jwt(user, expiration_time_in_seconds=expiration_time_in_seconds)
