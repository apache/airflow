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

from typing import TYPE_CHECKING

import requests
from keycloak import KeycloakOpenID
from starlette import status
from starlette.exceptions import HTTPException
from starlette.responses import RedirectResponse

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

if TYPE_CHECKING:
    from fastapi import Request
    from starlette.datastructures import URL

    from airflow.providers.keycloak.auth_manager.datamodels.login import LoginBody


class KeycloakAuthManagerLogin:
    """Service for handling login operations in Keycloak Auth Manager."""

    @classmethod
    def _get_keycloak_client(cls) -> KeycloakOpenID:
        client_id = conf.get("keycloak_auth_manager", "client_id")
        client_secret = conf.get("keycloak_auth_manager", "client_secret")
        realm = conf.get("keycloak_auth_manager", "realm")
        server_url = conf.get("keycloak_auth_manager", "server_url")

        return KeycloakOpenID(
            server_url=server_url,
            client_id=client_id,
            client_secret_key=client_secret,
            realm_name=realm,
        )

    @classmethod
    def get_auth_url(cls, request: Request) -> str:
        """
        Keycloak authentication URL for redirecting users to the login page.

        :param request: The FastAPI request object.
        :return: The Keycloak authentication URL.
        """
        client = cls._get_keycloak_client()
        redirect_uri = request.url_for("login_callback")
        auth_url = client.auth_url(redirect_uri=str(redirect_uri), scope="openid")
        return auth_url

    @classmethod
    def redirect_to_login(cls, request: Request) -> RedirectResponse:
        """Redirects the user to the Keycloak login page."""
        return RedirectResponse(cls.get_auth_url(request=request))

    @classmethod
    def _get_user_token_from_keycloak(cls, user_name: str, password: str) -> dict:
        keycloak_client = cls._get_keycloak_client()
        try:
            return keycloak_client.token(
                username=user_name,
                password=password,
                grant_type="password",
                scope="openid",
            )
        except requests.HTTPError as e:
            raise AirflowException(f"Unexpected error during login: {e}") from e

    @classmethod
    def refresh_token(
        cls,
        code: str,
        redirect_uri: URL,
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> str:
        """
        Authenticate user with given configuration.

        :param code: str authorization code received from Keycloak
        :param redirect_uri: URL redirect URI to be used in the authentication process
        :param expiration_time_in_seconds: int expiration time in seconds
        """
        client = cls._get_keycloak_client()
        tokens = client.token(
            grant_type="authorization_code",
            code=code,
            redirect_uri=str(redirect_uri),
        )
        userinfo = client.userinfo(tokens["access_token"])
        user = KeycloakAuthManagerUser(
            user_id=userinfo["sub"],
            name=userinfo["preferred_username"],
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
        )

        token = get_auth_manager().generate_jwt(
            user=user, expiration_time_in_seconds=expiration_time_in_seconds
        )
        return token

    @classmethod
    def create_token(
        cls,
        body: LoginBody,
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> str:
        """
        Authenticate user with given configuration.

        :param body: LoginBody should include username and password
        :param expiration_time_in_seconds: int expiration time in seconds
        """
        client = cls._get_keycloak_client()
        if not body.username or not body.password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username and password must be provided",
            )
        tokens = cls._get_user_token_from_keycloak(
            user_name=body.username,
            password=body.password,
        )
        if not tokens:
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
        return get_auth_manager().generate_jwt(
            user=user, expiration_time_in_seconds=expiration_time_in_seconds
        )

    @staticmethod
    def create_token_all_admins(
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> str:
        is_simple_auth_manager_all_admins = conf.getboolean("keycloak_auth_manager", "all_admins")
        if not is_simple_auth_manager_all_admins:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "This method is only allowed if ``[keycloak_auth_manager] all_admins`` is True",
            )

        return KeycloakAuthManagerLogin._create_anonymous_admin_user(
            expiration_time_in_seconds=expiration_time_in_seconds
        )

    @classmethod
    def _create_anonymous_admin_user(
        cls,
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> str:
        user = KeycloakAuthManagerUser(
            user_id="admin",  # Placeholder user ID for anonymous admin
            name="Anonymous",
            access_token="",  # Access token is not used in this context
            refresh_token="",  # Refresh token is not used in this context
        )
        return get_auth_manager().generate_jwt(
            user=user, expiration_time_in_seconds=expiration_time_in_seconds
        )

    @classmethod
    def logout(cls, request: Request) -> str:
        """
        Logout the user by clearing the session and cookies.

        :param request: The FastAPI request object.
        :return: The URL to redirect to after logout.
        """
        all_admins = conf.getboolean("keycloak_auth_manager", "all_admins")
        if not all_admins:

            def get_keycloak_logout_url(server_url, realm):
                if server_url.endswith("/"):
                    server_url = server_url[:-1]
                return f"{server_url}/realms/{realm}/protocol/openid-connect/logout"

            logout_url = get_keycloak_logout_url(
                server_url=conf.get("keycloak_auth_manager", "server_url"),
                realm=conf.get("keycloak_auth_manager", "realm"),
            )
            client_id = conf.get("keycloak_auth_manager", "client_id")
            return f"{logout_url}?post_logout_redirect_uri={request.url_for('login')}&scope=openid&client_id={client_id}"
        # If all_admins is True, we can just redirect to the base URL
        return conf.get("api", "base_url", fallback="/")
