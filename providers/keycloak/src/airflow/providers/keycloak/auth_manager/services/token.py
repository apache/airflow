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
from airflow.providers.common.compat.sdk import conf
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
