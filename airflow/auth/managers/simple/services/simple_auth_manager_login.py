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

from fastapi import HTTPException, status

from airflow.api_fastapi.app import get_auth_manager
from airflow.auth.managers.simple.datamodels.login import LoginBody, LoginResponse
from airflow.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import conf
from airflow.utils.jwt_signer import JWTSigner


class SimpleAuthManagerLogin:
    """Service for login."""

    @classmethod
    def create_token(cls, body: LoginBody, expiration_time_in_sec: int) -> LoginResponse:
        """
        Authenticate user with given configuration.

        :param body: LoginBody should include username and password
        :param expiration_time_in_sec: int expiration time in seconds

        :return: LoginResponse
        """
        if not body.username or not body.password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username and password must be provided",
            )

        users = SimpleAuthManager.get_users()
        passwords = SimpleAuthManager.get_passwords(users)
        found_users = [
            user
            for user in users
            if user["username"] == body.username and passwords[user["username"]] == body.password
        ]

        if len(found_users) == 0:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
            )

        user = SimpleAuthManagerUser(
            username=body.username,
            role=found_users[0]["role"],
        )

        signer = JWTSigner(
            secret_key=conf.get("api", "auth_jwt_secret"),
            expiration_time_in_seconds=expiration_time_in_sec,
            audience="front-apis",
        )
        token = signer.generate_signed_token(get_auth_manager().serialize_user(user))
        return LoginResponse(jwt_token=token)
