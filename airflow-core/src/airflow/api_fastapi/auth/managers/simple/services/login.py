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
from airflow.api_fastapi.auth.managers.simple.datamodels.login import LoginBody
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import conf


class SimpleAuthManagerLogin:
    """Service for login."""

    @staticmethod
    def create_token(
        body: LoginBody, expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time")
    ) -> str:
        """
        Authenticate user with given configuration.

        :param body: LoginBody should include username and password
        :param expiration_time_in_seconds: int expiration time in seconds
        """
        is_simple_auth_manager_all_admins = conf.getboolean("core", "simple_auth_manager_all_admins")
        if is_simple_auth_manager_all_admins:
            return SimpleAuthManagerLogin._create_anonymous_admin_user(
                expiration_time_in_seconds=expiration_time_in_seconds
            )

        if not body.username or not body.password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username and password must be provided",
            )

        users = SimpleAuthManager.get_users()
        passwords = SimpleAuthManager.get_passwords()
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

        return get_auth_manager().generate_jwt(
            user=user, expiration_time_in_seconds=expiration_time_in_seconds
        )

    @staticmethod
    def create_token_all_admins(
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> str:
        is_simple_auth_manager_all_admins = conf.getboolean("core", "simple_auth_manager_all_admins")
        if not is_simple_auth_manager_all_admins:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "This method is only allowed if ``[core] simple_auth_manager_all_admins`` is True",
            )

        return SimpleAuthManagerLogin._create_anonymous_admin_user(
            expiration_time_in_seconds=expiration_time_in_seconds
        )

    @staticmethod
    def _create_anonymous_admin_user(
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> str:
        user = SimpleAuthManagerUser(
            username="Anonymous",
            role="ADMIN",
        )
        return get_auth_manager().generate_jwt(
            user=user, expiration_time_in_seconds=expiration_time_in_seconds
        )
