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

from typing import TYPE_CHECKING, cast

from flask_appbuilder.const import AUTH_LDAP
from starlette import status
from starlette.exceptions import HTTPException

from airflow.api_fastapi.app import get_auth_manager
from airflow.configuration import conf
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginBody, LoginResponse

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
    from airflow.providers.fab.auth_manager.models import User


class FABAuthManagerLogin:
    """Login Service for FABAuthManager."""

    @classmethod
    def create_token(
        cls, body: LoginBody, expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time")
    ) -> LoginResponse:
        """Create a new token."""
        if not body.username or not body.password:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Username and password must be provided"
            )

        auth_manager = cast("FabAuthManager", get_auth_manager())
        user: User | None = None

        if auth_manager.security_manager.auth_type == AUTH_LDAP:
            user = auth_manager.security_manager.auth_user_ldap(
                body.username, body.password, rotate_session_id=False
            )
        if user is None:
            user = auth_manager.security_manager.auth_user_db(
                body.username, body.password, rotate_session_id=False
            )

        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

        return LoginResponse(
            access_token=auth_manager.generate_jwt(
                user=user, expiration_time_in_seconds=expiration_time_in_seconds
            )
        )
