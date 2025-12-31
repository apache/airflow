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

from typing import Any

from starlette import status
from starlette.exceptions import HTTPException

from airflow.providers.common.compat.sdk import conf
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginResponse
from airflow.providers.fab.www.utils import get_fab_auth_manager


class FABAuthManagerLogin:
    """Login Service for FABAuthManager."""

    @classmethod
    def create_token(
        cls,
        headers: dict[str, str],
        body: dict[str, Any],
        expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
    ) -> LoginResponse:
        """Create a new token."""
        auth_manager = get_fab_auth_manager()
        try:
            user = auth_manager.create_token(headers=headers, body=body)
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

        return LoginResponse(
            access_token=auth_manager.generate_jwt(
                user=user, expiration_time_in_seconds=expiration_time_in_seconds
            )
        )
