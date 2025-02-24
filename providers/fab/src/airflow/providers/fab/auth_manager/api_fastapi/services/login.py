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

from datetime import timedelta
from typing import cast

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginResponse
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager


class FABAuthManagerLogin:
    """Login Service for FABAuthManager."""

    @classmethod
    def create_token(cls, expiration_time_in_sec: int) -> LoginResponse:
        """Create a new token with the given expiration time."""
        auth_manager = cast(FabAuthManager, get_auth_manager())

        # There is no user base expiration in FAB, so we can't use this.
        # We can only set this globally but this would impact global expiration time for all tokens.
        auth_manager.appbuilder.app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(
            seconds=expiration_time_in_sec
        )

        # Refresh the token with the new expiration time
        return LoginResponse(jwt_token=auth_manager.security_manager.refresh_jwt_token())
