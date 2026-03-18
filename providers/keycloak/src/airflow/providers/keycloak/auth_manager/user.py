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

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser


class KeycloakAuthManagerUser(BaseUser):
    """User model for users managed by Keycloak auth manager."""

    def __init__(self, *, user_id: str, name: str, access_token: str, refresh_token: str | None) -> None:
        self.user_id = user_id
        self.name = name
        self.access_token = access_token
        self.refresh_token = refresh_token

    def get_id(self) -> str:
        return self.user_id

    def get_name(self) -> str:
        return self.name
