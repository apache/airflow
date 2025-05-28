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

import pytest

from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser


@pytest.fixture
def user():
    return KeycloakAuthManagerUser(
        user_id="user_id", name="name", access_token="access_token", refresh_token="refresh_token"
    )


class TestKeycloakAuthManagerUser:
    def test_get_id(self, user):
        assert user.get_id() == "user_id"

    def test_get_name(self, user):
        assert user.get_name() == "name"

    def test_get_access_token(self, user):
        assert user.access_token == "access_token"

    def test_get_refresh_token(self, user):
        assert user.refresh_token == "refresh_token"
