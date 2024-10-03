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

from unittest.mock import patch

import pytest

from airflow.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from tests.test_utils.api_connexion_utils import assert_401
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class BaseTestAuth:
    @pytest.fixture(autouse=True)
    def set_attrs(self, minimal_app_for_api):
        self.app = minimal_app_for_api


class TestSessionAuth(BaseTestAuth):
    @pytest.fixture(autouse=True, scope="class")
    def with_session_backend(self, minimal_app_for_api):
        from airflow.www.extensions.init_security import init_api_auth

        old_auth = getattr(minimal_app_for_api, "api_auth")

        try:
            with conf_vars({("api", "auth_backends"): "airflow.api.auth.backend.session"}):
                init_api_auth(minimal_app_for_api)
                yield
        finally:
            setattr(minimal_app_for_api, "api_auth", old_auth)

    @patch.object(SimpleAuthManager, "is_logged_in", return_value=True)
    @patch.object(
        SimpleAuthManager, "get_user", return_value=SimpleAuthManagerUser(username="test", role="admin")
    )
    def test_success(self, *args):
        clear_db_pools()

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools")
            assert response.status_code == 200
            assert response.json == {
                "pools": [
                    {
                        "name": "default_pool",
                        "slots": 128,
                        "occupied_slots": 0,
                        "running_slots": 0,
                        "queued_slots": 0,
                        "scheduled_slots": 0,
                        "deferred_slots": 0,
                        "open_slots": 128,
                        "description": "Default pool",
                        "include_deferred": False,
                    },
                ],
                "total_entries": 1,
            }

    def test_failure(self):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools")
            assert response.status_code == 401
            assert response.headers["Content-Type"] == "application/problem+json"
            assert_401(response)
