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

from airflow.security import permissions
from tests.test_utils.api_connexion_utils import create_user, delete_user

pytestmark = pytest.mark.db_test


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type:ignore
        username="test",
        role_name="Test",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG)],  # type: ignore
    )

    yield minimal_app_for_api

    delete_user(app, username="test")  # type: ignore


class TestSession:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore

    def test_session_not_created_on_api_request(self):
        self.client.get("api/v1/dags", environ_overrides={"REMOTE_USER": "test"})
        assert all(cookie.name != "session" for cookie in self.client.cookie_jar)

    def test_session_not_created_on_health_endpoint_request(self):
        self.client.get("health")
        assert all(cookie.name != "session" for cookie in self.client.cookie_jar)
