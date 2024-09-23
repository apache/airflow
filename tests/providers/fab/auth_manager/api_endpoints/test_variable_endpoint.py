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

from airflow.models import Variable
from airflow.security import permissions
from tests.providers.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user
from tests.test_utils.db import clear_db_variables

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api

    create_user(
        app,
        username="test_read_only",
        role_name="TestReadOnly",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE),
        ],
    )
    create_user(
        app,
        username="test_delete_only",
        role_name="TestDeleteOnly",
        permissions=[
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE),
        ],
    )

    yield app

    delete_user(app, username="test_read_only")
    delete_user(app, username="test_delete_only")


class TestVariableEndpoint:
    @pytest.fixture(autouse=True)
    def setup_method(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_variables()

    def teardown_method(self) -> None:
        clear_db_variables()


class TestGetVariable(TestVariableEndpoint):
    @pytest.mark.parametrize(
        "user, expected_status_code",
        [
            ("test_read_only", 200),
            ("test_delete_only", 403),
        ],
    )
    def test_read_variable(self, user, expected_status_code):
        expected_value = '{"foo": 1}'
        Variable.set("TEST_VARIABLE_KEY", expected_value)
        response = self.client.get(
            "/api/v1/variables/TEST_VARIABLE_KEY", environ_overrides={"REMOTE_USER": user}
        )
        assert response.status_code == expected_status_code
        if expected_status_code == 200:
            assert response.json == {"key": "TEST_VARIABLE_KEY", "value": expected_value, "description": None}
