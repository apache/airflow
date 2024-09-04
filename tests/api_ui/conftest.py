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
from fastapi import Request
from fastapi.security import HTTPBasicCredentials
from fastapi.testclient import TestClient

from airflow.api_ui.app import create_app
from airflow.api_ui.utils.security import security
from airflow.security import permissions
from tests.conftest import initial_db_init
from tests.test_utils.api_connexion_utils import create_user


@pytest.fixture(autouse=True)
def cleanup():
    """
    Before each test re-init the database dropping and recreating the tables.
    This will allow to reset indexes to be able to assert auto-incremented primary keys.
    """
    initial_db_init()


@pytest.fixture
def unauthenticated_test_client():
    app = create_app(testing=True)
    return TestClient(app)


@pytest.fixture
def authenticated_test_client():
    app = create_app(testing=True)
    user = create_user(
        app.state.flask_app,
        username="test_user",
        role_name="test_role",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET),
        ],
    )

    def security_override(request: Request):
        return HTTPBasicCredentials(username=user.username, password=user.username)

    app.dependency_overrides[security] = security_override

    yield TestClient(app)

    app.dependency_overrides = {}
