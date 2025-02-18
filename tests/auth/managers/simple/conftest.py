#
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
from fastapi.testclient import TestClient
from flask import Flask

from airflow.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.www.extensions.init_appbuilder import init_appbuilder


@pytest.fixture
def auth_manager():
    return SimpleAuthManager(None)


@pytest.fixture
def auth_manager_with_appbuilder():
    flask_app = Flask(__name__)
    auth_manager = SimpleAuthManager()
    auth_manager.appbuilder = init_appbuilder(flask_app)
    return auth_manager


@pytest.fixture
def test_user():
    return SimpleAuthManagerUser(username="test", role="test")


@pytest.fixture
def test_admin():
    return SimpleAuthManagerUser(username="test", role="admin")


@pytest.fixture
def test_client(auth_manager):
    return TestClient(auth_manager.get_fastapi_app())


@pytest.fixture
def client(auth_manager):
    """This fixture is more flexible than test_client, as it allows to specify which apps to include."""

    def create_test_client():
        app = auth_manager.get_fastapi_app()
        return TestClient(app)

    return create_test_client
