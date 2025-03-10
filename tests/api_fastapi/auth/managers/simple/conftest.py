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

import os

import pytest
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def auth_manager():
    auth_manager = SimpleAuthManager()
    if os.path.exists(auth_manager.get_generated_password_file()):
        os.remove(auth_manager.get_generated_password_file())
    return auth_manager


@pytest.fixture
def test_user():
    return SimpleAuthManagerUser(username="test", role="test")


@pytest.fixture
def test_admin():
    return SimpleAuthManagerUser(username="test", role="admin")


@pytest.fixture
def test_client():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
        }
    ):
        return TestClient(create_app("core"))
