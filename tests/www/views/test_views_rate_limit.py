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

from airflow.www.app import create_app
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules
from tests.test_utils.www import client_with_login

pytestmark = pytest.mark.db_test


@pytest.fixture
def app_with_rate_limit_one(examples_dag_bag):
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_api_connexion",
            "init_fab",
            "init_flash_views",
            "init_jinja_globals",
            "init_plugins",
            "init_airflow_session_interface",
        ]
    )
    def factory():
        with conf_vars({("fab", "auth_rate_limited"): "True", ("fab", "auth_rate_limit"): "1 per 20 second"}):
            return create_app(testing=True)

    app = factory()
    app.config["WTF_CSRF_ENABLED"] = False
    return app


def test_rate_limit_one(app_with_rate_limit_one):
    client_with_login(
        app_with_rate_limit_one, expected_response_code=302, username="test_admin", password="test_admin"
    )
    client_with_login(
        app_with_rate_limit_one, expected_response_code=429, username="test_admin", password="test_admin"
    )
    client_with_login(
        app_with_rate_limit_one, expected_response_code=429, username="test_admin", password="test_admin"
    )


def test_rate_limit_disabled(app):
    client_with_login(app, expected_response_code=302, username="test_admin", password="test_admin")
    client_with_login(app, expected_response_code=302, username="test_admin", password="test_admin")
    client_with_login(app, expected_response_code=302, username="test_admin", password="test_admin")
