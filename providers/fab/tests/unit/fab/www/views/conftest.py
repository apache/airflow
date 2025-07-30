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

import jinja2
import pytest

from airflow import settings
from airflow.providers.fab.www.app import create_app

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import parse_and_sync_to_db
from unit.fab.auth_manager.api_endpoints.api_connexion_utils import delete_user
from unit.fab.decorators import dont_initialize_flask_app_submodules
from unit.fab.utils import client_with_login


@pytest.fixture(autouse=True, scope="module")
def session():
    settings.configure_orm()
    return settings.Session


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag(session):
    dag_bag = parse_and_sync_to_db(os.devnull, include_examples=True)
    session.commit()
    return dag_bag


@pytest.fixture(scope="module")
def app(examples_dag_bag):
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_api_connexion",
            "init_appbuilder",
            "init_appbuilder_links",
            "init_appbuilder_views",
            "init_flash_views",
            "init_jinja_globals",
            "init_plugins",
            "init_airflow_session_interface",
        ]
    )
    def factory():
        with conf_vars({("fab", "auth_rate_limited"): "False"}):
            return create_app(enable_plugins=False)

    app = factory()
    app.config["WTF_CSRF_ENABLED"] = False
    app.dag_bag = examples_dag_bag
    app.jinja_env.undefined = jinja2.StrictUndefined

    security_manager = app.appbuilder.sm

    test_users = [
        {
            "username": "test_admin",
            "first_name": "test_admin_first_name",
            "last_name": "test_admin_last_name",
            "email": "test_admin@fab.org",
            "role": security_manager.find_role("Admin"),
            "password": "test_admin_password",
        },
        {
            "username": "test_user",
            "first_name": "test_user_first_name",
            "last_name": "test_user_last_name",
            "email": "test_user@fab.org",
            "role": security_manager.find_role("User"),
            "password": "test_user_password",
        },
        {
            "username": "test_viewer",
            "first_name": "test_viewer_first_name",
            "last_name": "test_viewer_last_name",
            "email": "test_viewer@fab.org",
            "role": security_manager.find_role("Viewer"),
            "password": "test_viewer_password",
        },
    ]

    for user_dict in test_users:
        if not security_manager.find_user(username=user_dict["username"]):
            security_manager.add_user(**user_dict)

    yield app

    for user_dict in test_users:
        delete_user(app, user_dict["username"])


@pytest.fixture
def user_client(app):
    return client_with_login(app, username="test_user", password="test_user")


@pytest.fixture
def viewer_client(app):
    return client_with_login(app, username="test_viewer", password="test_viewer")


@pytest.fixture
def admin_client(app):
    return client_with_login(app, username="test_admin", password="test_admin")
