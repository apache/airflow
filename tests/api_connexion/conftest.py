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

from airflow.www import app
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


@pytest.fixture(scope="session")
def minimal_app_for_api():
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_appbuilder",
            "init_api_auth",
            "init_api_connexion",
            "init_api_error_handlers",
            "init_airflow_session_interface",
            "init_appbuilder_views",
        ]
    )
    def factory():
        with conf_vars({("api", "auth_backends"): "tests.test_utils.remote_user_api_auth_backend"}):
            _app = app.create_app(testing=True, config={"WTF_CSRF_ENABLED": False})  # type:ignore
            _app.config["AUTH_ROLE_PUBLIC"] = None
            return _app

    return factory()


@pytest.fixture
def session():
    from airflow.utils.session import create_session

    with create_session() as session:
        yield session


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    DagBag(include_examples=True, read_dags_from_db=False).sync_to_db()
    return DagBag(include_examples=True, read_dags_from_db=True)


@pytest.fixture
def set_auto_role_public(request):
    app = request.getfixturevalue("minimal_app_for_api")
    auto_role_public = app.config["AUTH_ROLE_PUBLIC"]
    app.config["AUTH_ROLE_PUBLIC"] = request.param

    yield

    app.config["AUTH_ROLE_PUBLIC"] = auto_role_public
