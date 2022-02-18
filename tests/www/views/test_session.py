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

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.www import app
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


def test_session_cookie_created_on_login(user_client):
    assert any(cookie.name == 'session' for cookie in user_client.cookie_jar)


def test_session_inaccessible_after_logout(user_client):
    session_cookie = next((cookie for cookie in user_client.cookie_jar if cookie.name == 'session'), None)
    assert session_cookie is not None

    resp = user_client.get('/logout/')
    assert resp.status_code == 302

    # Try to access /home with the session cookie from earlier
    user_client.set_cookie('session', session_cookie.value)
    user_client.get('/home/')
    assert resp.status_code == 302


def test_invalid_session_backend_option():
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
    def poorly_configured_app_factory():
        with conf_vars({("webserver", "session_backend"): "invalid_value_for_session_backend"}):
            return app.create_app(testing=True)

    expected_exc_regex = (
        "^Unrecognized session backend specified in web_server_session_backend: "
        r"'invalid_value_for_session_backend'\. Please set this to .+\.$"
    )
    with pytest.raises(AirflowConfigException, match=expected_exc_regex):
        poorly_configured_app_factory()
