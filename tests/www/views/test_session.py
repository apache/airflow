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

from unittest import mock

import pytest

from airflow.exceptions import AirflowConfigException
from airflow.www import app

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.decorators import dont_initialize_flask_app_submodules

pytestmark = pytest.mark.db_test


def get_session_cookie(client):
    return next((cookie for cookie in client.cookie_jar if cookie.name == "session"), None)


def test_session_cookie_created_on_login(user_client):
    assert get_session_cookie(user_client) is not None


def test_session_inaccessible_after_logout(user_client):
    session_cookie = get_session_cookie(user_client)
    assert session_cookie is not None

    resp = user_client.post("/logout/")
    assert resp.status_code == 302

    # Try to access /home with the session cookie from earlier
    user_client.set_cookie("session", session_cookie.value)
    user_client.get("/home/")
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


def test_session_id_rotates(app, user_client):
    old_session_cookie = get_session_cookie(user_client)
    assert old_session_cookie is not None

    resp = user_client.post("/logout/")
    assert resp.status_code == 302

    patch_path = "airflow.providers.fab.auth_manager.security_manager.override.check_password_hash"
    with mock.patch(patch_path) as check_password_hash:
        check_password_hash.return_value = True
        resp = user_client.post("/login/", data={"username": "test_user", "password": "test_user"})
    assert resp.status_code == 302

    new_session_cookie = get_session_cookie(user_client)
    assert new_session_cookie is not None
    assert old_session_cookie.value != new_session_cookie.value


def test_check_active_user(app, user_client):
    user = app.appbuilder.sm.find_user(username="test_user")
    user.active = False
    resp = user_client.get("/home")
    assert resp.status_code == 302
    assert "/login/?next=http%3A%2F%2Flocalhost%2Fhome" in resp.headers.get("Location")


def test_check_deactivated_user_redirected_to_login(app, user_client):
    with app.test_request_context():
        user = app.appbuilder.sm.find_user(username="test_user")
        user.active = False
        resp = user_client.get("/home", follow_redirects=True)
        assert resp.status_code == 200
        assert "/login" in resp.request.url
