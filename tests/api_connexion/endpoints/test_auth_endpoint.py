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

from base64 import b64encode

import pytest

from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.config import conf_vars

TEST_USERNAME = "test"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
    )

    yield app

    delete_user(app, username="test")  # type: ignore


class TestBase:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore


class TestLoginEndpoint(TestBase):
    def test_user_can_login_with_basic_auth(self):
        token = "Basic " + b64encode(b"test:test").decode()
        self.client.post("api/v1/login", headers={"Authorization": token})
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)

    def test_no_authorization_header_raises(self):
        response = self.client.post("api/v1/login")
        assert response.status_code == 401

    def test_malformed_authorization_header_fails(self):
        token = "Basic " + b64encode(b"test").decode()
        response = self.client.post("api/v1/login", headers={"Authorization": token})
        assert response.status_code == 401

    def test_wrong_password_or_username_fails(self):
        token = "Basic " + b64encode(b"test:tes").decode()
        response = self.client.post("api/v1/login", headers={"Authorization": token})
        assert response.status_code == 401

    def test_remote_auth_user_can_get_token(self):
        self.client.post("api/v1/login", environ_overrides={'REMOTE_USER': "test"})
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)

    @conf_vars({("api", "disable_db_login"): "True"})
    def test_user_cannot_login_through_db_when_db_login_is_disabled(self):
        token = "Basic " + b64encode(b"test:test").decode()
        with self.app.test_client() as test_client:
            test_client.post("api/v1/login", headers={"Authorization": token})
            cookie = next(
                (cookie for cookie in test_client.cookie_jar if cookie.name == "access_token_cookie"), None
            )
        assert cookie is None

    @conf_vars({("api", "disable_db_login"): "True"})
    def test_cookie_is_set_for_remote_user_when_db_login_is_disabled(self):
        self.client.post("api/v1/login", environ_overrides={'REMOTE_USER': "test"})
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)


class TestLogout(TestBase):
    def test_logout_unsets_cookie(self):
        self.client.post("api/v1/login", environ_overrides={'REMOTE_USER': "test"})
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)
        self.client.post("api/v1/logout", environ_overrides={'REMOTE_USER': "test"})
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is None
