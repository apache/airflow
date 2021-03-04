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

import unittest
from base64 import b64encode

from airflow.www import app
from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.config import conf_vars

TEST_USERNAME = "test"


class TestLoginEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars(
            {
                ("webserver", "session_lifetime_minutes"): "1",
                ("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend",
            }
        ):
            cls.app = app.create_app(testing=True)  # type:ignore
        create_user(cls.app, username="test", role_name="Test")  # type: ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore

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
