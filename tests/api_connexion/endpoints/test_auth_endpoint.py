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
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH

from tests.test_utils.api_connexion_utils import delete_user
from tests.test_utils.fab_utils import create_user


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(app, username="test", role_name="Test")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore


class TestLoginEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore


class TestDBLoginEndpoint(TestLoginEndpoint):
    def test_user_can_login(self):
        self.app.config['AUTH_TYPE'] = AUTH_DB
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.json['username'] == 'test'
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)

    def test_logged_in_user_cant_relogin(self):
        self.app.config['AUTH_TYPE'] = AUTH_DB
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.json['username'] == 'test'
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 401
        assert response.json['detail'] == "Client already authenticated"

    def test_incorrect_username_raises(self):
        self.app.config['AUTH_TYPE'] = AUTH_DB
        payload = {"username": "tests", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 404
        assert response.json['detail'] == 'Invalid login'

    def test_post_body_conforms(self):
        self.app.config['AUTH_TYPE'] = AUTH_DB
        payload = {"username": "tests"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == "{'password': ['Missing data for required field.']}"

    def test_auth_type_must_be_db(self):
        self.app.config['AUTH_TYPE'] = AUTH_OAUTH
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == 'Authentication type do not match'


class TestLDAPLoginEndpoint(TestLoginEndpoint):
    def test_user_can_login(self):
        self.app.config['AUTH_TYPE'] = AUTH_LDAP
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.json['username'] == 'test'
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)

    def test_logged_in_user_cant_relogin(self):
        self.app.config['AUTH_TYPE'] = AUTH_LDAP
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.json['username'] == 'test'
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 401
        assert response.json['detail'] == "Client already authenticated"

    def test_incorrect_username_raises(self):
        self.app.config['AUTH_TYPE'] = AUTH_LDAP
        payload = {"username": "tests", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 404
        assert response.json['detail'] == 'Invalid login'

    def test_post_body_conforms(self):
        self.app.config['AUTH_TYPE'] = AUTH_LDAP
        payload = {"username": "tests"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == "{'password': ['Missing data for required field.']}"

    def test_auth_type_must_be_db(self):
        self.app.config['AUTH_TYPE'] = AUTH_OAUTH
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == 'Authentication type do not match'


class TestOauthAuthorizationURLEndpoint(TestLoginEndpoint):
    def test_can_generate_authorization_url(self):
        pass

    def test_already_logged_in_user_cant_get_auth_url(self):
        pass

    def test_incorrect_auth_type_raises(self):
        pass


class TestAuthorizeOauth(TestLoginEndpoint):
    def test_user_refused_sign_in_request(self):
        pass

    def test_wrong_state_signature_raises(self):
        pass
