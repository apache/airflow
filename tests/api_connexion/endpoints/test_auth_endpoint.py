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
from unittest import mock

import pytest
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_REMOTE_USER

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

    def auth_type(self, auth):
        self.app.config['AUTH_TYPE'] = auth


def auth_type(self, auth):
    self.app.config['AUTH_TYPE'] = auth


class TestDBLoginEndpoint(TestLoginEndpoint):
    def test_user_can_login(self):
        self.auth_type(AUTH_DB)
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.json['username'] == 'test'
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)

    def test_logged_in_user_cant_relogin(self):
        self.auth_type(AUTH_DB)
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.json['username'] == 'test'
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 401
        assert response.json['detail'] == "Client already authenticated"

    def test_incorrect_username_raises(self):
        self.auth_type(AUTH_DB)
        payload = {"username": "tests", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 404
        assert response.json['detail'] == 'Invalid login'

    def test_post_body_conforms(self):
        self.auth_type(AUTH_DB)
        payload = {"username": "tests"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == "{'password': ['Missing data for required field.']}"

    def test_auth_type_must_be_db(self):
        self.auth_type(AUTH_OAUTH)
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == 'Authentication type do not match'


class TestLDAPLoginEndpoint(TestLoginEndpoint):
    def test_user_can_login(self):
        self.auth_type(AUTH_LDAP)
        self.app.appbuilder.sm.auth_user_ldap = mock.Mock()
        user = self.app.appbuilder.sm.find_user(username='test')
        self.app.appbuilder.sm.auth_user_ldap.return_value = user
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)

        assert response.json['username'] == 'test'
        cookie = next(
            (cookie for cookie in self.client.cookie_jar if cookie.name == "access_token_cookie"), None
        )
        assert cookie is not None
        assert isinstance(cookie.value, str)

    def test_logged_in_user_cant_relogin(self):
        self.auth_type(AUTH_LDAP)
        self.app.appbuilder.sm.auth_user_ldap = mock.Mock()
        user = self.app.appbuilder.sm.find_user(username='test')
        self.app.appbuilder.sm.auth_user_ldap.return_value = user
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.json['username'] == 'test'
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 401
        assert response.json['detail'] == "Client already authenticated"

    def test_incorrect_username_raises(self):
        self.auth_type(AUTH_LDAP)
        self.app.appbuilder.sm.auth_user_ldap = mock.Mock()
        self.app.appbuilder.sm.auth_user_ldap.return_value = None
        payload = {"username": "tests", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 404
        assert response.json['detail'] == 'Invalid login'

    def test_post_body_conforms(self):
        self.auth_type(AUTH_LDAP)
        payload = {"username": "tests"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == "{'password': ['Missing data for required field.']}"

    def test_auth_type_must_be_ldap(self):
        self.auth_type(AUTH_OAUTH)
        payload = {"username": "test", "password": "test"}
        response = self.client.post('api/v1/auth-dblogin', json=payload)
        assert response.status_code == 400
        assert response.json['detail'] == 'Authentication type do not match'


class TestOauthAuthorizationURLEndpoint(TestLoginEndpoint):
    @mock.patch("airflow.api_connexion.endpoints.auth_endpoint.jwt.encode")
    def test_can_generate_authorization_url(self, mock_jwt_encode):
        self.auth_type(AUTH_OAUTH)
        mock_jwt_encode.return_value = "state"
        mock_google_auth_provider = mock.Mock()
        self.app.appbuilder.sm.oauth_remotes = {
            "google": mock_google_auth_provider,
        }
        mock_auth = mock.MagicMock(return_value={"state": "state", "url": "authurl"})
        mock_google_auth_provider.create_authorization_url = mock_auth
        redirect_url = "http://localhost:8080"
        self.client.get(f'api/v1/auth-oauth/google?register=True&redirect_url={redirect_url}')
        mock_auth.assert_called_once_with(redirect_uri=redirect_url, state="state")

    @mock.patch("airflow.api_connexion.endpoints.auth_endpoint.jwt.encode")
    def test_can_generate_authorization_url_for_twitter(self, mock_jwt_encode):
        self.auth_type(AUTH_OAUTH)
        mock_jwt_encode.return_value = "state"
        mock_twitter_auth_provider = mock.Mock()
        self.app.appbuilder.sm.oauth_remotes = {
            "twitter": mock_twitter_auth_provider,
        }
        mock_auth = mock.MagicMock(return_value={"state": "state", "url": "authurl"})
        mock_twitter_auth_provider.create_authorization_url = mock_auth
        redirect_url = "http://localhost:8080"
        self.client.get(f'api/v1/auth-oauth/twitter?register=True&redirect_url={redirect_url}')
        mock_auth.assert_called_once_with(redirect_uri=redirect_url + "&state=state")

    def test_incorrect_auth_type_raises(self):
        self.auth_type(AUTH_DB)
        redirect_url = "http://localhost:8080"
        resp = self.client.get(f'api/v1/auth-oauth/google?register=True&redirect_url={redirect_url}')
        assert resp.status_code == 400
        assert resp.json['detail'] == "Authentication type do not match"


class TestAuthorizeOauth(TestLoginEndpoint):
    def test_user_refused_sign_in_request(self):
        self.auth_type(AUTH_OAUTH)
        mock_twitter_auth_provider = mock.Mock()
        self.app.appbuilder.sm.oauth_remotes = {
            "twitter": mock_twitter_auth_provider,
        }
        mock_twitter_auth_provider.authorize_access_token.return_value = None
        response = self.client.get('api/v1/oauth-authorized/twitter?state=state')
        assert response.status_code == 400
        assert response.json['detail'] == "You denied the request to sign in"

    def test_wrong_state_signature_raises(self):
        self.auth_type(AUTH_OAUTH)
        mock_twitter_auth_provider = mock.Mock()
        self.app.appbuilder.sm.oauth_remotes = {
            "twitter": mock_twitter_auth_provider,
        }
        mock_twitter_auth_provider.authorize_access_token.return_value = mock.MagicMock()
        response = self.client.get('api/v1/oauth-authorized/twitter?state=state')
        assert response.status_code == 400
        assert response.json['detail'] == "State signature is not valid!"

    @mock.patch("airflow.api_connexion.endpoints.auth_endpoint.jwt.decode")
    def test_successful_authorization(self, mock_jwt_decode):
        self.auth_type(AUTH_OAUTH)
        mock_jwt_decode.return_value = {'some': 'payload'}
        mock_twitter_auth_provider = mock.Mock()
        mock_oauth_session = mock.MagicMock()
        mock_user_info = mock.MagicMock()
        mock_user_oauth = mock.MagicMock()
        self.app.appbuilder.sm.set_oauth_session = mock_oauth_session
        self.app.appbuilder.sm.oauth_user_info = mock_user_info
        self.app.appbuilder.sm.auth_user_oauth = mock_user_oauth
        user = self.app.appbuilder.sm.find_user(username='test')
        self.app.appbuilder.sm.auth_user_oauth.return_value = user
        self.app.appbuilder.sm.oauth_remotes = {
            "twitter": mock_twitter_auth_provider,
        }
        mock_authorized = mock.MagicMock()
        mock_twitter_auth_provider.authorize_access_token.return_value = mock_authorized
        self.client.get('api/v1/oauth-authorized/twitter?state=state')
        mock_oauth_session.assert_called_once_with('twitter', mock_authorized)
        mock_user_info.assert_called_once_with('twitter', mock_authorized)
        mock_user_oauth.assert_called_once_with(mock_user_info.return_value)


class TestRemoteUserLoginEndpoint(TestLoginEndpoint):
    def test_remote_user_can_login(self):
        self.auth_type(AUTH_REMOTE_USER)
        response = self.client.get('api/v1/auth-remoteuser', environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json['username'] == 'test'

    def test_remote_user_cant_relogin(self):
        self.auth_type(AUTH_REMOTE_USER)
        response = self.client.get('api/v1/auth-remoteuser', environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        response = self.client.get('api/v1/auth-remoteuser', environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 401
        assert response.json['detail'] == "Client already authenticated"

    def test_incorrect_username_raises(self):
        self.auth_type(AUTH_REMOTE_USER)
        response = self.client.get('api/v1/auth-remoteuser', environ_overrides={"REMOTE_USER": "tes"})
        assert response.status_code == 404
        assert response.json['detail'] == 'Invalid login'

    def test_incorrect_auth_type_raises(self):
        self.auth_type(AUTH_DB)
        resp = self.client.get('api/v1/auth-remoteuser', environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 400
        assert resp.json['detail'] == "Authentication type do not match"
