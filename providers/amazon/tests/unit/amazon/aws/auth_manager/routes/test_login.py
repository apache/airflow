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

from unittest.mock import Mock, patch

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("AWS auth manager is only compatible with Airflow >= 3.0.0", allow_module_level=True)

from fastapi.testclient import TestClient

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX, create_app


from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.mock_plugins import mock_plugin_manager

# onelogin is optional dependency from apache-airflow-providers-amazon[python3-saml]
# we want to skip it for the lowest dependency checks as it does not install extra dependencies
# https://github.com/apache/airflow/pull/50449#issuecomment-2897572327
OneLogin_Saml2_IdPMetadataParser = pytest.importorskip(
    "onelogin.saml2.idp_metadata_parser"
).OneLogin_Saml2_IdPMetadataParser

# Imported after the importorskip above: this module raises ImportError when python3-saml
# is absent, and the lowest-dependency check deliberately runs without it.
from airflow.providers.amazon.aws.auth_manager.routes.login import COOKIE_NAME_LOGIN_STATE

SAML_METADATA_URL = "/saml/metadata"
SAML_METADATA_PARSED = {
    "idp": {
        "entityId": "https://portal.sso.us-east-1.amazonaws.com/saml/assertion/<assertion>",
        "singleSignOnService": {
            "url": "https://portal.sso.us-east-1.amazonaws.com/saml/assertion/<assertion>",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
        },
        "singleLogoutService": {
            "url": "https://portal.sso.us-east-1.amazonaws.com/saml/logout/<assertion>",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
        },
        "x509cert": "<cert>",
    },
    "security": {"authnRequestsSigned": False},
    "sp": {"NameIDFormat": "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"},
}


EXPECTED_REQUEST_ID = "ONELOGIN_authn_request_id"


@pytest.fixture
def test_client():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
            ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
        }
    ):
        with (
            patch.object(OneLogin_Saml2_IdPMetadataParser, "parse_remote") as mock_parse_remote,
            patch(
                "airflow.providers.amazon.aws.auth_manager.avp.facade.AwsAuthManagerAmazonVerifiedPermissionsFacade.is_policy_store_schema_up_to_date"
            ) as mock_is_policy_store_schema_up_to_date,
        ):
            mock_is_policy_store_schema_up_to_date.return_value = True
            mock_parse_remote.return_value = SAML_METADATA_PARSED
            yield TestClient(create_app())


def get_login_callback_response(
    relay_state: str,
    *,
    base_url: str = "http://testserver",
    login_state: str | None = None,
    return_auth_mock: bool = False,
):
    """Post a SAML response to the callback.

    ``login_state`` is the value of the browser's login-state cookie. It defaults to a
    state matching ``relay_state``, i.e. a browser that really did start this login.
    Pass ``None`` explicitly via ``login_state=""`` to simulate a browser that did not.
    """
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
            ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
            ("api", "ssl_cert"): "",
        }
    ):
        with (
            patch.object(OneLogin_Saml2_IdPMetadataParser, "parse_remote") as mock_parse_remote,
            patch(
                "airflow.providers.amazon.aws.auth_manager.routes.login._init_saml_auth"
            ) as mock_init_saml_auth,
            patch(
                "airflow.providers.amazon.aws.auth_manager.avp.facade.AwsAuthManagerAmazonVerifiedPermissionsFacade.is_policy_store_schema_up_to_date"
            ) as mock_is_policy_store_schema_up_to_date,
        ):
            mock_is_policy_store_schema_up_to_date.return_value = True
            mock_parse_remote.return_value = SAML_METADATA_PARSED

            auth = Mock()
            auth.is_authenticated.return_value = True
            auth.get_nameid.return_value = "user_id"
            auth.get_attributes.return_value = {
                "id": ["1"],
                "groups": ["group_1", "group_2"],
                "email": ["email"],
            }
            mock_init_saml_auth.return_value = auth
            client = TestClient(create_app(), base_url=base_url)
            state = f"{EXPECTED_REQUEST_ID}:{relay_state}" if login_state is None else login_state
            if state:
                client.cookies.set(COOKIE_NAME_LOGIN_STATE, state)
            response = client.post(
                AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback",
                follow_redirects=False,
                data={"RelayState": relay_state},
            )
            if return_auth_mock:
                return response, auth
            return response


@mock_plugin_manager(plugins=[])
class TestLoginRouter:
    @pytest.mark.parametrize(
        "url",
        ["/login", "/login/token"],
    )
    def test_login(self, test_client, url):
        response = test_client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + url, follow_redirects=False)
        assert response.status_code == 307
        assert "location" in response.headers
        assert response.headers["location"].startswith(
            "https://portal.sso.us-east-1.amazonaws.com/saml/assertion/"
        )

    def test_login_callback_successful_with_relay_state_redirect(self):
        response = get_login_callback_response("login-redirect")
        assert response.status_code == 303
        assert "location" in response.headers
        assert "_token" in response.cookies
        assert response.headers["location"].startswith("http://localhost:8080/")

    def test_login_callback_sets_secure_cookie_behind_tls_proxy(self):
        response = get_login_callback_response("login-redirect", base_url="https://testserver")

        assert "Secure" in response.headers["set-cookie"]

    def test_login_callback_successful_with_relay_state_token(self):
        response = get_login_callback_response("login-token")
        assert response.status_code == 200
        assert "access_token" in response.json()

    def test_login_callback_with_invalid_relay_state(self):
        response = get_login_callback_response("dummy")
        assert response.status_code == 401

    # ------------------------------------------------------------------
    # Binding the SAML response to the browser that started the login
    # ------------------------------------------------------------------

    def test_login_sets_a_login_state_cookie(self, test_client):
        """The AuthnRequest id must be remembered so the response can be tied to it."""
        response = test_client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login", follow_redirects=False)
        assert COOKIE_NAME_LOGIN_STATE in response.cookies
        set_cookie = response.headers["set-cookie"]
        assert "HttpOnly" in set_cookie
        assert "Lax" in set_cookie

    def test_login_callback_rejects_a_browser_that_started_no_login(self):
        """The core of the attack: an assertion replayed into an uninvolved browser.

        The assertion here is valid and authenticates successfully -- the mock returns an
        authenticated user. What must stop it is the absence of any login this browser
        began, so the caller is not logged in as the assertion's subject.
        """
        response = get_login_callback_response("login-redirect", login_state="")
        assert response.status_code == 401
        assert "_token" not in response.cookies

    def test_login_callback_enforces_in_response_to(self):
        """python3-saml only validates InResponseTo when it is given the request id."""
        response, auth = get_login_callback_response("login-redirect", return_auth_mock=True)
        assert response.status_code == 303
        auth.process_response.assert_called_once_with(request_id=EXPECTED_REQUEST_ID)

    def test_login_callback_rejects_a_relay_state_the_browser_did_not_ask_for(self):
        """The return mode is fixed when the flow starts, not chosen by the response."""
        response = get_login_callback_response(
            "login-token", login_state=f"{EXPECTED_REQUEST_ID}:login-redirect"
        )
        assert response.status_code == 401

    def test_login_callback_clears_the_login_state_on_the_token_path(self):
        """A consumed request id must not stay usable after a token login either.

        Left set, the same assertion could be reposted within the cookie's lifetime to
        mint further API tokens.
        """
        response = get_login_callback_response("login-token")
        assert response.status_code == 200
        cleared = [
            c for c in response.headers.get_list("set-cookie") if c.startswith(f"{COOKIE_NAME_LOGIN_STATE}=")
        ]
        assert cleared, "login state cookie was not cleared on the token path"

    def test_login_callback_clears_the_login_state_on_success(self):
        """A consumed request id must not stay usable for a second response."""
        response = get_login_callback_response("login-redirect")
        assert response.status_code == 303
        cookies = response.headers.get_list("set-cookie")
        cleared = [c for c in cookies if c.startswith(f"{COOKIE_NAME_LOGIN_STATE}=")]
        assert cleared, "login state cookie was not cleared"
        assert "Max-Age=0" in cleared[0] or '""' in cleared[0] or "expires=" in cleared[0].lower()

    def test_login_callback_unsuccessful(self):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
                ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
            }
        ):
            with (
                patch.object(OneLogin_Saml2_IdPMetadataParser, "parse_remote") as mock_parse_remote,
                patch(
                    "airflow.providers.amazon.aws.auth_manager.routes.login._init_saml_auth"
                ) as mock_init_saml_auth,
                patch(
                    "airflow.providers.amazon.aws.auth_manager.avp.facade.AwsAuthManagerAmazonVerifiedPermissionsFacade.is_policy_store_schema_up_to_date"
                ) as mock_is_policy_store_schema_up_to_date,
            ):
                mock_is_policy_store_schema_up_to_date.return_value = True
                mock_parse_remote.return_value = SAML_METADATA_PARSED

                auth = Mock()
                auth.is_authenticated.return_value = False
                mock_init_saml_auth.return_value = auth
                client = TestClient(create_app())
                response = client.post(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback")
                assert response.status_code == 500
