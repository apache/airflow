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

import base64
import json
import time
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
from airflow.providers.amazon.aws.auth_manager.routes.login import (  # noqa: E402
    COOKIE_NAME_LOGIN_STATE,
    LOGIN_MODE_REDIRECT,
    LOGIN_MODE_TOKEN,
    _sign_login_state,
)

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
TEST_NONCE = "nonce-for-this-login"
TEST_SECRET_KEY = "login-state-signing-key"

RELAY_REDIRECT = f"{LOGIN_MODE_REDIRECT}:{TEST_NONCE}"
RELAY_TOKEN = f"{LOGIN_MODE_TOKEN}:{TEST_NONCE}"

BASE_CONF = {
    ("core", "auth_manager"): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
    ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
    ("api", "ssl_cert"): "",
    ("api", "secret_key"): TEST_SECRET_KEY,
    ("api", "base_url"): "http://localhost:8080/",
}


def make_login_state(*entries: tuple[str, str, str], expires_in: float = 600) -> str:
    """
    Build the signed login-state cookie a browser would hold for ``entries``.

    Each entry is ``(nonce, request_id, mode)``. Must be called inside the ``conf_vars``
    block that sets the signing key, so the signature matches what the route computes.
    """
    payload = base64.urlsafe_b64encode(
        json.dumps(
            [
                {
                    "nonce": nonce,
                    "request_id": request_id,
                    "mode": mode,
                    "exp": time.time() + expires_in,
                }
                for nonce, request_id, mode in entries
            ],
            separators=(",", ":"),
        ).encode()
    ).decode()
    return f"{payload}.{_sign_login_state(payload)}"


def read_login_state_cookie(response) -> str | None:
    """Return the login-state cookie value the response sets, if it sets one."""
    for header in response.headers.get_list("set-cookie"):
        if header.startswith(f"{COOKIE_NAME_LOGIN_STATE}="):
            return header.split("=", 1)[1].split(";", 1)[0]
    return None


@pytest.fixture(autouse=True)
def no_plugins():
    """
    Keep the plugin manager out of these tests.

    Applied as a fixture rather than as ``@mock_plugin_manager(...)`` on the class:
    ``mock_plugin_manager`` is a ``contextmanager``, and a ``ContextDecorator`` used on a class
    replaces it with a function, which pytest then does not collect at all.
    """
    with mock_plugin_manager(plugins=[]):
        yield


@pytest.fixture
def test_client():
    with conf_vars(BASE_CONF):
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
    pending: list[tuple[str, str, str]] | None = None,
    raw_login_state: str | None = None,
    expires_in: float = 600,
    in_response_to: str | None = None,
    is_authenticated: bool = True,
    extra_conf: dict | None = None,
    return_auth_mock: bool = False,
):
    """
    Post a SAML response to the callback.

    ``pending`` is what this browser has started, as ``(nonce, request_id, mode)`` tuples; it
    defaults to a single entry answering ``relay_state``, i.e. a browser that really did start
    this login. ``raw_login_state`` sets the cookie verbatim instead -- pass ``""`` for a
    browser that started nothing.
    """
    with conf_vars({**BASE_CONF, **(extra_conf or {})}):
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
            auth.is_authenticated.return_value = is_authenticated
            auth.get_nameid.return_value = "user_id"
            auth.get_last_response_in_response_to.return_value = in_response_to
            auth.get_attributes.return_value = {
                "id": ["1"],
                "groups": ["group_1", "group_2"],
                "email": ["email"],
            }
            mock_init_saml_auth.return_value = auth

            if raw_login_state is None:
                mode, _, nonce = relay_state.partition(":")
                entries = (
                    pending
                    if pending is not None
                    else [(nonce or TEST_NONCE, EXPECTED_REQUEST_ID, mode or LOGIN_MODE_REDIRECT)]
                )
                cookie = make_login_state(*entries, expires_in=expires_in)
            else:
                cookie = raw_login_state

            client = TestClient(create_app(), base_url=base_url)
            if cookie:
                client.cookies.set(COOKIE_NAME_LOGIN_STATE, cookie)
            response = client.post(
                AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback",
                follow_redirects=False,
                data={"RelayState": relay_state},
            )
            if return_auth_mock:
                return response, auth
            return response


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
        response = get_login_callback_response(RELAY_REDIRECT)
        assert response.status_code == 303
        assert "location" in response.headers
        assert "_token" in response.cookies
        assert response.headers["location"].startswith("http://localhost:8080/")

    def test_login_callback_sets_secure_cookie_behind_tls_proxy(self):
        response = get_login_callback_response(RELAY_REDIRECT, base_url="https://testserver")

        assert "Secure" in response.headers["set-cookie"]

    def test_login_callback_successful_with_relay_state_token(self):
        response = get_login_callback_response(RELAY_TOKEN)
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
        set_cookie = response.headers["set-cookie"].lower()
        assert "httponly" in set_cookie
        assert "samesite=lax" in set_cookie

    def test_login_sends_the_nonce_to_the_idp(self, test_client):
        """The nonce has to survive the round trip, so it goes out in RelayState."""
        response = test_client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login", follow_redirects=False)
        assert f"RelayState={LOGIN_MODE_REDIRECT}" in response.headers["location"]

    def test_login_callback_rejects_a_browser_that_started_no_login(self):
        """
        The core case: an assertion posted to a browser that never started a login.

        The assertion here is valid and authenticates successfully -- the mock returns an
        authenticated user. What must stop it is the absence of any login this browser
        began, so the caller is not logged in as the assertion's subject.
        """
        response = get_login_callback_response(RELAY_REDIRECT, raw_login_state="")
        assert response.status_code == 401
        assert "_token" not in response.cookies

    def test_login_callback_enforces_in_response_to(self):
        """python3-saml only validates InResponseTo when it is given the request id."""
        response, auth = get_login_callback_response(RELAY_REDIRECT, return_auth_mock=True)
        assert response.status_code == 303
        auth.process_response.assert_called_once_with(request_id=EXPECTED_REQUEST_ID)

    def test_login_callback_rejects_a_relay_state_the_browser_did_not_ask_for(self):
        """The return mode is fixed when the flow starts, not chosen by the response."""
        response = get_login_callback_response(
            RELAY_TOKEN, pending=[(TEST_NONCE, EXPECTED_REQUEST_ID, LOGIN_MODE_REDIRECT)]
        )
        assert response.status_code == 401

    def test_login_callback_rejects_a_nonce_this_browser_never_had(self):
        """The nonce names one of this browser's pending logins; an unknown one matches none."""
        response = get_login_callback_response(
            f"{LOGIN_MODE_REDIRECT}:some-other-nonce",
            pending=[(TEST_NONCE, EXPECTED_REQUEST_ID, LOGIN_MODE_REDIRECT)],
        )
        assert response.status_code == 401

    @pytest.mark.parametrize(
        "mangle",
        [
            pytest.param(lambda state: state.split(".")[0], id="signature_stripped"),
            pytest.param(lambda state: f"{state.split('.')[0]}.{'0' * 64}", id="signature_wrong"),
            pytest.param(lambda state: f"x{state}", id="payload_altered"),
            pytest.param(lambda state: "not-a-cookie", id="not_a_cookie"),
        ],
    )
    def test_login_callback_rejects_a_login_state_it_did_not_sign(self, mangle):
        """
        Forging an entry would defeat the binding outright.

        Anyone who could write this cookie could name an arbitrary AuthnRequest id, so an
        unrelated response would match it. The signature is what makes the entry
        something only this deployment can produce.
        """
        with conf_vars(BASE_CONF):
            valid = make_login_state((TEST_NONCE, EXPECTED_REQUEST_ID, LOGIN_MODE_REDIRECT))
        response = get_login_callback_response(RELAY_REDIRECT, raw_login_state=mangle(valid))
        assert response.status_code == 401

    def test_login_callback_rejects_an_expired_login_state(self):
        """A login left unfinished stops being answerable once its deadline passes."""
        response = get_login_callback_response(RELAY_REDIRECT, expires_in=-1)
        assert response.status_code == 401

    def test_login_callback_clears_the_login_state_on_the_token_path(self):
        """
        A consumed request id must not stay usable after a token login either.

        Left set, the same assertion could be reposted within the cookie's lifetime to
        mint further API tokens.
        """
        response = get_login_callback_response(RELAY_TOKEN)
        assert response.status_code == 200
        assert read_login_state_cookie(response) == '""'

    def test_login_callback_clears_the_login_state_on_success(self):
        """A consumed request id must not stay usable for a second response."""
        response = get_login_callback_response(RELAY_REDIRECT)
        assert response.status_code == 303
        assert read_login_state_cookie(response) == '""'

    def test_login_callback_rejects_the_same_response_twice(self):
        """
        Single use, end to end: the cookie the first response hands back no longer answers it.

        This is the replay the clearing above exists to stop, exercised through the route
        rather than by inspecting the header.
        """
        first = get_login_callback_response(RELAY_REDIRECT)
        assert first.status_code == 303
        replayed = get_login_callback_response(
            RELAY_REDIRECT, raw_login_state=read_login_state_cookie(first).strip('"')
        )
        assert replayed.status_code == 401

    def test_login_callback_leaves_logins_started_in_other_tabs_alone(self):
        """
        Two tabs, two pending logins. Finishing one must not strand the other.

        A single-slot cookie made the second tab overwrite the first, so whichever login
        finished second failed for a user who had done nothing wrong.
        """
        other = ("nonce-from-the-other-tab", "other_request_id", LOGIN_MODE_REDIRECT)
        response = get_login_callback_response(
            RELAY_REDIRECT,
            pending=[other, (TEST_NONCE, EXPECTED_REQUEST_ID, LOGIN_MODE_REDIRECT)],
        )
        assert response.status_code == 303

        remaining = read_login_state_cookie(response)
        assert remaining is not None
        assert remaining != '""'
        entries = json.loads(base64.urlsafe_b64decode(remaining.split(".")[0]))
        assert [entry["nonce"] for entry in entries] == [other[0]]

    # ------------------------------------------------------------------
    # IdP-initiated (unsolicited) logins, which are opt-in
    # ------------------------------------------------------------------

    def test_login_callback_rejects_an_unsolicited_response_by_default(self):
        """The access portal flow is off unless a deployment asks for it."""
        response = get_login_callback_response("", raw_login_state="")
        assert response.status_code == 401

    def test_login_callback_accepts_an_unsolicited_response_when_opted_in(self):
        """With the option on, an assertion nobody asked for logs the caller in."""
        response = get_login_callback_response(
            "",
            raw_login_state="",
            extra_conf={("aws_auth_manager", "allow_idp_initiated_login"): "True"},
        )
        assert response.status_code == 303
        assert "_token" in response.cookies

    def test_login_callback_does_not_enforce_in_response_to_when_unsolicited(self):
        """There is no request to bind to, so no request id is given to python3-saml."""
        response, auth = get_login_callback_response(
            "",
            raw_login_state="",
            extra_conf={("aws_auth_manager", "allow_idp_initiated_login"): "True"},
            return_auth_mock=True,
        )
        assert response.status_code == 303
        auth.process_response.assert_called_once_with(request_id=None)

    def test_login_callback_refuses_an_unsolicited_response_carrying_in_response_to(self):
        """
        An unsolicited assertion answers no request, so one naming a request is a replay.

        python3-saml skips the comparison entirely when it is handed no request id, so
        opting in to the access portal flow would otherwise also accept a solicited
        assertion reposted with the browser's cookie removed.
        """
        response = get_login_callback_response(
            "",
            raw_login_state="",
            in_response_to=EXPECTED_REQUEST_ID,
            extra_conf={("aws_auth_manager", "allow_idp_initiated_login"): "True"},
        )
        assert response.status_code == 401

    def test_login_callback_still_binds_a_solicited_response_when_unsolicited_allowed(self):
        """Opting in relaxes the no-state case only; a login that started here is still bound."""
        response, auth = get_login_callback_response(
            RELAY_REDIRECT,
            extra_conf={("aws_auth_manager", "allow_idp_initiated_login"): "True"},
            return_auth_mock=True,
        )
        assert response.status_code == 303
        auth.process_response.assert_called_once_with(request_id=EXPECTED_REQUEST_ID)

    def test_login_callback_unsuccessful(self):
        response = get_login_callback_response(RELAY_REDIRECT, is_authenticated=False)
        assert response.status_code == 500
