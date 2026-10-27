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

import base64
import hashlib
import threading
import urllib.error
import urllib.request
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest

from airflowctl.ctl.utils.browser_login import build_pkce_pair, get_authorization_code
from airflowctl.exceptions import AirflowCtlBrowserLoginException


def _fake_browser(build_callback_query):
    """Return a ``webbrowser.open`` stand-in that calls back the loopback redirect like Keycloak would."""

    def open_login_page(login_url):
        params = {key: values[0] for key, values in parse_qs(urlsplit(login_url).query).items()}
        callback_url = f"{params['redirect_uri']}?{urlencode(build_callback_query(params))}"

        def call_back():
            # A stray request must not end the wait.
            redirect = urlsplit(params["redirect_uri"])
            with pytest.raises(urllib.error.HTTPError):
                urllib.request.urlopen(f"http://{redirect.netloc}/favicon.ico")
            urllib.request.urlopen(callback_url).read()

        threading.Thread(target=call_back, daemon=True).start()
        return True

    return open_login_page


def test_build_pkce_pair():
    code_verifier, code_challenge = build_pkce_pair()

    assert 43 <= len(code_verifier) <= 128
    expected = base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest()).rstrip(b"=").decode()
    assert code_challenge == expected


@patch("airflowctl.ctl.utils.browser_login.webbrowser.open", autospec=True)
def test_get_authorization_code(mock_open):
    seen_params = {}

    def build_callback_query(params):
        seen_params.update(params)
        return {"code": "the-code", "state": params["state"]}

    mock_open.side_effect = _fake_browser(build_callback_query)

    result = get_authorization_code(
        api_url="https://airflow.example.com/prefix",
        authorization_endpoint="/prefix/auth/login/cli",
        open_browser=True,
        timeout=10,
    )

    login_url = urlsplit(mock_open.call_args.args[0])
    assert (login_url.netloc, login_url.path) == ("airflow.example.com", "/prefix/auth/login/cli")
    assert seen_params["code_challenge_method"] == "S256"
    assert result.code == "the-code"
    assert result.redirect_uri == seen_params["redirect_uri"]
    assert urlsplit(result.redirect_uri).hostname == "127.0.0.1"
    challenge = base64.urlsafe_b64encode(hashlib.sha256(result.code_verifier.encode()).digest())
    assert challenge.rstrip(b"=").decode() == seen_params["code_challenge"]


@pytest.mark.parametrize(
    ("build_callback_query", "message"),
    [
        pytest.param(
            lambda params: {"code": "the-code", "state": "forged"},
            "does not match this login attempt",
            id="state-mismatch",
        ),
        pytest.param(
            lambda params: {
                "error": "access_denied",
                "error_description": "User refused",
                "state": params["state"],
            },
            "Login refused: User refused",
            id="error",
        ),
        pytest.param(
            lambda params: {"state": params["state"]},
            "contains no authorization code",
            id="no-code",
        ),
    ],
)
@patch("airflowctl.ctl.utils.browser_login.webbrowser.open", autospec=True)
def test_get_authorization_code_rejects_callback(mock_open, build_callback_query, message):
    mock_open.side_effect = _fake_browser(build_callback_query)

    with pytest.raises(AirflowCtlBrowserLoginException, match=message):
        get_authorization_code(
            api_url="http://localhost:8080",
            authorization_endpoint="/auth/login/cli",
            open_browser=True,
            timeout=10,
        )


@patch("airflowctl.ctl.utils.browser_login.webbrowser.open", autospec=True)
def test_get_authorization_code_no_browser_times_out(mock_open):
    with pytest.raises(AirflowCtlBrowserLoginException, match="No login completed within 0.2 seconds"):
        get_authorization_code(
            api_url="http://localhost:8080",
            authorization_endpoint="/auth/login/cli",
            open_browser=False,
            timeout=0.2,
        )

    mock_open.assert_not_called()
