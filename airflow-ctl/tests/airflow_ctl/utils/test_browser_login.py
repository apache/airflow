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

import threading
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from airflowctl.utils.browser_login import (
    CallbackResult,
    LoopbackCallbackServer,
    build_authorize_url,
    compute_code_challenge,
    generate_pkce_pair,
    get_confirmation_code,
    is_ssh_session,
)

# Test vector from RFC 7636, appendix B.
RFC_7636_VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
RFC_7636_CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, False),
        ({"SSH_CONNECTION": "10.0.0.1 50000 10.0.0.2 22"}, True),
        ({"SSH_TTY": "/dev/pts/0"}, True),
    ],
)
def test_is_ssh_session(monkeypatch, environ, expected):
    monkeypatch.delenv("SSH_CONNECTION", raising=False)
    monkeypatch.delenv("SSH_TTY", raising=False)
    for key, value in environ.items():
        monkeypatch.setenv(key, value)

    assert is_ssh_session() is expected


def test_compute_code_challenge_matches_rfc_7636_test_vector():
    assert compute_code_challenge(RFC_7636_VERIFIER) == RFC_7636_CHALLENGE


def test_generate_pkce_pair_returns_a_fresh_verifier_with_its_challenge():
    verifier, challenge = generate_pkce_pair()
    other_verifier, _ = generate_pkce_pair()

    assert 43 <= len(verifier) <= 128
    assert challenge == compute_code_challenge(verifier)
    assert verifier != other_verifier


def test_confirmation_code_is_the_start_of_the_challenge():
    assert get_confirmation_code(RFC_7636_CHALLENGE) == "E9Melhoa"


def test_build_authorize_url_points_at_the_approval_page():
    url = build_authorize_url(
        "https://airflow.example.com/", port=53682, state="the-state", code_challenge=RFC_7636_CHALLENGE
    )

    parts = urlsplit(url)
    assert f"{parts.scheme}://{parts.netloc}{parts.path}" == "https://airflow.example.com/cli/authorize"
    assert parse_qs(parts.query) == {
        "port": ["53682"],
        "state": ["the-state"],
        "code_challenge": [RFC_7636_CHALLENGE],
        "code_challenge_method": ["S256"],
    }


class TestLoopbackCallbackServer:
    @pytest.fixture
    def server(self):
        with LoopbackCallbackServer(expected_state="the-state") as server:
            yield server

    @staticmethod
    def wait(
        server: LoopbackCallbackServer, path: str, timeout: float = 5
    ) -> tuple[CallbackResult | None, httpx.Response]:
        """Hit the listener from another thread, like a browser would, while the server waits for the result."""
        responses: list[httpx.Response] = []
        thread = threading.Thread(
            target=lambda: responses.append(httpx.get(f"http://127.0.0.1:{server.port}{path}"))
        )
        thread.start()
        result = server.wait_for_callback(timeout=timeout)
        thread.join(timeout=5)
        return result, responses[0]

    def test_listens_on_an_ephemeral_loopback_port(self, server):
        assert server.server_address[0] == "127.0.0.1"
        assert server.port >= 1024

    def test_delivers_the_code_of_a_matching_callback(self, server):
        result, response = self.wait(server, "/callback?code=one-time-code&state=the-state")

        assert result == CallbackResult(code="one-time-code", error=None)
        assert response.status_code == 200
        assert "You can close this tab" in response.text
        assert response.headers["content-type"] == "text/html; charset=utf-8"

    def test_delivers_a_denial(self, server):
        result, response = self.wait(server, "/callback?error=access_denied&state=the-state")

        assert result == CallbackResult(code=None, error="access_denied")
        assert response.status_code == 200
        assert "Login denied" in response.text

    @pytest.mark.parametrize(
        ("path", "status"),
        [
            pytest.param("/callback?code=stolen&state=someone-elses", 400, id="foreign-state"),
            pytest.param("/callback?state=the-state", 400, id="no-code-no-error"),
            pytest.param("/favicon.ico", 404, id="other-path"),
        ],
    )
    def test_ignores_requests_that_are_not_its_callback(self, server, path, status):
        result, response = self.wait(server, path, timeout=1.5)

        assert result is None
        assert response.status_code == status

    def test_times_out_without_a_callback(self, server):
        assert server.wait_for_callback(timeout=0.1) is None
