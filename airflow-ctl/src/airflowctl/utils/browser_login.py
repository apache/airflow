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
"""
Pieces of ``airflowctl auth login --web`` that talk to the browser.

The login is the OAuth 2.0 authorization code grant with PKCE (RFC 7636) for a native app with a
loopback redirect (RFC 8252), with the Airflow API server acting as the authorization server:
airflowctl opens the approval page of the Airflow UI, the user signs in and approves, and the
browser is redirected to the listener here with a one-time code. airflowctl exchanges that code,
together with the PKCE verifier only it knows, for a CLI token.
"""

from __future__ import annotations

import hashlib
import html
import os
import re
import secrets
import time
from base64 import urlsafe_b64encode
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import NamedTuple
from urllib.parse import parse_qs, urlencode, urlsplit

# The approval page and both endpoints shipped with this Airflow version.
MIN_SERVER_VERSION = (3, 5)
# Long enough to sign in through an identity provider with a second factor first.
BROWSER_LOGIN_TIMEOUT_SECONDS = 300

_RESPONSE_PAGE = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>airflowctl</title></head>
<body style="font-family: system-ui, sans-serif; margin: 3em; color: #222;">
<h1 style="font-size: 1.4em;">airflowctl</h1>
<p>{message}</p>
</body>
</html>
"""


class CallbackResult(NamedTuple):
    """What the browser brought back: a code on approval, an error otherwise."""

    code: str | None
    error: str | None


def is_browser_login_supported(server_version: str) -> bool:
    """Return whether the API server is new enough to have the approval page and its endpoints."""
    match = re.match(r"(\d+)\.(\d+)", server_version)
    if not match:
        return True
    return (int(match.group(1)), int(match.group(2))) >= MIN_SERVER_VERSION


def is_ssh_session() -> bool:
    """Return whether this terminal was opened over SSH, where a browser could not reach the local port."""
    return bool(os.environ.get("SSH_CONNECTION") or os.environ.get("SSH_TTY"))


def compute_code_challenge(code_verifier: str) -> str:
    """Derive the S256 code challenge of a PKCE verifier (RFC 7636 section 4.2)."""
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def generate_pkce_pair() -> tuple[str, str]:
    """Return a fresh PKCE verifier and its S256 challenge."""
    code_verifier = secrets.token_urlsafe(64)
    return code_verifier, compute_code_challenge(code_verifier)


def get_confirmation_code(code_challenge: str) -> str:
    """
    Return the short code the approval page shows for this request.

    The UI derives it from the same characters, so a user can tell the request on the page is the one
    their terminal started.
    """
    return code_challenge[:8]


def build_authorize_url(api_url: str, *, port: int, state: str, code_challenge: str) -> str:
    """Return the URL of the approval page in the Airflow UI for this login attempt."""
    query = urlencode(
        {"port": port, "state": state, "code_challenge": code_challenge, "code_challenge_method": "S256"}
    )
    return f"{api_url.rstrip('/')}/cli/authorize?{query}"


class _CallbackHandler(BaseHTTPRequestHandler):
    server: LoopbackCallbackServer

    # Browsers open speculative connections without sending a request; without a timeout one of
    # those would block the listener for good.
    timeout = 10

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        if url.path != "/callback":
            self._respond(HTTPStatus.NOT_FOUND, "Not found.")
            return
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        if not secrets.compare_digest(params.get("state", ""), self.server.expected_state):
            self._respond(
                HTTPStatus.BAD_REQUEST,
                "This response does not belong to the airflowctl login in progress. "
                "Run airflowctl auth login --web again.",
            )
            return
        code, error = params.get("code"), params.get("error")
        if not code and not error:
            self._respond(HTTPStatus.BAD_REQUEST, "The response carries neither a code nor an error.")
            return
        self.server.result = CallbackResult(code=code, error=error)
        self._respond(
            HTTPStatus.OK,
            "Login approved. You can close this tab and return to the terminal."
            if code
            else "Login denied. You can close this tab.",
        )

    def log_message(self, format: str, *args) -> None:
        """Keep the request log out of the terminal, which is showing the login's own messages."""

    def _respond(self, status: HTTPStatus, message: str) -> None:
        body = _RESPONSE_PAGE.format(message=html.escape(message)).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


class LoopbackCallbackServer(HTTPServer):
    """Listens on an ephemeral loopback port for the browser's single callback."""

    def __init__(self, expected_state: str):
        super().__init__(("127.0.0.1", 0), _CallbackHandler)
        self.expected_state = expected_state
        self.result: CallbackResult | None = None

    @property
    def port(self) -> int:
        return self.server_address[1]

    def wait_for_callback(self, timeout: float = BROWSER_LOGIN_TIMEOUT_SECONDS) -> CallbackResult | None:
        """Serve requests until the browser delivers a result or ``timeout`` seconds pass."""
        deadline = time.monotonic() + timeout
        # handle_request() returns after this long without a request, so the deadline is re-checked
        # and Ctrl-C is not swallowed by a blocking accept().
        self.timeout = 1
        while self.result is None and time.monotonic() < deadline:
            self.handle_request()
        return self.result
