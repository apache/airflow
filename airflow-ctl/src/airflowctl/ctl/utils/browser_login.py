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
"""Browser login with the authorization code flow and PKCE (RFC 7636), over a loopback redirect (RFC 8252)."""

from __future__ import annotations

import base64
import hashlib
import secrets
import time
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

from rich.console import Console
from rich.markup import escape

from airflowctl.exceptions import AirflowCtlBrowserLoginException

CALLBACK_PATH = "/callback"
# Sent before the code is exchanged for a token, so it must not claim that the login worked.
_CALLBACK_PAGE = (
    b"<!doctype html><title>airflowctl</title>"
    b"<p>You can close this window and return to the terminal to see the result of the login.</p>"
)
_CALLBACK_FAILED_PAGE = (
    b"<!doctype html><title>airflowctl</title><p>Login failed. Return to the terminal for details.</p>"
)


@dataclass
class AuthorizationCode:
    """Authorization code returned to the loopback redirect, with what the token endpoint needs to redeem it."""

    code: str
    code_verifier: str
    redirect_uri: str


def build_pkce_pair() -> tuple[str, str]:
    """Return a PKCE ``(code_verifier, code_challenge)`` pair for the ``S256`` method."""
    code_verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    code_challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return code_verifier, code_challenge


def resolve_endpoint(api_url: str, endpoint: str) -> str:
    """Return the URL of a login metadata endpoint, which must be on the API server."""
    url = urljoin(api_url, endpoint)
    if urlsplit(url)[:2] != urlsplit(api_url)[:2]:
        raise AirflowCtlBrowserLoginException(f"{endpoint} is not an endpoint of {api_url}.")
    return url


def _find_login_error(params: dict[str, str], state: str) -> str | None:
    """Return why the callback parameters do not complete this login, or ``None`` if they do."""
    # compare_digest raises TypeError on non-ASCII str.
    if not secrets.compare_digest(params.get("state", "").encode(), state.encode()):
        return "The login response does not match this login attempt."
    if "error" in params:
        return f"Login refused: {params.get('error_description') or params['error']}"
    if not params.get("code"):
        return "The login response contains no authorization code."
    return None


class _CallbackServer(HTTPServer):
    state: str
    callback_params: dict[str, str] | None = None
    login_error: str | None = None


class _CallbackHandler(BaseHTTPRequestHandler):
    server: _CallbackServer
    # server.timeout only bounds accept(), so an idle connection such as a browser preconnect would block.
    timeout = 2

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        if url.path != CALLBACK_PATH or self.server.callback_params is not None:
            self.send_error(404)
            return
        params = {key: values[0] for key, values in parse_qs(url.query).items()}
        login_error = _find_login_error(params, self.server.state)
        self.server.callback_params = params
        self.server.login_error = login_error
        self.send_response(400 if login_error else 200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(_CALLBACK_FAILED_PAGE if login_error else _CALLBACK_PAGE)

    def log_message(self, format, *args) -> None:
        # Request lines contain the authorization code.
        pass


def get_authorization_code(
    *, authorization_url: str, open_browser: bool, timeout: float
) -> AuthorizationCode:
    """
    Run the browser part of the login and return the authorization code it produced.

    :param authorization_url: URL of the ``authorization_endpoint`` from the CLI login metadata
    :param open_browser: open the login page in the default browser, otherwise only print its URL
    :param timeout: seconds to wait for the user to finish logging in
    """
    code_verifier, code_challenge = build_pkce_pair()
    state = secrets.token_urlsafe(32)

    with _CallbackServer(("127.0.0.1", 0), _CallbackHandler) as server:
        server.state = state
        redirect_uri = f"http://127.0.0.1:{server.server_address[1]}{CALLBACK_PATH}"
        query = urlencode(
            {
                "redirect_uri": redirect_uri,
                "state": state,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
        )
        login_url = f"{authorization_url}?{query}"
        # `auth token --sso` prints the token on stdout.
        Console(stderr=True, soft_wrap=True).print(
            f"[blue]Log in from your browser:[/blue] {escape(login_url)}"
        )
        if open_browser:
            webbrowser.open(login_url)

        deadline = time.monotonic() + timeout
        while server.callback_params is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AirflowCtlBrowserLoginException(f"No login completed within {timeout:g} seconds.")
            server.timeout = remaining
            server.handle_request()
        params = server.callback_params
        login_error = server.login_error

    if login_error:
        raise AirflowCtlBrowserLoginException(login_error)
    return AuthorizationCode(code=params["code"], code_verifier=code_verifier, redirect_uri=redirect_uri)
