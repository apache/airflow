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

import rich

from airflowctl.exceptions import AirflowCtlBrowserLoginException

CALLBACK_PATH = "/callback"
_CALLBACK_PAGE = (
    b"<!doctype html><title>airflowctl</title>"
    b"<p>Login complete. You can close this window and return to the terminal.</p>"
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


class _CallbackServer(HTTPServer):
    callback_params: dict[str, str] | None = None


class _CallbackHandler(BaseHTTPRequestHandler):
    server: _CallbackServer

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        # Browsers also ask for /favicon.ico; only the callback ends the wait.
        if url.path != CALLBACK_PATH or self.server.callback_params is not None:
            self.send_error(404)
            return
        self.server.callback_params = {key: values[0] for key, values in parse_qs(url.query).items()}
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(_CALLBACK_PAGE)

    def log_message(self, format, *args) -> None:
        # The default handler logs every request to stderr, including the code in the query string.
        pass


def get_authorization_code(
    *, api_url: str, authorization_endpoint: str, open_browser: bool, timeout: float
) -> AuthorizationCode:
    """
    Run the browser part of the login and return the authorization code it produced.

    :param api_url: URL of the Airflow API server
    :param authorization_endpoint: ``authorization_endpoint`` from the CLI login metadata
    :param open_browser: open the login page in the default browser, otherwise only print its URL
    :param timeout: seconds to wait for the user to finish logging in
    """
    code_verifier, code_challenge = build_pkce_pair()
    state = secrets.token_urlsafe(32)

    with _CallbackServer(("127.0.0.1", 0), _CallbackHandler) as server:
        redirect_uri = f"http://127.0.0.1:{server.server_address[1]}{CALLBACK_PATH}"
        # Endpoints in the metadata are absolute paths, which keeps any path prefix of the API server.
        login_url = (
            urljoin(api_url, authorization_endpoint)
            + "?"
            + urlencode(
                {
                    "redirect_uri": redirect_uri,
                    "state": state,
                    "code_challenge": code_challenge,
                    "code_challenge_method": "S256",
                }
            )
        )
        rich.print(f"[blue]Log in from your browser:[/blue] {login_url}")
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

    if not secrets.compare_digest(params.get("state", ""), state):
        raise AirflowCtlBrowserLoginException("The login response does not match this login attempt.")
    if "error" in params:
        raise AirflowCtlBrowserLoginException(
            f"Login refused: {params.get('error_description') or params['error']}"
        )
    if not params.get("code"):
        raise AirflowCtlBrowserLoginException("The login response contains no authorization code.")
    return AuthorizationCode(code=params["code"], code_verifier=code_verifier, redirect_uri=redirect_uri)
