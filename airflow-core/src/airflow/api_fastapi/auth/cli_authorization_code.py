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
One-time authorization codes for ``airflowctl auth login --web``.

The browser login follows the OAuth 2.0 authorization code grant with PKCE (RFC 7636) for a native
app with a loopback redirect (RFC 8252), with Airflow acting as its own authorization server. The
approval page in the UI asks ``POST /ui/auth/cli/authorize`` for a code and redirects the browser to
``http://127.0.0.1:<port>/callback``, where airflowctl is listening. airflowctl then exchanges the
code and the PKCE verifier only it knows at ``POST /api/v2/auth/cli/token`` for a CLI token.

A code is a short-lived JWT signed with the same key as Airflow's own tokens. It carries the
serialized user, the PKCE challenge and the redirect URI, so the exchange needs no server-side
state and works across API server replicas. Its dedicated audience keeps a code from being
accepted as an API token, and an API token from being accepted as a code.
"""

from __future__ import annotations

import hashlib
import hmac
from base64 import urlsafe_b64encode
from functools import cache
from typing import Any

import jwt

from airflow.api_fastapi.auth.tokens import (
    JWTGenerator,
    JWTValidator,
    get_sig_validation_args,
    get_signing_args,
)

CLI_AUTHORIZATION_CODE_AUDIENCE = "urn:airflow.apache.org:cli-authorization-code"
CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS = 60
# Only S256 (RFC 7636 section 4.2): with ``plain``, whoever sees the code in the browser also holds the verifier.
CODE_CHALLENGE_METHOD = "S256"
# airflowctl listens on an ephemeral port, so a request naming a privileged one did not come from it.
MIN_LOOPBACK_PORT = 1024
MAX_LOOPBACK_PORT = 65535
# Unpadded base64url of a SHA-256 digest is always 43 characters.
CODE_CHALLENGE_PATTERN = r"^[A-Za-z0-9_-]{43}$"
# Length and alphabet of a verifier, from RFC 7636 section 4.1.
CODE_VERIFIER_PATTERN = r"^[A-Za-z0-9._~-]{43,128}$"


class InvalidCliAuthorizationCodeError(ValueError):
    """Raised when a code cannot be exchanged for a CLI token."""


def build_loopback_redirect_uri(port: int) -> str:
    """Return the only redirect URI a code for ``port`` is valid for: the host is never anything but loopback."""
    return f"http://127.0.0.1:{port}/callback"


def compute_code_challenge(code_verifier: str) -> str:
    """Derive the S256 code challenge of a PKCE verifier (RFC 7636 section 4.2)."""
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


@cache
def _get_code_signer() -> JWTGenerator:
    return JWTGenerator(
        **get_signing_args(),
        valid_for=CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS,
        audience=CLI_AUTHORIZATION_CODE_AUDIENCE,
    )


@cache
def _get_code_validator() -> JWTValidator:
    return JWTValidator(**get_sig_validation_args(), audience=CLI_AUTHORIZATION_CODE_AUDIENCE)


def issue_cli_authorization_code(*, user: dict[str, Any], code_challenge: str, redirect_uri: str) -> str:
    """Return a signed code bound to the serialized ``user``, the PKCE challenge and the redirect URI."""
    return _get_code_signer().generate(
        {"user": user, "code_challenge": code_challenge, "redirect_uri": redirect_uri}
    )


def validate_cli_authorization_code(code: str, *, code_verifier: str, redirect_uri: str) -> dict[str, Any]:
    """
    Return the claims of ``code`` if it was issued for ``redirect_uri`` and ``code_verifier``.

    Single use is not enforced here: the caller records the ``jti`` claim before issuing a token.
    """
    try:
        claims = _get_code_validator().validated_claims(code)
    except jwt.ExpiredSignatureError:
        raise InvalidCliAuthorizationCodeError("The authorization code has expired")
    except jwt.InvalidTokenError:
        raise InvalidCliAuthorizationCodeError("The authorization code is not valid")

    if not isinstance(claims.get("user"), dict) or not claims.get("jti"):
        raise InvalidCliAuthorizationCodeError("The authorization code is not valid")
    if claims.get("redirect_uri") != redirect_uri:
        raise InvalidCliAuthorizationCodeError("The authorization code was not issued for this port")
    if not hmac.compare_digest(compute_code_challenge(code_verifier), str(claims.get("code_challenge", ""))):
        raise InvalidCliAuthorizationCodeError("The code verifier does not match the authorization code")
    return claims
