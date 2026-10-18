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

import time
from datetime import timedelta

import jwt
import pytest
import time_machine

from airflow.api_fastapi.auth.cli_authorization_code import (
    CLI_AUTHORIZATION_CODE_AUDIENCE,
    CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS,
    InvalidCliAuthorizationCodeError,
    _get_code_signer,
    build_loopback_redirect_uri,
    compute_code_challenge,
    issue_cli_authorization_code,
    validate_cli_authorization_code,
)
from airflow.api_fastapi.auth.tokens import JWTGenerator, get_signing_args

# Test vector from RFC 7636, appendix B.
RFC_7636_VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
RFC_7636_CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"

USER = {"sub": "jdoe", "role": "admin"}
REDIRECT_URI = build_loopback_redirect_uri(53682)


def issue_code() -> str:
    return issue_cli_authorization_code(
        user=USER, code_challenge=RFC_7636_CHALLENGE, redirect_uri=REDIRECT_URI
    )


def test_build_loopback_redirect_uri_only_ever_points_at_loopback():
    assert build_loopback_redirect_uri(53682) == "http://127.0.0.1:53682/callback"


def test_compute_code_challenge_matches_rfc_7636_test_vector():
    assert compute_code_challenge(RFC_7636_VERIFIER) == RFC_7636_CHALLENGE


def test_issued_code_validates_with_its_verifier_and_redirect_uri():
    claims = validate_cli_authorization_code(
        issue_code(), code_verifier=RFC_7636_VERIFIER, redirect_uri=REDIRECT_URI
    )

    assert claims["user"] == USER
    assert claims["aud"] == CLI_AUTHORIZATION_CODE_AUDIENCE
    assert claims["jti"]
    assert claims["exp"] - claims["iat"] == CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS


def test_code_expires_after_its_lifetime():
    code = issue_code()

    with time_machine.travel(timedelta(seconds=CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS + 30), tick=False):
        with pytest.raises(InvalidCliAuthorizationCodeError, match="has expired"):
            validate_cli_authorization_code(code, code_verifier=RFC_7636_VERIFIER, redirect_uri=REDIRECT_URI)


def test_code_rejects_a_verifier_that_does_not_match_its_challenge():
    with pytest.raises(InvalidCliAuthorizationCodeError, match="verifier does not match"):
        validate_cli_authorization_code(issue_code(), code_verifier="x" * 43, redirect_uri=REDIRECT_URI)


def test_code_is_bound_to_the_port_it_was_issued_for():
    with pytest.raises(InvalidCliAuthorizationCodeError, match="not issued for this port"):
        validate_cli_authorization_code(
            issue_code(), code_verifier=RFC_7636_VERIFIER, redirect_uri=build_loopback_redirect_uri(53683)
        )


def test_api_token_is_not_accepted_as_a_code():
    api_token = JWTGenerator(**get_signing_args(), valid_for=3600, audience="apache-airflow").generate(
        {"sub": "jdoe", "user": USER, "code_challenge": RFC_7636_CHALLENGE, "redirect_uri": REDIRECT_URI}
    )

    with pytest.raises(InvalidCliAuthorizationCodeError, match="not valid"):
        validate_cli_authorization_code(api_token, code_verifier=RFC_7636_VERIFIER, redirect_uri=REDIRECT_URI)


def test_tampered_code_is_rejected():
    header, payload, signature = issue_code().split(".")
    tampered = f"{header}.{payload}.{signature[:-2]}AA"

    with pytest.raises(InvalidCliAuthorizationCodeError, match="not valid"):
        validate_cli_authorization_code(tampered, code_verifier=RFC_7636_VERIFIER, redirect_uri=REDIRECT_URI)


@pytest.mark.parametrize("missing_claim", ["user", "jti"])
def test_code_without_user_or_jti_is_rejected(missing_claim):
    signer = _get_code_signer()
    now = int(time.time())
    claims = {
        "user": USER,
        "jti": "abc",
        "code_challenge": RFC_7636_CHALLENGE,
        "redirect_uri": REDIRECT_URI,
        "aud": CLI_AUTHORIZATION_CODE_AUDIENCE,
        "iss": signer.issuer,
        "iat": now,
        "nbf": now,
        "exp": now + 60,
    }
    del claims[missing_claim]
    if claims.get("iss") is None:
        del claims["iss"]
    code = jwt.encode(claims, signer.signing_arg, algorithm=signer.algorithm, headers={"kid": signer.kid})

    with pytest.raises(InvalidCliAuthorizationCodeError, match="not valid"):
        validate_cli_authorization_code(code, code_verifier=RFC_7636_VERIFIER, redirect_uri=REDIRECT_URI)
