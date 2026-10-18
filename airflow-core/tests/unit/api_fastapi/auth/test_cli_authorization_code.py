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
from unittest.mock import patch

import jwt
import pytest
import time_machine
from cryptography.fernet import Fernet, MultiFernet

from airflow.api_fastapi.auth.cli_authorization_code import (
    CLI_AUTHORIZATION_CODE_AUDIENCE,
    CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS,
    CliAuthorizationCodeUnavailableError,
    InvalidCliAuthorizationCodeError,
    _get_code_signer,
    build_loopback_redirect_uri,
    compute_code_challenge,
    issue_cli_authorization_code,
    validate_cli_authorization_code,
)
from airflow.api_fastapi.auth.tokens import JWTGenerator, get_signing_args
from airflow.models.crypto import _NullFernet, _RealFernet, get_fernet

# Test vector from RFC 7636, appendix B.
RFC_7636_VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
RFC_7636_CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"

CLI_TOKEN = "header.payload.signature"
TOKEN_EXPIRES_AT = 2_000_000_000
REDIRECT_URI = build_loopback_redirect_uri(53682)


def issue_code() -> str:
    return issue_cli_authorization_code(
        cli_token=CLI_TOKEN,
        token_expires_at=TOKEN_EXPIRES_AT,
        user_name="jdoe",
        code_challenge=RFC_7636_CHALLENGE,
        redirect_uri=REDIRECT_URI,
    )


def validate(code: str) -> dict:
    return validate_cli_authorization_code(code, code_verifier=RFC_7636_VERIFIER, redirect_uri=REDIRECT_URI)


def test_build_loopback_redirect_uri_only_ever_points_at_loopback():
    assert build_loopback_redirect_uri(53682) == "http://127.0.0.1:53682/callback"


def test_compute_code_challenge_matches_rfc_7636_test_vector():
    assert compute_code_challenge(RFC_7636_VERIFIER) == RFC_7636_CHALLENGE


def test_issued_code_validates_with_its_verifier_and_redirect_uri():
    claims = validate(issue_code())

    assert claims["token"] == CLI_TOKEN
    assert claims["token_expires_at"] == TOKEN_EXPIRES_AT
    assert claims["name"] == "jdoe"
    assert claims["aud"] == CLI_AUTHORIZATION_CODE_AUDIENCE
    assert claims["jti"]
    assert claims["exp"] - claims["iat"] == CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS


def test_code_carries_the_token_only_encrypted():
    payload = jwt.decode(issue_code(), options={"verify_signature": False})

    assert CLI_TOKEN not in payload["token"]
    assert get_fernet().decrypt(payload["token"].encode("ascii")) == CLI_TOKEN.encode("utf-8")


@patch(
    "airflow.api_fastapi.auth.cli_authorization_code.get_fernet", autospec=True, return_value=_NullFernet()
)
def test_codes_need_a_fernet_key(mock_get_fernet):
    with pytest.raises(CliAuthorizationCodeUnavailableError, match="fernet_key"):
        issue_code()


def test_code_cannot_be_redeemed_without_the_fernet_key():
    code = issue_code()

    with (
        patch(
            "airflow.api_fastapi.auth.cli_authorization_code.get_fernet",
            autospec=True,
            return_value=_NullFernet(),
        ),
        pytest.raises(CliAuthorizationCodeUnavailableError, match="fernet_key"),
    ):
        validate(code)


def test_code_encrypted_with_another_key_is_rejected():
    code = issue_code()
    other_key = _RealFernet(MultiFernet([Fernet(Fernet.generate_key())]))

    with (
        patch(
            "airflow.api_fastapi.auth.cli_authorization_code.get_fernet",
            autospec=True,
            return_value=other_key,
        ),
        pytest.raises(InvalidCliAuthorizationCodeError, match="cannot be decrypted"),
    ):
        validate(code)


def test_code_expires_after_its_lifetime():
    code = issue_code()

    with time_machine.travel(timedelta(seconds=CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS + 30), tick=False):
        with pytest.raises(InvalidCliAuthorizationCodeError, match="has expired"):
            validate(code)


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
        {
            "sub": "jdoe",
            "token": CLI_TOKEN,
            "code_challenge": RFC_7636_CHALLENGE,
            "redirect_uri": REDIRECT_URI,
        }
    )

    with pytest.raises(InvalidCliAuthorizationCodeError, match="not valid"):
        validate(api_token)


def test_tampered_code_is_rejected():
    header, payload, signature = issue_code().split(".")
    tampered = f"{header}.{payload}.{signature[:-2]}AA"

    with pytest.raises(InvalidCliAuthorizationCodeError, match="not valid"):
        validate(tampered)


@pytest.mark.parametrize("missing_claim", ["token", "token_expires_at", "jti"])
def test_code_without_its_required_claims_is_rejected(missing_claim):
    signer = _get_code_signer()
    now = int(time.time())
    claims = {
        "token": get_fernet().encrypt(CLI_TOKEN.encode("utf-8")).decode("ascii"),
        "token_expires_at": TOKEN_EXPIRES_AT,
        "name": "jdoe",
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
        validate(code)
