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

import json
import pathlib
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import (
    JWKS,
    InvalidClaimError,
    JWTGenerator,
    JWTValidator,
    generate_private_key,
    get_sig_validation_args,
    key_to_jwk_dict,
    key_to_pem,
)

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from kgb import SpyAgency
    from time_machine import TimeMachineFixture


pytestmark = [pytest.mark.asyncio]


@pytest.fixture
def private_key(request):
    return request.getfixturevalue(request.param or "ed25519_private_key")


class TestJWKS:
    @pytest.mark.parametrize("private_key", ["rsa_private_key", "ed25519_private_key"], indirect=True)
    async def test_fetch_jwks_success(self, private_key):
        jwk_content = json.dumps({"keys": [key_to_jwk_dict(private_key, "kid")]})

        async def mock_transport(request):
            return httpx.Response(status_code=200, content=jwk_content)

        client = httpx.AsyncClient(transport=httpx.MockTransport(mock_transport))
        jwks = JWKS(url="https://example.com/jwks.json", client=client)

        # Test fetching JWKS
        await jwks.fetch_jwks()
        assert isinstance(await jwks.get_key("kid"), jwt.PyJWK)

    async def test_refresh_remote_jwks(
        self, time_machine: TimeMachineFixture, ed25519_private_key, spy_agency: SpyAgency
    ):
        time_machine.move_to(datetime(2023, 10, 1, 12, 0, 0))  # Initial time: 12:00 PM
        jwk_content = json.dumps({"keys": [key_to_jwk_dict(ed25519_private_key, "kid")]})

        async def mock_transport(request):
            return httpx.Response(status_code=200, content=jwk_content)

        client = httpx.AsyncClient(transport=httpx.MockTransport(mock_transport))
        jwks = JWKS(url="https://example.com/jwks.json", client=client)
        spy = spy_agency.spy_on(JWKS._fetch_remote_jwks)

        key = await jwks.get_key("kid")
        assert isinstance(key, jwt.PyJWK)

        # Move forward in time, but not to a point where it updates. Should not end up re-requesting.
        spy.reset_calls()
        time_machine.shift(1800)
        assert await jwks.get_key("kid") is key
        spy_agency.assert_spy_not_called(spy)

        # Not to a point where it should refresh
        time_machine.shift(1801)

        key2 = key_to_jwk_dict(generate_private_key("Ed25519"), "kid2")
        jwk_content = json.dumps({"keys": [key2]})
        with pytest.raises(KeyError):
            # Not in the document anymore, should have gone from the keyset
            await jwks.get_key("kid")
        assert isinstance(await jwks.get_key("kid2"), jwt.PyJWK)
        spy_agency.assert_spy_called(spy)


def test_load_pk_from_file(tmp_path: pathlib.Path, rsa_private_key):
    from tests_common.test_utils.config import conf_vars

    path = tmp_path / "test.pem"
    path.write_bytes(key_to_pem(rsa_private_key))

    with conf_vars({("api_auth", "jwt_private_key_path"): str(path), ("api_auth", "jwt_secret"): None}):
        generator = JWTGenerator(audience="", valid_for=0)

    assert isinstance(generator._private_key, RSAPrivateKey)
    assert generator.signing_arg is generator._private_key
    assert generator.kid != "not-used"
    assert len(generator.kid) == 43  # A 43 char thumprint should be returned


def test_with_secret_key():
    generator = JWTGenerator(secret_key="abc", audience=0, valid_for=0)
    assert generator._private_key is None
    assert generator.kid == "not-used"
    assert generator.signing_arg == "abc"


@pytest.fixture
def jwt_generator(ed25519_private_key: Ed25519PrivateKey):
    key = ed25519_private_key
    return JWTGenerator(
        private_key=key,
        kid="kid1",
        valid_for=60,
        issuer="http://test-issuer",
        algorithm="EdDSA",
        audience="abc",
    )


@pytest.fixture
def jwt_validator(ed25519_private_key: Ed25519PrivateKey):
    key = ed25519_private_key
    jwks = JWKS.from_private_key((key, "kid1"))
    return JWTValidator(jwks=jwks, issuer="http://test-issuer", algorithm="EdDSA", audience="abc")


async def test_task_jwt_generator_validator(
    jwt_generator: JWTGenerator, jwt_validator: JWTValidator, ed25519_private_key
):
    token = jwt_generator.generate({"sub": "test_subject"})
    # if this does not raise ValueError then the generated token can be decoded and contains all the required
    # fields.
    claims = await jwt_validator.avalidated_claims(
        token, required_claims={"sub": {"essential": True, "value": "test_subject"}}
    )
    nbf = datetime.fromtimestamp(claims["nbf"], timezone.utc)
    iat = datetime.fromtimestamp(claims["iat"], timezone.utc)
    exp = datetime.fromtimestamp(claims["exp"], timezone.utc)
    now = datetime.now(timezone.utc)
    assert nbf == iat, "issued at is different then not before"
    assert nbf < exp, "not before is after expiration"
    assert nbf <= now, "not before is in the future"
    assert exp >= now, "expiration is in the past"
    assert exp <= nbf + timedelta(minutes=10), "expiration is more then 10 minutes after not before"
    assert "jti" in claims, "JWT ID is missing"
    assert len(claims["jti"]) == 32, "JWT ID is not a valid UUID"

    def token_without_claim(claim: str) -> str:
        # remove claim and re-encode
        bad_claims = claims.copy()
        bad_claims.pop(required_claim)
        return jwt.encode(
            bad_claims,
            ed25519_private_key,
            headers={"kid": jwt_generator.kid},
            algorithm=jwt_generator.algorithm,
        )

    for required_claim in jwt_validator.required_claims:
        bad_token = token_without_claim(required_claim)
        # check that the missing claim is detected in validation
        with pytest.raises(jwt.MissingRequiredClaimError) as exc_info:
            await jwt_validator.avalidated_claims(bad_token, required_claims={"sub": "test_subject"})
        assert exc_info.value.claim == required_claim


async def test_jwt_wrong_subject(jwt_generator, jwt_validator):
    # check that the token is invalid if the subject is not as expected
    wrong_subject = jwt_generator.generate({"sub": "wrong_subject"})
    with pytest.raises(InvalidClaimError, match="Invalid claim: sub"):
        await jwt_validator.avalidated_claims(
            wrong_subject, required_claims={"sub": {"essential": True, "value": "test_subject"}}
        )


@pytest.mark.parametrize(
    ("private_key", "algorithm"),
    [("rsa_private_key", "RS256"), ("ed25519_private_key", "EdDSA")],
    indirect=["private_key"],
)
async def test_jwt_generate_validate_roundtrip_with_jwks(private_key, algorithm, tmp_path: pathlib.Path):
    jwk_content = json.dumps({"keys": [key_to_jwk_dict(private_key, "custom-kid")]})

    jwks = tmp_path.joinpath("jwks.json")
    jwks.write_text(jwk_content)

    priv_key = tmp_path.joinpath("key.pem")
    priv_key.write_bytes(key_to_pem(private_key))

    with conf_vars(
        {
            ("api_auth", "trusted_jwks_url"): str(jwks),
            ("api_auth", "jwt_kid"): "custom-kid",
            ("api_auth", "jwt_issuer"): "http://my-issuer.localdomain",
            ("api_auth", "jwt_private_key_path"): str(priv_key),
            ("api_auth", "jwt_algorithm"): algorithm,
            ("api_auth", "jwt_secret"): "",
        }
    ):
        gen = JWTGenerator(audience="airflow1", valid_for=300)
        token = gen.generate({"sub": "test"})

        validator = JWTValidator(
            audience="airflow1",
            leeway=0,
            **get_sig_validation_args(make_secret_key_if_needed=False),
        )
        assert await validator.avalidated_claims(token)


@pytest.fixture(scope="session")
def rsa_private_key():
    return generate_private_key()


@pytest.fixture(scope="session")
def ed25519_private_key():
    return generate_private_key(key_type="Ed25519")
