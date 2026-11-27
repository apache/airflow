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

import asyncio
import json
import pathlib
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import patch

import anyio
import httpx2
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
            return httpx2.Response(status_code=200, content=jwk_content)

        client = httpx2.AsyncClient(transport=httpx2.MockTransport(mock_transport))
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
            return httpx2.Response(status_code=200, content=jwk_content)

        client = httpx2.AsyncClient(transport=httpx2.MockTransport(mock_transport))
        current = 1_000_000.0

        def mock_monotonic():
            return current

        with patch("airflow.api_fastapi.auth.tokens.time.monotonic", side_effect=mock_monotonic):
            jwks = JWKS(url="https://example.com/jwks.json", client=client)
            spy = spy_agency.spy_on(JWKS._fetch_remote_jwks)

            key = await jwks.get_key("kid")
            assert isinstance(key, jwt.PyJWK)

            # Move forward in time, but not to a point where it updates. Should not end up re-requesting.
            spy.reset_calls()
            # time_machine.shift(1800)
            current += 1800
            assert await jwks.get_key("kid") is key
            spy_agency.assert_spy_not_called(spy)

            # Not to a point where it should refresh
            # time_machine.shift(1801)
            current += 1801

            key2 = key_to_jwk_dict(generate_private_key("Ed25519"), "kid2")
            jwk_content = json.dumps({"keys": [key2]})
            with pytest.raises(KeyError):
                # Not in the document anymore, should have gone from the keyset
                await jwks.get_key("kid")
            assert isinstance(await jwks.get_key("kid2"), jwt.PyJWK)
            spy_agency.assert_spy_called(spy)


class _Clock:
    def __init__(self):
        self.now = 1_000_000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock():
    fake_clock = _Clock()
    with patch("airflow.api_fastapi.auth.tokens.time.monotonic", side_effect=fake_clock.monotonic):
        yield fake_clock


def _jwk_without_alg(private_key, kid: str) -> dict:
    jwk_dict = key_to_jwk_dict(private_key, kid)
    jwk_dict.pop("alg", None)
    return jwk_dict


def _write_jwks(path: pathlib.Path, *jwk_dicts: dict) -> None:
    path.write_text(json.dumps({"keys": list(jwk_dicts)}))


def _served_jwks_client(documents: list) -> httpx2.AsyncClient:
    """Serve the last entry of ``documents``; an exception entry is raised instead."""

    async def mock_transport(request):
        document = documents[-1]
        if isinstance(document, Exception):
            raise document
        return httpx2.Response(status_code=200, json=document)

    return httpx2.AsyncClient(transport=httpx2.MockTransport(mock_transport))


@pytest.mark.usefixtures("clock")
class TestJWKSKeyRotation:
    @pytest.fixture
    def jwks_file(self, tmp_path: pathlib.Path) -> pathlib.Path:
        return tmp_path / "jwks.json"

    async def test_unknown_kid_reloads_local_jwks(self, clock, jwks_file, rsa_private_key):
        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "old"))
        jwks = JWKS(url=str(jwks_file))
        await jwks.get_key("old")

        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 11

        assert (await jwks.get_key("new")).key_id == "new"

    async def test_unknown_kid_reloads_remote_jwks(self, clock, rsa_private_key):
        documents: list = [{"keys": [key_to_jwk_dict(rsa_private_key, "old")]}]
        jwks = JWKS(url="https://example.com/jwks.json", client=_served_jwks_client(documents))
        await jwks.get_key("old")

        documents.append({"keys": [key_to_jwk_dict(rsa_private_key, "new")]})
        clock.now += 11

        assert (await jwks.get_key("new")).key_id == "new"

    async def test_unknown_kid_reload_is_rate_limited(self, clock, jwks_file, rsa_private_key):
        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "old"))
        jwks = JWKS(url=str(jwks_file), refresh_retry_interval_secs=30)
        await jwks.get_key("old")

        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 5
        with pytest.raises(KeyError, match="new"):
            await jwks.get_key("new")

        clock.now += 30
        assert (await jwks.get_key("new")).key_id == "new"

    async def test_unknown_kid_does_not_reload_when_refresh_disabled(self, clock, jwks_file, rsa_private_key):
        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "old"))
        jwks = JWKS(url=str(jwks_file), refresh_jwks=False)
        await jwks.get_key("old")

        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 3601

        with pytest.raises(KeyError, match="new"):
            await jwks.get_key("new")

    async def test_kid_still_unknown_after_reload_is_rejected(self, clock, jwks_file, rsa_private_key):
        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "old"))
        validator = JWTValidator(
            jwks=JWKS(url=str(jwks_file)), issuer="http://test-issuer", algorithm="RS256", audience="abc"
        )
        generator = JWTGenerator(
            private_key=rsa_private_key,
            kid="unpublished",
            issuer="http://test-issuer",
            algorithm="RS256",
            audience="abc",
            valid_for=300,
        )
        clock.now += 11

        with pytest.raises(jwt.InvalidTokenError, match="Kid did not match any validation keys"):
            await validator.avalidated_claims(generator.generate())

    @pytest.mark.parametrize(
        ("elapsed_secs", "expected_kid"),
        [
            pytest.param(3599, "old", id="before-refresh-interval"),
            pytest.param(3601, "new", id="after-refresh-interval"),
        ],
    )
    async def test_local_jwks_is_reloaded_after_refresh_interval(
        self, clock, jwks_file, rsa_private_key, elapsed_secs, expected_kid
    ):
        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "old"))
        jwks = JWKS(url=str(jwks_file))
        await jwks.fetch_jwks()

        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += elapsed_secs
        await jwks.fetch_jwks()

        assert [key.key_id for key in jwks._jwks.keys] == [expected_kid]

    @pytest.mark.parametrize(
        "broken_content",
        [
            pytest.param("not json", id="unreadable"),
            pytest.param(json.dumps(["not", "an", "object"]), id="not-an-object"),
            pytest.param(json.dumps({"keys": []}), id="no-keys"),
            pytest.param(json.dumps({"keys": [{"kty": "unknown"}]}), id="no-usable-keys"),
        ],
    )
    async def test_failed_local_reload_keeps_current_keyset(
        self, clock, jwks_file, rsa_private_key, broken_content
    ):
        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "old"))
        jwks = JWKS(url=str(jwks_file))
        await jwks.fetch_jwks()

        jwks_file.write_text(broken_content)
        clock.now += 3601
        await jwks.fetch_jwks()

        assert (await jwks.get_key("old")).key_id == "old"

    async def test_failed_remote_reload_keeps_current_keyset(self, clock, rsa_private_key):
        documents: list = [{"keys": [key_to_jwk_dict(rsa_private_key, "old")]}]
        jwks = JWKS(url="https://example.com/jwks.json", client=_served_jwks_client(documents))
        await jwks.fetch_jwks()

        documents.append(httpx2.ConnectError("issuer unreachable"))
        clock.now += 3601
        await jwks.fetch_jwks()

        assert (await jwks.get_key("old")).key_id == "old"

    async def test_failed_reload_is_retried_after_retry_interval(self, clock, jwks_file, rsa_private_key):
        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "old"))
        jwks = JWKS(url=str(jwks_file))
        await jwks.fetch_jwks()

        jwks_file.write_text("not json")
        clock.now += 3601
        await jwks.fetch_jwks()

        _write_jwks(jwks_file, key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 11
        await jwks.fetch_jwks()

        assert [key.key_id for key in jwks._jwks.keys] == ["new"]

    async def test_unloadable_jwks_rejects_every_kid(self, jwks_file):
        jwks = JWKS(url=str(jwks_file))

        with pytest.raises(KeyError, match="kid"):
            await jwks.get_key("kid")

    async def test_from_private_key_is_not_reloaded(self, clock, rsa_private_key):
        jwks = JWKS.from_private_key((rsa_private_key, "kid"))
        clock.now += 3601

        assert (await jwks.get_key("kid")).key_id == "kid"
        with pytest.raises(KeyError, match="other"):
            await jwks.get_key("other")


class _FakeIssuer:
    """Serve the last entry of ``documents``, recording each fetch; the next fetch can be held in flight."""

    def __init__(self, *jwk_dicts: dict):
        self.documents: list = [{"keys": list(jwk_dicts)}]
        self.fetches: list = []
        self.fetch_started = threading.Event()
        self._hold_next = False
        self._held: tuple[asyncio.AbstractEventLoop, asyncio.Event] | None = None

    def rotate_to(self, *jwk_dicts: dict) -> None:
        self.documents.append({"keys": list(jwk_dicts)})

    def hold_next_fetch(self) -> None:
        self._hold_next = True
        self.fetch_started.clear()

    def release_fetch(self) -> None:
        assert self._held is not None
        loop, released = self._held
        loop.call_soon_threadsafe(released.set)

    async def _serve(self, request):
        self.fetches.append(request)
        if self._hold_next:
            self._hold_next = False
            self._held = (asyncio.get_running_loop(), asyncio.Event())
            self.fetch_started.set()
            await self._held[1].wait()
        return httpx2.Response(status_code=200, json=self.documents[-1])

    def jwks(self) -> JWKS:
        client = httpx2.AsyncClient(transport=httpx2.MockTransport(self._serve))
        return JWKS(url="https://example.com/jwks.json", client=client)


async def _get_key_concurrently(jwks: JWKS, kid: str, requests: int = 20) -> list:
    return list(await asyncio.gather(*(jwks.get_key(kid) for _ in range(requests)), return_exceptions=True))


async def _start_reload_and_shut_down_once_joined(jwks: JWKS, kid: str, joined: threading.Event) -> None:
    request = asyncio.create_task(jwks.get_key(kid))
    await asyncio.to_thread(joined.wait, 5)
    assert not request.done()


async def _get_key_while_fetch_held(jwks: JWKS, kid: str, issuer: _FakeIssuer) -> jwt.PyJWK:
    request = asyncio.create_task(jwks.get_key(kid))
    for _ in range(10):
        await asyncio.sleep(0)
    assert not request.done()
    issuer.release_fetch()
    return await request


@pytest.mark.usefixtures("clock")
class TestJWKSConcurrentRequests:
    async def test_concurrent_first_requests_share_one_fetch(self, rsa_private_key):
        issuer = _FakeIssuer(key_to_jwk_dict(rsa_private_key, "kid"))
        jwks = issuer.jwks()

        results = await _get_key_concurrently(jwks, "kid")

        assert [result.key_id for result in results] == ["kid"] * 20
        assert len(issuer.fetches) == 1

    async def test_concurrent_requests_for_rotated_key_share_one_reload(self, clock, rsa_private_key):
        issuer = _FakeIssuer(key_to_jwk_dict(rsa_private_key, "old"))
        jwks = issuer.jwks()
        await jwks.get_key("old")

        issuer.rotate_to(key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 11
        results = await _get_key_concurrently(jwks, "new")

        assert [result.key_id for result in results] == ["new"] * 20
        assert len(issuer.fetches) == 2

    async def test_concurrent_requests_for_made_up_key_share_one_reload(self, clock, rsa_private_key):
        issuer = _FakeIssuer(key_to_jwk_dict(rsa_private_key, "kid"))
        jwks = issuer.jwks()
        await jwks.get_key("kid")

        clock.now += 11
        results = await _get_key_concurrently(jwks, "made-up")

        assert all(isinstance(result, KeyError) for result in results)
        assert len(issuer.fetches) == 2

    def test_reload_on_another_event_loop(self, clock, rsa_private_key):
        issuer = _FakeIssuer(key_to_jwk_dict(rsa_private_key, "old"))
        jwks = issuer.jwks()
        asyncio.run(jwks.get_key("old"))

        issuer.rotate_to(key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 11

        assert asyncio.run(jwks.get_key("new")).key_id == "new"
        assert len(issuer.fetches) == 2

    def test_request_on_another_event_loop_waits_for_reload_in_flight(self, clock, rsa_private_key):
        issuer = _FakeIssuer(key_to_jwk_dict(rsa_private_key, "old"))
        jwks = issuer.jwks()
        asyncio.run(jwks.get_key("old"))

        issuer.rotate_to(key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 11
        issuer.hold_next_fetch()

        with ThreadPoolExecutor(max_workers=1) as other_thread:
            reload_on_other_loop = other_thread.submit(asyncio.run, jwks.get_key("new"))
            assert issuer.fetch_started.wait(timeout=5)

            key = asyncio.run(_get_key_while_fetch_held(jwks, "new", issuer))

            assert key.key_id == "new"
            assert reload_on_other_loop.result(timeout=5).key_id == "new"
        assert len(issuer.fetches) == 2

    async def test_cancelling_the_request_that_started_a_reload_does_not_cut_it_short(
        self, clock, rsa_private_key
    ):
        issuer = _FakeIssuer(key_to_jwk_dict(rsa_private_key, "old"))
        jwks = issuer.jwks()
        await jwks.get_key("old")

        issuer.rotate_to(key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 11
        issuer.hold_next_fetch()
        starting_request = asyncio.create_task(jwks.get_key("new"))
        await asyncio.to_thread(issuer.fetch_started.wait, 5)
        waiting_request = asyncio.create_task(jwks.get_key("new"))
        await asyncio.sleep(0)

        starting_request.cancel()
        issuer.release_fetch()

        assert (await waiting_request).key_id == "new"
        assert starting_request.cancelled()
        assert len(issuer.fetches) == 2

    def test_reload_cut_short_by_event_loop_shutdown_is_retried_by_waiting_request(
        self, clock, rsa_private_key
    ):
        issuer = _FakeIssuer(key_to_jwk_dict(rsa_private_key, "old"))
        jwks = issuer.jwks()
        asyncio.run(jwks.get_key("old"))

        issuer.rotate_to(key_to_jwk_dict(rsa_private_key, "new"))
        clock.now += 11
        issuer.hold_next_fetch()
        joined = threading.Event()

        async def request_joining_reload_on_loop_that_shuts_down(other_loop):
            request = asyncio.create_task(jwks.get_key("new"))
            await asyncio.sleep(0)
            joined.set()
            await asyncio.wrap_future(other_loop)
            return await request

        with ThreadPoolExecutor(max_workers=1) as other_thread:
            other_loop = other_thread.submit(
                asyncio.run, _start_reload_and_shut_down_once_joined(jwks, "new", joined)
            )
            assert issuer.fetch_started.wait(timeout=5)

            key = asyncio.run(request_joining_reload_on_loop_that_shuts_down(other_loop))

        assert key.key_id == "new"
        assert len(issuer.fetches) == 3


class TestJWKSDefaultAlgorithm:
    @pytest.mark.parametrize(
        ("default_algorithm", "published_alg", "expected_algorithm"),
        [
            pytest.param("RS512", None, "RS512", id="alg-less-key-gets-default"),
            pytest.param("RS512", "RS384", "RS384", id="published-alg-is-kept"),
            pytest.param(None, None, "RS256", id="no-default-keeps-key-type-default"),
        ],
    )
    async def test_key_algorithm(
        self, tmp_path: pathlib.Path, rsa_private_key, default_algorithm, published_alg, expected_algorithm
    ):
        jwk_dict = _jwk_without_alg(rsa_private_key, "kid")
        if published_alg:
            jwk_dict["alg"] = published_alg
        jwks_file = tmp_path / "jwks.json"
        _write_jwks(jwks_file, jwk_dict)

        jwks = JWKS(url=str(jwks_file), default_algorithm=default_algorithm)

        assert (await jwks.get_key("kid")).algorithm_name == expected_algorithm

    @pytest.mark.parametrize("default_algorithm", ["RS521", "none", "None"])
    def test_unusable_default_algorithm_is_rejected(self, default_algorithm):
        with pytest.raises(ValueError, match="JWKS default algorithm"):
            JWKS(url="https://example.com/jwks.json", default_algorithm=default_algorithm)

    async def test_alg_less_key_validates_token_of_default_algorithm(
        self, tmp_path: pathlib.Path, rsa_private_key
    ):
        jwks_file = tmp_path / "jwks.json"
        _write_jwks(jwks_file, _jwk_without_alg(rsa_private_key, "kid"))
        validator = JWTValidator(
            jwks=JWKS(url=str(jwks_file), default_algorithm="RS512"),
            issuer="http://test-issuer",
            algorithm="RS512",
            audience="abc",
        )
        generator = JWTGenerator(
            private_key=rsa_private_key,
            kid="kid",
            issuer="http://test-issuer",
            algorithm="RS512",
            audience="abc",
            valid_for=300,
        )

        claims = await validator.avalidated_claims(generator.generate())

        assert claims["iss"] == "http://test-issuer"


@conf_vars(
    {
        ("api_auth", "trusted_jwks_url"): "https://example.com/jwks.json",
        ("api_auth", "trusted_jwks_refresh_interval"): "600",
        ("api_auth", "trusted_jwks_refresh_retry_interval"): "15",
        ("api_auth", "trusted_jwks_default_algorithm"): "RS512",
    }
)
def test_sig_validation_args_configure_trusted_jwks():
    jwks = get_sig_validation_args()["jwks"]

    assert (jwks.refresh_interval_secs, jwks.refresh_retry_interval_secs, jwks.default_algorithm) == (
        600,
        15,
        "RS512",
    )


@conf_vars({("api_auth", "trusted_jwks_url"): "https://example.com/jwks.json"})
def test_sig_validation_args_trusted_jwks_defaults():
    jwks = get_sig_validation_args()["jwks"]

    assert (jwks.refresh_interval_secs, jwks.refresh_retry_interval_secs, jwks.default_algorithm) == (
        3600,
        10,
        None,
    )


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


def test_secret_key_token_includes_kid_in_header():
    """Symmetric (secret_key) tokens must include 'kid' in the JWT header so the validator accepts them."""
    generator = JWTGenerator(secret_key="test-secret", audience="test", valid_for=60)
    token = generator.generate({"sub": "user"})
    header = jwt.get_unverified_header(token)
    assert "kid" in header, "kid must always be present in the JWT header"
    assert header["kid"] == "not-used"


def test_secret_key_with_configured_kid():
    """When jwt_kid is configured, symmetric key generators should use it."""
    from unittest.mock import patch

    with patch.dict(
        "os.environ",
        {"AIRFLOW__API_AUTH__JWT_KID": "my-custom-kid"},
    ):
        generator = JWTGenerator(secret_key="test-secret", audience="test", valid_for=60)
        assert generator.kid == "my-custom-kid"
        token = generator.generate({"sub": "user"})
        header = jwt.get_unverified_header(token)
        assert header["kid"] == "my-custom-kid"


def test_generate_with_custom_valid_for():
    """generate() accepts a valid_for override."""
    generator = JWTGenerator(secret_key="test-secret", audience="test", valid_for=60)
    token = generator.generate(extras={"sub": "user"}, valid_for=3600)
    claims = jwt.decode(token, "test-secret", algorithms=["HS512"], audience="test")
    assert claims["exp"] - claims["iat"] == 3600


def test_generate_workload_scope_via_extras():
    """generate() with scope='workload' in extras produces a workload-scoped token."""
    generator = JWTGenerator(secret_key="test-secret", audience="test", valid_for=60)

    token = generator.generate(extras={"sub": "ti-123", "scope": "workload"}, valid_for=86400)
    claims = jwt.decode(token, "test-secret", algorithms=["HS512"], audience="test")
    assert claims["sub"] == "ti-123"
    assert claims["scope"] == "workload"
    assert claims["exp"] - claims["iat"] == 86400


def test_regular_token_has_no_scope():
    """Regular tokens without scope in extras have no scope claim."""
    generator = JWTGenerator(secret_key="test-secret", audience="test", valid_for=60)

    regular = generator.generate(extras={"sub": "user"})
    regular_claims = jwt.decode(regular, "test-secret", algorithms=["HS512"], audience="test")
    assert "scope" not in regular_claims


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
    await anyio.Path(jwks).write_text(jwk_content)

    priv_key = tmp_path.joinpath("key.pem")
    await anyio.Path(priv_key).write_bytes(key_to_pem(private_key))

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


@pytest.mark.parametrize("private_key", ["rsa_private_key", "ed25519_private_key"], indirect=True)
async def test_jwt_validate_roundtrip_with_jwks_and_guess_algorithm(private_key, tmp_path: pathlib.Path):
    jwk_content = json.dumps({"keys": [key_to_jwk_dict(private_key, "custom-kid")]})

    jwks = tmp_path.joinpath("jwks.json")
    await anyio.Path(jwks).write_text(jwk_content)

    priv_key = tmp_path.joinpath("key.pem")
    await anyio.Path(priv_key).write_bytes(key_to_pem(private_key))

    with conf_vars(
        {
            ("api_auth", "trusted_jwks_url"): str(jwks),
            ("api_auth", "jwt_kid"): "custom-kid",
            ("api_auth", "jwt_issuer"): "http://my-issuer.localdomain",
            ("api_auth", "jwt_private_key_path"): str(priv_key),
            ("api_auth", "jwt_algorithm"): "GUESS",
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


class TestRevokeToken:
    pytestmark = [pytest.mark.db_test]

    @pytest.fixture(autouse=True)
    def cleanup_revoked_tokens(self):
        from tests_common.test_utils.db import clear_db_revoked_tokens

        clear_db_revoked_tokens()
        yield
        clear_db_revoked_tokens()

    def test_revoke_token_persists_in_db(self):
        """Test that revoke_token validates the token and persists the jti in the database."""
        import time

        from airflow.models.revoked_token import RevokedToken

        now = int(time.time())
        payload = {
            "sub": "user",
            "jti": "revoke-test-jti",
            "exp": now + 3600,
            "iat": now,
            "nbf": now,
            "aud": "test",
        }
        token = jwt.encode(payload, "secret", algorithm="HS256")

        # Pass issuer=None explicitly so the validator does not pick up the
        # process-wide test-env default `[api_auth] jwt_issuer` and demand an
        # `iss` claim that the synthetic tokens below intentionally omit.
        validator = JWTValidator(
            secret_key="secret", audience="test", algorithm=["HS256"], leeway=0, issuer=None
        )
        validator.revoke_token(token)

        assert RevokedToken.is_revoked("revoke-test-jti") is True

    def test_revoke_token_without_jti_does_not_persist(self):
        """Test that a token without jti does not create a revoked token entry."""
        import time

        from airflow.models.revoked_token import RevokedToken

        now = int(time.time())
        payload = {"sub": "user", "exp": now + 3600, "iat": now, "nbf": now, "aud": "test"}
        token = jwt.encode(payload, "secret", algorithm="HS256")

        # Pass issuer=None explicitly so the validator does not pick up the
        # process-wide test-env default `[api_auth] jwt_issuer` and demand an
        # `iss` claim that the synthetic tokens below intentionally omit.
        validator = JWTValidator(
            secret_key="secret", audience="test", algorithm=["HS256"], leeway=0, issuer=None
        )
        validator.revoke_token(token)

        assert RevokedToken.is_revoked("any-jti") is False

    def test_revoke_token_with_invalid_token_does_not_raise(self):
        """Test that revoke_token logs a warning instead of raising for an invalid token."""
        from airflow.models.revoked_token import RevokedToken

        # Pass issuer=None explicitly so the validator does not pick up the
        # process-wide test-env default `[api_auth] jwt_issuer` and demand an
        # `iss` claim that the synthetic tokens below intentionally omit.
        validator = JWTValidator(
            secret_key="secret", audience="test", algorithm=["HS256"], leeway=0, issuer=None
        )
        validator.revoke_token("invalid-token")

        assert RevokedToken.is_revoked("any-jti") is False

    def test_revoke_token_with_db_error_does_not_raise(self):
        """Test that revoke_token handles database errors gracefully."""
        import time
        from unittest.mock import patch

        from sqlalchemy.exc import SQLAlchemyError

        now = int(time.time())
        payload = {
            "sub": "user",
            "jti": "db-error-jti",
            "exp": now + 3600,
            "iat": now,
            "nbf": now,
            "aud": "test",
        }
        token = jwt.encode(payload, "secret", algorithm="HS256")

        # Pass issuer=None explicitly so the validator does not pick up the
        # process-wide test-env default `[api_auth] jwt_issuer` and demand an
        # `iss` claim that the synthetic tokens below intentionally omit.
        validator = JWTValidator(
            secret_key="secret", audience="test", algorithm=["HS256"], leeway=0, issuer=None
        )
        with patch(
            "airflow.models.revoked_token.RevokedToken.revoke", side_effect=SQLAlchemyError("db down")
        ):
            validator.revoke_token(token)

    def test_revoke_token_db_error_is_logged_as_error(self):
        """A valid token that cannot be revoked stays usable, so the failure is logged as an error."""
        import time
        from unittest.mock import patch

        from sqlalchemy.exc import SQLAlchemyError

        now = int(time.time())
        payload = {
            "sub": "user",
            "jti": "db-error-jti",
            "exp": now + 3600,
            "iat": now,
            "nbf": now,
            "aud": "test",
        }
        token = jwt.encode(payload, "secret", algorithm="HS256")
        validator = JWTValidator(
            secret_key="secret", audience="test", algorithm=["HS256"], leeway=0, issuer=None
        )
        with (
            patch("airflow.api_fastapi.auth.tokens.log") as mock_log,
            patch("airflow.models.revoked_token.RevokedToken.revoke", side_effect=SQLAlchemyError("db down")),
        ):
            validator.revoke_token(token)

        mock_log.exception.assert_called_once()
        assert "db-error-jti" in mock_log.exception.call_args.args

    def test_revoke_token_invalid_token_is_not_logged_as_error(self):
        """A token that does not validate is already unusable, so it is only a warning."""
        from unittest.mock import patch

        validator = JWTValidator(
            secret_key="secret", audience="test", algorithm=["HS256"], leeway=0, issuer=None
        )
        with patch("airflow.api_fastapi.auth.tokens.log") as mock_log:
            validator.revoke_token("invalid-token")

        mock_log.exception.assert_not_called()
        mock_log.error.assert_not_called()
        mock_log.warning.assert_called_once()


@pytest.fixture(scope="session")
def rsa_private_key():
    return generate_private_key()


@pytest.fixture(scope="session")
def ed25519_private_key():
    return generate_private_key(key_type="Ed25519")
