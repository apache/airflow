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
import os
import time
import uuid
from base64 import urlsafe_b64encode
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, overload

import attrs
import httpx
import jwt
import structlog
from asgiref.sync import async_to_sync
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from airflow._shared.timezones import timezone

if TYPE_CHECKING:
    from jwt.algorithms import AllowedKeys, AllowedPrivateKeys

log = structlog.get_logger(logger_name=__name__)

__all__ = [
    "InvalidClaimError",
    "JWKS",
    "JWTGenerator",
    "JWTValidator",
    "generate_private_key",
    "get_sig_validation_args",
    "get_signing_args",
    "get_signing_key",
    "key_to_pem",
    "key_to_jwk_dict",
]


class InvalidClaimError(ValueError):
    """Raised when a claim in the JWT is invalid."""

    def __init__(self, claim: str):
        super().__init__(f"Invalid claim: {claim}")


def key_to_jwk_dict(key: AllowedKeys, kid: str | None = None):
    """Convert a public or private key into a valid JWKS dict."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
    from jwt.algorithms import OKPAlgorithm, RSAAlgorithm

    if isinstance(key, (RSAPrivateKey, Ed25519PrivateKey)):
        key = key.public_key()

    if isinstance(key, RSAPublicKey):
        jwk_dict = RSAAlgorithm(RSAAlgorithm.SHA256).to_jwk(key, as_dict=True)

    elif isinstance(key, Ed25519PublicKey):
        jwk_dict = OKPAlgorithm().to_jwk(key, as_dict=True)
    else:
        raise ValueError(f"Unknown key object {type(key)}")

    if not kid:
        kid = thumbprint(jwk_dict)

    jwk_dict["kid"] = kid

    return jwk_dict


def _guess_best_algorithm(key: AllowedPrivateKeys):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

    if isinstance(key, RSAPrivateKey):
        return "RS512"
    if isinstance(key, Ed25519PrivateKey):
        return "EdDSA"
    raise ValueError(f"Unknown key object {type(key)}")


@attrs.define(repr=False)
class JWKS:
    """A class to fetch and sync a set of JSON Web Keys."""

    url: str
    fetched_at: float = 0
    last_fetch_attempt_at: float = 0

    client: httpx.AsyncClient = attrs.field(factory=httpx.AsyncClient)

    _jwks: jwt.PyJWKSet | None = None
    refresh_jwks: bool = True
    refresh_interval_secs: int = 3600
    refresh_retry_interval_secs: int = 10

    def __repr__(self) -> str:
        return f"JWKS(url={self.url}, fetched_at={self.fetched_at})"

    @classmethod
    def from_private_key(cls, *keys: AllowedPrivateKeys | tuple[AllowedPrivateKeys, str]):
        obj = cls(url=os.devnull)
        keyset = [
            # Each `key` is either the key directly or `(key, "my-kid")`
            key_to_jwk_dict(*key) if isinstance(key, tuple) else key_to_jwk_dict(key)
            for key in keys
        ]
        obj._jwks = jwt.PyJWKSet(keyset)
        return obj

    async def fetch_jwks(self) -> None:
        if not self._should_fetch_jwks():
            return
        if self.url.startswith("http"):
            data = await self._fetch_remote_jwks()
        else:
            data = self._fetch_local_jwks()

        if not data:
            return

        self._jwks = jwt.PyJWKSet.from_dict(data)
        log.debug("Fetched JWKS", url=self.url, keys=len(self._jwks.keys))

    async def _fetch_remote_jwks(self) -> dict[str, Any] | None:
        try:
            log.debug(
                "Fetching JWKS",
                url=self.url,
                last_fetched_secs_ago=int(time.monotonic() - self.fetched_at) if self.fetched_at else None,
            )
            if TYPE_CHECKING:
                assert self.url
            self.last_fetch_attempt_at = int(time.monotonic())
            response = await self.client.get(self.url)
            response.raise_for_status()
            self.fetched_at = int(time.monotonic())
            await response.aread()
            await response.aclose()
            return response.json()
        except Exception:
            log.exception("Failed to fetch remote JWKS", url=self.url)
            return None

    def _fetch_local_jwks(self) -> dict[str, Any] | None:
        try:
            with open(self.url) as jwks_file:
                content = json.load(jwks_file)
            self.fetched_at = int(time.monotonic())
            return content
        except Exception:
            log.exception("Failed to read local JWKS", url=self.url)
            return None

    def _should_fetch_jwks(self) -> bool:
        """
        Check if we need to fetch the JWKS based on the last fetch time and the refresh interval.

        If the JWKS URL is local, we only fetch it once. For remote JWKS URLs we fetch it based
        on the refresh interval if refreshing has been enabled with a minimum interval between
        attempts. The fetcher functions set the fetched_at timestamp to the current monotonic time
        when the JWKS is fetched.
        """
        if not self.url.startswith("http"):
            # Fetch local JWKS only if not already loaded
            # This could be improved in future by looking at mtime of file.
            return not self._jwks
        # For remote fetches we check if the JWKS is not loaded (fetched_at = 0) or if the last fetch was more than
        # refresh_interval_secs ago and the last fetch attempt was more than refresh_retry_interval_secs ago
        now = time.monotonic()
        return self.refresh_jwks and (
            not self._jwks
            or (
                self.fetched_at == 0
                or (
                    now - self.fetched_at > self.refresh_interval_secs
                    and now - self.last_fetch_attempt_at > self.refresh_retry_interval_secs
                )
            )
        )

    async def get_key(self, kid: str) -> jwt.PyJWK:
        """Fetch the JWKS and find the matching key for the token."""
        await self.fetch_jwks()

        if self._jwks:
            return self._jwks[kid]

        # It didn't load!
        raise KeyError(f"Key ID {kid} not found in keyset")

    def status(self):
        # https://svcs.hynek.me/en/stable/core-concepts.html#health-checks
        if not self._should_fetch_jwks():
            # Up-to-date, we are healthy
            return

        if self.fetched_at == 0:
            raise RuntimeError("JWKS never fetched")

        last_successful_fetch = time.monotonic() - self.fetched_at
        if last_successful_fetch > 3 * self.refresh_interval_secs:
            raise RuntimeError(f"JWKS last fetched {last_successful_fetch}s ago")


def _conf_factory(section, key, **kwargs):
    def factory() -> str:
        from airflow.configuration import conf

        return conf.get(section, key, **kwargs, suppress_warnings=True)

    return factory


@overload
def _conf_list_factory(section, key, first_only: Literal[True], **kwargs) -> Callable[[], str]: ...


@overload
def _conf_list_factory(
    section, key, first_only: Literal[False] = False, **kwargs
) -> Callable[[], list[str]]: ...


def _conf_list_factory(section, key, first_only: bool = False, **kwargs):
    def factory() -> list[str] | str:
        from airflow.configuration import conf

        val = conf.getlist(section, key, **kwargs, suppress_warnings=True)

        if first_only and val:
            return val[0]
        return val or []

    return factory


def _to_list(val: str | list[str]) -> list[str]:
    if isinstance(val, str):
        val = [val]
    return val


@attrs.define(kw_only=True)
class JWTValidator:
    """
    Validate the claims and validitory of a JWT.

    This will either validate the JWT is signed with the symmetric key if ``secret_key`` is passed, or else
    that it is signed by one of the public keys in the keyset in ``jwks`` attribute.
    """

    jwks: JWKS | None = None
    secret_key: str | None = attrs.field(repr=False, default=None, converter=lambda v: None if v == "" else v)
    issuer: str | list[str] | None = attrs.field(
        factory=_conf_list_factory("api_auth", "jwt_issuer", fallback=None),
        # Ensure we have None, instead of an empty list, else pyjwt will fail to validate it
        converter=lambda v: None if v == [] else v,
    )
    # By default, we just validate these
    required_claims: frozenset[str] = frozenset({"exp", "iat", "nbf"})
    audience: str | Sequence[str]
    algorithm: list[str] = attrs.field(
        factory=_conf_list_factory("api_auth", "jwt_algorithm", fallback="GUESS"), converter=_to_list
    )

    leeway: float = attrs.field(factory=_conf_factory("api_auth", "jwt_leeway"), converter=int)

    def __attrs_post_init__(self):
        if not (self.jwks is None) ^ (self.secret_key is None):
            raise ValueError("Exactly one of private_key and secret_key must be specified")

        if self.algorithm == ["GUESS"]:
            if self.jwks:
                # TODO: We could probably populate this from the jwks document, but we don't have that at
                # construction time.
                raise ValueError(
                    "Cannot guess the algorithm when using JWKS - please specify it in the config option "
                    "[api_auth] jwt_algorithm"
                )
            self.algorithm = ["HS512"]

    def _get_kid_from_header(self, unvalidated: str) -> str:
        header = jwt.get_unverified_header(unvalidated)
        if "kid" not in header:
            raise jwt.InvalidTokenError("Missing 'kid' in token header")
        return header["kid"]

    async def _get_validation_key(self, unvalidated: str) -> str | jwt.PyJWK:
        if self.secret_key:
            return self.secret_key

        if TYPE_CHECKING:
            assert self.jwks is not None

        kid = self._get_kid_from_header(unvalidated)
        return await self.jwks.get_key(kid)

    def validated_claims(
        self, unvalidated: str, required_claims: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return async_to_sync(self.avalidated_claims)(unvalidated, required_claims)

    async def avalidated_claims(
        self, unvalidated: str, required_claims: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Decode the JWT token, returning the validated claims or raising an exception."""
        key = await self._get_validation_key(unvalidated)
        claims = jwt.decode(
            unvalidated,
            key,
            audience=self.audience,
            issuer=self.issuer,
            options={"require": self.required_claims},
            algorithms=self.algorithm,
            leeway=self.leeway,
        )

        # Validate additional claims if provided
        if required_claims:
            for claim, expected_value in required_claims.items():
                if expected_value["essential"] and (
                    claim not in claims or claims[claim] != expected_value["value"]
                ):
                    raise InvalidClaimError(claim)

        return claims

    def status(self):
        if self.jwks:
            self.jwks.status()


def _pem_to_key(pem_data: str | bytes | AllowedPrivateKeys) -> AllowedPrivateKeys:
    if isinstance(pem_data, str):
        pem_data = pem_data.encode()
    elif not isinstance(pem_data, bytes):
        # Assume it's already a key object
        return pem_data

    return load_pem_private_key(pem_data, password=None)  # type: ignore[return-value]


def _load_key_from_configured_file() -> AllowedPrivateKeys | None:
    from airflow.configuration import conf

    path = conf.get("api_auth", "jwt_private_key_path", fallback=None)
    if not path:
        return None

    with open(path, mode="rb") as fh:
        return _pem_to_key(fh.read())


def _generate_kid(gen) -> str:
    if not gen._private_key:
        return "not-used"

    if kid := _conf_factory("api_auth", "jwt_kid", fallback=None)():
        return kid

    # Generate it from the thumbprint of the private key
    info = key_to_jwk_dict(gen._private_key)
    return info["kid"]


@attrs.define(repr=False, kw_only=True)
class JWTGenerator:
    """Generate JWT tokens."""

    _private_key: AllowedPrivateKeys | None = attrs.field(
        repr=False, alias="private_key", converter=_pem_to_key, factory=_load_key_from_configured_file
    )
    """
    Private key to sign generated tokens.

    Should be either a private key object from the cryptography module, or a PEM-encoded byte string
    """
    _secret_key: str | None = attrs.field(
        repr=False,
        alias="secret_key",
        default=None,
        converter=lambda v: None if v == "" else v,
    )
    """A pre-shared secret key to sign tokens with symmetric encryption"""

    kid: str = attrs.field(default=attrs.Factory(_generate_kid, takes_self=True))
    valid_for: float
    audience: str
    issuer: str | list[str] | None = attrs.field(
        factory=_conf_list_factory("api_auth", "jwt_issuer", first_only=True, fallback=None)
    )
    algorithm: str = attrs.field(
        factory=_conf_list_factory("api_auth", "jwt_algorithm", first_only=True, fallback="GUESS")
    )

    def __attrs_post_init__(self):
        if not (self._private_key is None) ^ (self._secret_key is None):
            raise ValueError("Exactly one of private_key and secret_key must be specified")

        if self.algorithm == "GUESS":
            if self._private_key:
                self.algorithm = _guess_best_algorithm(self._private_key)
            else:
                self.algorithm = "HS512"

    @property
    def signing_arg(self) -> AllowedPrivateKeys | str:
        if callable(self._private_key):
            return self._private_key()
        if self._private_key:
            return self._private_key
        if TYPE_CHECKING:
            # Already handled at in post_init
            assert self._secret_key
        return self._secret_key

    def generate(self, extras: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> str:
        """Generate a signed JWT for the subject."""
        now = int(datetime.now(tz=timezone.utc).timestamp())
        claims = {
            "jti": uuid.uuid4().hex,
            "iss": self.issuer,
            "aud": self.audience,
            "nbf": now,
            "exp": int(now + self.valid_for),
            "iat": now,
        }

        if claims["iss"] is None:
            del claims["iss"]
        if claims["aud"] is None:
            del claims["aud"]

        if extras is not None:
            claims = extras | claims
        headers = {"alg": self.algorithm, **(headers or {})}
        if self._private_key:
            headers["kid"] = self.kid
        return jwt.encode(claims, self.signing_arg, algorithm=self.algorithm, headers=headers)


def generate_private_key(key_type: str = "RSA", key_size: int = 2048):
    """
    Generate a valid private key for testing.

    Args:
        key_type (str): Type of key to generate. Can be "RSA" or "Ed25516". Defaults to "RSA".
        key_size (int): Size of the key in bits. Only applicable for RSA keys. Defaults to 2048.

    Returns:
        tuple: A tuple containing the private key in PEM format and the corresponding public key in PEM format.
    """
    from cryptography.hazmat.primitives.asymmetric import ed25519, rsa

    if key_type == "RSA":
        # Generate an RSA private key

        return rsa.generate_private_key(public_exponent=65537, key_size=key_size, backend=default_backend())
    if key_type == "Ed25519":
        return ed25519.Ed25519PrivateKey.generate()
    raise ValueError(f"unsupported key type: {key_type}")


def key_to_pem(key: AllowedPrivateKeys) -> bytes:
    from cryptography.hazmat.primitives import serialization

    # Serialize the private key in PEM format
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def thumbprint(jwk: dict[str, Any], hashalg=hashes.SHA256()) -> str:
    """
    Return the key thumbprint as specified by RFC 7638.

    :param hashalg: A hash function (defaults to SHA256)

    :return: A base64url encoded digest of the key
    """
    digest = hashes.Hash(hashalg, backend=default_backend())
    jsonstr = json.dumps(jwk, separators=(",", ":"), sort_keys=True)
    digest.update(jsonstr.encode("utf8"))
    return base64url_encode(digest.finalize())


def base64url_encode(payload):
    if not isinstance(payload, bytes):
        payload = payload.encode("utf-8")
    encode = urlsafe_b64encode(payload)
    return encode.decode("utf-8").rstrip("=")


def get_signing_key(section: str, key: str, make_secret_key_if_needed: bool = True) -> str:
    """
    Get a signing shared key from the config.

    If the config option is empty this will generate a random one and warn about the lack of it.
    """
    from airflow.configuration import conf

    sentinel = object()
    secret_key = conf.get(section, key, fallback=sentinel)

    if not secret_key or secret_key is sentinel:
        if make_secret_key_if_needed:
            log.warning(
                "`%s/%s` was empty, using a generated one for now. Please set this in your config",
                section,
                key,
            )
            secret_key = base64url_encode(os.urandom(16))
            # Set it back so any other callers get the same value for the duration of this process
            conf.set(section, key, secret_key)
        else:
            raise ValueError(f"The value {section}/{key} must be set!")

    # Mypy can't grock the `if not secret_key`
    return secret_key


def get_signing_args(make_secret_key_if_needed: bool = True) -> dict[str, Any]:
    """
    Return the args to splat into JWTGenerator for private or secret key.

    Will use ``get_signing_key`` to generate a key if nothing else suitable is found.
    """
    # Try private key first
    priv = _load_key_from_configured_file()

    if priv is not None:
        return {"private_key": priv}

    # Don't call this unless we have to as it might issue a warning
    return {"secret_key": get_signing_key("api_auth", "jwt_secret", make_secret_key_if_needed)}


def get_sig_validation_args(make_secret_key_if_needed: bool = True) -> dict[str, Any]:
    from airflow.configuration import conf

    sentinel = object()

    # Try JWKS url first
    url = conf.get("api_auth", "trusted_jwks_url", fallback=sentinel)

    if url and url is not sentinel:
        jwks = JWKS(url=url)
        return {"jwks": jwks}

    key = _load_key_from_configured_file()

    if key is not None:
        jwks = JWKS.from_private_key(key)
        return {
            "jwks": jwks,
            "algorithm": conf.get("api_auth", "jwt_algorithm", fallback=None) or _guess_best_algorithm(key),
        }

    return {"secret_key": get_signing_key("api_auth", "jwt_secret", make_secret_key_if_needed)}
