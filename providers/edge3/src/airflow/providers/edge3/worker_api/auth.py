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

import logging
from functools import cache
from typing import TYPE_CHECKING, TypedDict
from uuid import uuid4

from fastapi import Header, HTTPException, Request, status
from itsdangerous import BadSignature
from jwt import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidSignatureError,
)

from airflow.api_fastapi.auth.tokens import JWKS, JWTValidator
from airflow.providers.common.compat.sdk import AirflowConfigException, conf

if TYPE_CHECKING:
    from collections.abc import Callable

log = logging.getLogger(__name__)


class WorkerTokenAuthorization(TypedDict, total=False):
    """
    Result of authorizing an OIDC worker token beyond signature verification.

    Returned by a ``[edge] jwt_verifier`` callable to answer "may this token act
    as an edge worker?". ``authorized`` must be ``True`` for the request to
    proceed; a falsy result (or a raised exception) rejects it.
    """

    authorized: bool


def _default_jwt_verifier(claims: dict) -> WorkerTokenAuthorization:
    """Authorize any token that passed signature, issuer and audience verification."""
    return {"authorized": True}


def _trusted_jwks_url() -> str:
    """Return the configured trusted JWKS URL, or an empty string when unset."""
    return conf.get("edge", "trusted_jwks_url", fallback="") or ""


def _jwt_algorithms() -> list[str]:
    """Return the accepted signing algorithms for OIDC worker tokens."""
    configured = conf.get("edge", "jwt_algorithm", fallback="RS256") or "RS256"
    return [algorithm.strip() for algorithm in configured.split(",") if algorithm.strip()]


def _jwt_audience() -> str | None:
    """Return the configured audience, or None to accept only tokens without an ``aud`` claim."""
    return conf.get("edge", "jwt_audience", fallback="") or None


def _jwt_issuer() -> str | None:
    """Return the expected issuer, or None to skip issuer verification when left empty."""
    return conf.get("edge", "jwt_issuer", fallback="") or None


def _jwt_leeway() -> int:
    """Return the clock-skew leeway (seconds) for OIDC worker tokens."""
    return conf.getint("edge", "jwt_leeway", fallback=30)


def _jwt_verifier() -> Callable[[dict], WorkerTokenAuthorization | None]:
    """Return the configured worker-authorization callable, or the permissive default."""
    return conf.getimport("edge", "jwt_verifier", fallback=None) or _default_jwt_verifier


def _jwt_verifier_configured() -> bool:
    """Return whether an explicit ``[edge] jwt_verifier`` is set."""
    return bool(conf.get("edge", "jwt_verifier", fallback=""))


@cache
def _oidc_enabled() -> bool:
    """
    Return whether OIDC verification is enabled, decided once and cached.

    The validator is also cached, so the mode must be read from a single place;
    otherwise a request-time re-read could disagree with the cached validator and
    skip the ``method``-claim check for a shared-secret token.
    """
    return bool(_trusted_jwks_url())


def _shared_secret_validator() -> JWTValidator:
    """Build a validator for worker tokens signed with the shared ``[api_auth] jwt_secret``."""
    return JWTValidator(
        secret_key=conf.get("api_auth", "jwt_secret"),
        leeway=conf.getint("api_auth", "jwt_leeway", fallback=30),
        audience="api",
    )


def _oidc_validator(jwks_url: str) -> JWTValidator:
    """
    Build a validator for worker tokens issued by a trusted OIDC provider.

    Verifies the token signature against the provider JWKS and checks the
    ``iss`` and (optionally) ``aud`` claims. Used when ``[edge] trusted_jwks_url``
    is configured, so workers can authenticate with tokens minted by an
    external identity provider instead of the shared secret.

    Rejects the configuration when issuer verification is skipped (empty
    ``jwt_issuer``) without a ``jwt_verifier``: that combination would accept any
    token signed by a key in the JWKS. The validator is built lazily on the first
    request, so this surfaces as a rejected request (403) rather than a startup
    failure.
    """
    if not _jwt_issuer() and not _jwt_verifier_configured():
        raise AirflowConfigException(
            "[edge] jwt_verifier must be set when trusted_jwks_url is configured "
            "without jwt_issuer, otherwise any token signed by the JWKS is accepted."
        )
    return JWTValidator(
        jwks=JWKS(url=jwks_url),
        issuer=_jwt_issuer(),
        audience=_jwt_audience(),
        algorithm=_jwt_algorithms(),
        required_claims=frozenset({"iat", "exp"}),
        leeway=_jwt_leeway(),
    )


@cache
def jwt_validator() -> JWTValidator:
    if _oidc_enabled():
        return _oidc_validator(_trusted_jwks_url())
    return _shared_secret_validator()


def jwt_validate(authorization: str) -> dict:
    return jwt_validator().validated_claims(authorization)


def _check_worker_authorization(payload: dict) -> None:
    """
    Verify the token identity is allowed to act as an edge worker in OIDC mode.

    A valid signature proves who signed the token, not that the identity may act
    as a worker. The configured ``[edge] jwt_verifier`` answers the second
    question; a falsy result rejects the request.
    """
    if not _oidc_enabled():
        return

    result = _jwt_verifier()(payload)
    if not result or not result.get("authorized"):
        _forbidden_response("Token is not authorized to act as an edge worker.")


def _forbidden_response(message: str):
    """Log the error and return the response anonymized."""
    error_id = uuid4()
    log.exception("%s error_id=%s", message, error_id)
    raise HTTPException(
        status.HTTP_403_FORBIDDEN,
        f"Forbidden. The server side traceback may be identified with error_id={error_id}",
    )


def _check_method_claim(method: str, payload: dict) -> None:
    """
    Verify the signed ``method`` claim for shared-secret tokens.

    Tokens minted by the Edge API carry the request ``method`` they are valid
    for. Tokens issued by an external OIDC provider do not, so the check is
    skipped when OIDC verification is enabled.
    """
    if _oidc_enabled():
        return

    signed_method = payload.get("method")
    if not signed_method or signed_method != method:
        _forbidden_response(
            "Invalid method in token authorization. "
            f"signed method='{signed_method}' "
            f"called method='{method}'",
        )


def jwt_token_authorization(method: str, authorization: str):
    """Check if the JWT token is correct."""
    try:
        payload = jwt_validate(authorization)
        _check_method_claim(method, payload)
        _check_worker_authorization(payload)
    except BadSignature:
        _forbidden_response("Bad Signature. Please use only the tokens provided by the API.")
    except InvalidAudienceError:
        _forbidden_response("Invalid audience for the request")
    except InvalidSignatureError:
        _forbidden_response("The signature of the request was wrong")
    except ImmatureSignatureError:
        _forbidden_response("The signature of the request was sent from the future")
    except ExpiredSignatureError:
        _forbidden_response(
            "The signature of the request has expired. Make sure that all components "
            "in your system have synchronized clocks.",
        )
    except InvalidIssuedAtError:
        _forbidden_response(
            "The request was issues in the future. Make sure that all components "
            "in your system have synchronized clocks.",
        )
    except Exception:
        _forbidden_response("Unable to authenticate API via token.")


def jwt_token_authorization_rest(
    request: Request, authorization: str = Header(description="JWT Authorization Token")
):
    """Check if the JWT token is correct for REST API requests."""
    PREFIX = "/edge_worker/v1/"
    path = request.url.path
    method_path = path[path.find(PREFIX) + len(PREFIX) :] if PREFIX in path else path
    jwt_token_authorization(method_path, authorization)
