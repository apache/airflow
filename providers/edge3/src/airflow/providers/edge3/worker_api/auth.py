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
from typing import cast
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
from airflow.providers.common.compat.sdk import conf

log = logging.getLogger(__name__)


def _trusted_jwks_url() -> str:
    """Return the configured trusted JWKS URL, or an empty string when unset."""
    return conf.get("edge", "trusted_jwks_url", fallback="") or ""


def _jwt_algorithms() -> list[str]:
    """Return the accepted signing algorithms for OIDC worker tokens."""
    configured = conf.get("edge", "jwt_algorithm", fallback="RS256") or "RS256"
    return [algorithm.strip() for algorithm in configured.split(",") if algorithm.strip()]


def _jwt_audience() -> str | None:
    """Return the expected token audience, or None to skip audience verification."""
    return conf.get("edge", "jwt_audience", fallback="") or None


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
    """
    return JWTValidator(
        jwks=JWKS(url=jwks_url),
        issuer=conf.get("edge", "jwt_issuer", fallback=None),
        audience=cast("str", _jwt_audience()),
        algorithm=_jwt_algorithms(),
        required_claims=frozenset({"iat", "exp"}),
        leeway=conf.getint("api_auth", "jwt_leeway", fallback=30),
    )


@cache
def jwt_validator() -> JWTValidator:
    jwks_url = _trusted_jwks_url()
    if jwks_url:
        return _oidc_validator(jwks_url)
    return _shared_secret_validator()


def jwt_validate(authorization: str) -> dict:
    return jwt_validator().validated_claims(authorization)


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
    if _trusted_jwks_url():
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
