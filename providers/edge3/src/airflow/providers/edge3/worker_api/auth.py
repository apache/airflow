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

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.providers.common.compat.sdk import conf

log = logging.getLogger(__name__)


@cache
def jwt_validator() -> JWTValidator:
    return JWTValidator(
        secret_key=conf.get("api_auth", "jwt_secret"),
        leeway=conf.getint("api_auth", "jwt_leeway", fallback=30),
        audience="api",
    )


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


def jwt_token_authorization(method: str, authorization: str) -> dict:
    """Check if the JWT token is correct and return the validated payload."""
    try:
        payload = jwt_validate(authorization)
        signed_method = payload.get("method")
        if not signed_method or signed_method != method:
            _forbidden_response(
                "Invalid method in token authorization. "
                f"signed method='{signed_method}' "
                f"called method='{method}'",
            )
        return payload
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
    # _forbidden_response always raises; this is unreachable but keeps type checkers happy.
    return {}


def jwt_token_authorization_rest(
    request: Request, authorization: str = Header(description="JWT Authorization Token")
) -> dict:
    """
    Check the JWT for a REST request; return the validated payload.

    Routes can capture the return value via ``Depends(jwt_token_authorization_rest)``
    to read claims (e.g. ``team_name``) that the server must trust over any
    matching field in the request body. The payload is returned as a plain dict.
    """
    PREFIX = "/edge_worker/v1/"
    path = request.url.path
    method_path = path[path.find(PREFIX) + len(PREFIX) :] if PREFIX in path else path
    return jwt_token_authorization(method_path, authorization)


def assert_jwt_team_matches_body(jwt_payload: dict, body_team_name: str | None) -> str | None:
    """
    Return the JWT-bound team_name; reject if the body claims a different team.

    The JWT is the source of truth for team membership: it was issued by the
    central site to a specific worker for a specific team. Any team_name in the
    request body is at most an echo of the local config; if it disagrees with
    the JWT we treat it as an attempt to cross team boundaries and reject the
    request with HTTP 403.

    Returns the JWT-bound team_name (which may be ``None`` for backwards-compat
    workers whose JWTs predate the team_name claim — in that case the body's
    team_name is used so older workers keep working).
    """
    jwt_team = jwt_payload.get("team_name")
    if jwt_team is None:
        # Backwards-compat: pre-team-claim JWTs. Fall back to body's value so an
        # in-flight upgrade does not lock workers out.
        return body_team_name
    if body_team_name is not None and body_team_name != jwt_team:
        _forbidden_response(
            "team_name in request body does not match JWT claim. "
            f"jwt_team='{jwt_team}' body_team='{body_team_name}'",
        )
    return jwt_team
