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

from itsdangerous import BadSignature
from jwt import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAudienceError,
    InvalidIssuedAtError,
    InvalidSignatureError,
)

from airflow.configuration import conf
from airflow.providers.edge3.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.edge3.worker_api.datamodels import JsonRpcRequestBase  # noqa: TCH001
from airflow.providers.edge3.worker_api.routes._v2_compat import (
    Header,
    HTTPException,
    Request,
    status,
)

log = logging.getLogger(__name__)


if AIRFLOW_V_3_0_PLUS:
    from airflow.api_fastapi.auth.tokens import JWTValidator

    @cache
    def jwt_validator() -> JWTValidator:
        clock_grace = conf.getint("core", "internal_api_clock_grace", fallback=30)
        return JWTValidator(
            secret_key=conf.get("core", "internal_api_secret_key"),
            leeway=clock_grace,
            audience="api",
        )

    def jwt_validate(authorization: str) -> dict:
        return jwt_validator().validated_claims(authorization)

else:
    # Airflow 2.10 compatibility
    from airflow.utils.jwt_signer import JWTSigner  # type: ignore

    @cache
    def jwt_signer() -> JWTSigner:
        clock_grace = conf.getint("core", "internal_api_clock_grace", fallback=30)
        return JWTSigner(
            secret_key=conf.get("core", "internal_api_secret_key"),
            expiration_time_in_seconds=clock_grace,
            leeway_in_seconds=clock_grace,
            audience="api",
        )

    def jwt_validate(authorization: str) -> dict:
        return jwt_signer().verify_token(authorization)


def _forbidden_response(message: str):
    """Log the error and return the response anonymized."""
    error_id = uuid4()
    log.exception("%s error_id=%s", message, error_id)
    raise HTTPException(
        status.HTTP_403_FORBIDDEN,
        f"Forbidden. The server side traceback may be identified with error_id={error_id}",
    )


def jwt_token_authorization(method: str, authorization: str):
    """Check if the JWT token is correct."""
    try:
        payload = jwt_validate(authorization)
        signed_method = payload.get("method")
        if not signed_method or signed_method != method:
            _forbidden_response(
                "Invalid method in token authorization. "
                f"signed method='{signed_method}' "
                f"called method='{method}'",
            )
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


def jwt_token_authorization_rpc(
    body: JsonRpcRequestBase, authorization: str = Header(description="JWT Authorization Token")
):
    """Check if the JWT token is correct for JSON PRC requests."""
    jwt_token_authorization(body.method, authorization)


def jwt_token_authorization_rest(
    request: Request, authorization: str = Header(description="JWT Authorization Token")
):
    """Check if the JWT token is correct for REST API requests."""
    PREFIX = "/edge_worker/v1/"
    path = request.url.path
    method_path = path[path.find(PREFIX) + len(PREFIX) :] if PREFIX in path else path
    jwt_token_authorization(method_path, authorization)
