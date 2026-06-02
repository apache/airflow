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
"""Authentication for the DAG Processing API (AIP-92)."""

from __future__ import annotations

import functools
import logging

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from airflow.api_fastapi.auth.tokens import JWTValidator, get_sig_validation_args
from airflow.configuration import conf

log = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _token_validator() -> JWTValidator:
    """
    Build the JWT validator for the DAG Processing API.

    ``get_sig_validation_args`` selects symmetric (deployment ``jwt_secret``) or asymmetric/JWKS
    validation (``[api_auth] trusted_jwks_url``) the same way the Execution API does, so a
    multi-tenant deployment validates externally-issued tokens without any change here.
    """
    required_claims = frozenset({"aud", "exp", "iat"})
    issuer = conf.get("api_auth", "jwt_issuer", fallback=None)
    if issuer:
        required_claims = required_claims | {"iss"}
    return JWTValidator(
        required_claims=required_claims,
        issuer=issuer,
        audience=conf.get_mandatory_list_value("dag_processor", "jwt_audience"),
        **get_sig_validation_args(make_secret_key_if_needed=False),
    )


_bearer = HTTPBearer(auto_error=False)


def require_dag_processing_auth(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> None:
    """
    Reject DAG Processing API calls that lack a valid DAG-processor token.

    The DAG processor self-signs a token for the configured ``[dag_processor] jwt_audience`` with
    the deployment signing key; this validates the signature, audience, and issuer so the
    persistence endpoints are not callable unauthenticated.
    """
    if creds is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing auth token")
    try:
        _token_validator().validated_claims(creds.credentials)
    except Exception:
        log.warning("Invalid DAG Processing API token", exc_info=True)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid auth token")
