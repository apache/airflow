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

"""Exchange an authenticated workload identity for a Dag processor session."""

from __future__ import annotations

from typing import Annotated

import jwt
import svcs
from cadwyn import VersionedAPIRouter
from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import ValidationError

from airflow.api_fastapi.auth.tokens import JWKSUnavailableError, JWTGenerator
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.execution_api.dag_processor_tokens import (
    ExpiredDagProcessorToken,
    generate_dag_processor_session_token,
    get_token_lifetime,
)
from airflow.api_fastapi.execution_api.datamodels.job import DagProcessorSessionTokenResponse
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.api_fastapi.execution_api.workload_identity import (
    DagProcessorIdentity,
    DagProcessorIdentityVerifier,
    WorkloadIdentityNotAllowed,
)

router = VersionedAPIRouter()
identity_bearer = HTTPBearer(auto_error=False, scheme_name="DagProcessorWorkloadIdentity")


async def require_workload_identity(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(identity_bearer)],
) -> DagProcessorIdentity:
    request.state.skip_execution_token_reissue = True
    verifier: DagProcessorIdentityVerifier | None = request.app.state.dag_processor_identity_verifier
    if verifier is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Workload identity provisioning is not enabled")
    if credentials is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "Workload identity required", headers={"WWW-Authenticate": "Bearer"}
        )
    try:
        return await verifier.authenticate(credentials.credentials)
    except JWKSUnavailableError:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Workload identity signing keys are unavailable",
            headers={"Retry-After": str(verifier.jwks.refresh_retry_interval_secs)},
        ) from None
    except (jwt.PyJWTError, ValidationError, TypeError, OverflowError):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED, "Invalid workload identity", headers={"WWW-Authenticate": "Bearer"}
        ) from None
    except WorkloadIdentityNotAllowed:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, "Workload identity is not granted any Dag bundles"
        ) from None


@router.post(
    "/dag-processor/session-token",
    responses=create_openapi_http_exception_doc(
        [
            (status.HTTP_401_UNAUTHORIZED, "The workload identity is missing, invalid, or expired"),
            (status.HTTP_403_FORBIDDEN, "The identity is not authorized as a Dag processor"),
            (status.HTTP_404_NOT_FOUND, "Workload identity provisioning is disabled"),
            (status.HTTP_503_SERVICE_UNAVAILABLE, "The identity provider's signing keys are unavailable"),
        ]
    ),
)
def exchange_session_token(
    response: Response,
    identity: Annotated[DagProcessorIdentity, Depends(require_workload_identity)],
    services: svcs.Container = DepContainer,
) -> DagProcessorSessionTokenResponse:
    generator: JWTGenerator = services.get(JWTGenerator)
    try:
        lifetime = get_token_lifetime(generator, identity.expires_at, "Workload identity has expired")
    except ExpiredDagProcessorToken as error:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(error)) from None
    token = generate_dag_processor_session_token(
        generator, session_id=identity.session_id, bundle_names=identity.bundle_names, valid_for=lifetime
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return DagProcessorSessionTokenResponse(token=token, expires_in=lifetime)
