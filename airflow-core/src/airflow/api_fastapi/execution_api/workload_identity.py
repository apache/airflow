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

"""Authenticate deployment identities for Dag processor session provisioning."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx2
import jwt
import structlog
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from airflow.api_fastapi.auth.tokens import JWKS, JWTValidator
from airflow.api_fastapi.execution_api.datamodels.token import DagBundleGrant
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import get_configured_bundle_names

log = structlog.get_logger(logger_name=__name__)

NonemptyString = Annotated[str, Field(min_length=1)]


class WorkloadIdentityConfig(BaseModel):
    """Trust settings owned by the deployment manager, never supplied by a request."""

    model_config = ConfigDict(extra="forbid")

    issuer: NonemptyString
    jwks_url: HttpUrl
    audience: NonemptyString
    algorithm: Literal["RS256", "ES256", "EdDSA"] = "RS256"
    instance_id_claim: list[NonemptyString] = Field(min_length=1)
    grants: dict[NonemptyString, DagBundleGrant] = Field(min_length=1)

    @field_validator("jwks_url")
    @classmethod
    def require_https(cls, value: HttpUrl) -> HttpUrl:
        if value.scheme != "https" or value.username or value.password or value.fragment:
            raise ValueError(
                "The workload identity JWKS URL must use HTTPS without credentials or a fragment"
            )
        return value


class WorkloadIdentityNotAllowed(PermissionError):
    """The verified identity has no Dag processor grant."""


@dataclass(frozen=True)
class DagProcessorIdentity:
    """Verified identity with server-selected grants and a stable, non-secret session ID."""

    session_id: UUID
    bundle_names: frozenset[str]
    expires_at: float


class _IdentityClaims(BaseModel):
    sub: NonemptyString
    exp: float = Field(allow_inf_nan=False)


class DagProcessorIdentityVerifier:
    """Validate external JWTs independently of Airflow's runtime token trust."""

    def __init__(self, config: WorkloadIdentityConfig, *, client: httpx2.AsyncClient):
        if config.audience in conf.getlist("execution_api", "jwt_audience"):
            raise ValueError("Workload identity must use a dedicated audience")
        configured = get_configured_bundle_names()
        # Not fatal: a renamed bundle must not stop the whole API server, only the subjects granted it.
        self._unconfigured_grants = {
            subject: missing
            for subject, bundles in config.grants.items()
            if (missing := bundles - configured)
        }
        if self._unconfigured_grants:
            log.error(
                "[execution_api] dag_processor_workload_identity grants Dag bundles missing from "
                "[dag_processor] dag_bundle_config_list; these subjects cannot obtain a session",
                unconfigured_grants={
                    subject: sorted(missing) for subject, missing in self._unconfigured_grants.items()
                },
            )
        self.config = config
        self.jwks = JWKS(
            url=str(config.jwks_url), client=client, refresh_on_unknown_kid=True, raise_on_fetch_error=True
        )
        self.validator = JWTValidator(
            jwks=self.jwks,
            issuer=config.issuer,
            audience=config.audience,
            algorithm=[config.algorithm],
            required_claims=frozenset({"iss", "sub", "aud", "exp", "iat"}),
        )

    async def authenticate(self, token: str) -> DagProcessorIdentity:
        try:
            claims = await self.validator.avalidated_claims(token)
        except (TypeError, OverflowError) as error:
            # PyJWT before 2.15 raises these for non-numeric or infinite time claims instead of DecodeError.
            log.warning("Rejecting a workload identity with malformed time claims", exc_info=True)
            raise jwt.InvalidTokenError("Workload identity has malformed time claims") from error
        identity = _IdentityClaims.model_validate(claims)
        if not (grants := self.config.grants.get(identity.sub)):
            raise WorkloadIdentityNotAllowed("Workload identity is not granted any Dag bundles")
        if missing := self._unconfigured_grants.get(identity.sub):
            raise WorkloadIdentityNotAllowed(
                f"Workload identity is granted Dag bundles this API server does not have: {sorted(missing)}"
            )
        instance_id: object = claims
        for key in self.config.instance_id_claim:
            if not isinstance(instance_id, dict) or key not in instance_id:
                raise jwt.InvalidTokenError("Workload identity lacks its configured instance claim")
            instance_id = instance_id[key]
        if not isinstance(instance_id, str) or not instance_id:
            raise jwt.InvalidTokenError("Workload identity instance claim must be a nonempty string")
        # Bind to a signed replica identity, not a caller-supplied hostname or a shared service account.
        # Including grants retires an existing registration when its authorization changes.
        session_id = uuid5(
            NAMESPACE_URL,
            json.dumps(
                [
                    "airflow:dag-processor",
                    self.config.issuer,
                    self.config.audience,
                    identity.sub,
                    self.config.instance_id_claim,
                    instance_id,
                    sorted(grants),
                ],
                separators=(",", ":"),
            ),
        )
        return DagProcessorIdentity(session_id, grants, identity.exp)


@asynccontextmanager
async def create_dag_processor_identity_verifier() -> AsyncIterator[DagProcessorIdentityVerifier | None]:
    """Create one verifier per application lifespan and close its JWKS connection pool."""
    raw = conf.get("execution_api", "dag_processor_workload_identity", fallback="")
    if not raw:
        yield None
        return
    config = WorkloadIdentityConfig.model_validate_json(raw)
    async with httpx2.AsyncClient(timeout=5.0, follow_redirects=False) as client:
        yield DagProcessorIdentityVerifier(config, client=client)
