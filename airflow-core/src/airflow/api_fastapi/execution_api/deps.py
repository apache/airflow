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

# Disable future annotations in this file to work around https://github.com/fastapi/fastapi/issues/13056
# ruff: noqa: I002

from typing import Any

import structlog
import svcs
from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, SecurityScopes
from sqlalchemy import select

from airflow.api_fastapi.auth.tokens import (
    SCOPE_EXECUTION,
    SCOPE_MAPPING,
    SCOPE_WORKLOAD,
    JWTValidator,
)
from airflow.api_fastapi.common.db.common import AsyncSessionDep
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.configuration import conf
from airflow.models import DagModel, TaskInstance
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import Team

log = structlog.get_logger(logger_name=__name__)


# See https://github.com/fastapi/fastapi/issues/13056
async def _container(request: Request):
    async with svcs.Container(request.app.state.svcs_registry) as cont:
        yield cont


DepContainer: svcs.Container = Depends(_container)


class JWTBearer(HTTPBearer):
    """JWT Bearer auth with scope validation via FastAPI's SecurityScopes."""

    def __init__(
        self,
        path_param_name: str | None = None,
        required_claims: dict[str, Any] | None = None,
    ):
        super().__init__(auto_error=False)
        self.path_param_name = path_param_name
        self.required_claims = required_claims or {}

    async def __call__(
        self,
        request: Request,
        security_scopes: SecurityScopes,
        services=DepContainer,
    ) -> TIToken:
        creds: HTTPAuthorizationCredentials | None = await super().__call__(request)
        if not creds:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing auth token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        validator: JWTValidator = await services.aget(JWTValidator)

        try:
            if self.path_param_name:
                ti_id = request.path_params[self.path_param_name]
                validators: dict[str, Any] = {
                    **self.required_claims,
                    "sub": {"essential": True, "value": ti_id},
                }
            else:
                validators = self.required_claims
            claims = await validator.avalidated_claims(creds.credentials, validators)
            self._validate_scopes(claims, security_scopes)
            return TIToken(id=claims["sub"], claims=claims)
        except HTTPException:
            raise
        except Exception as err:
            log.warning("Failed to validate JWT", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Invalid auth token: {err}",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def _validate_scopes(self, claims: dict[str, Any], security_scopes: SecurityScopes) -> None:
        if not security_scopes.scopes:
            return

        token_scope = claims.get("scope", "")
        mapped_scope = SCOPE_MAPPING.get(token_scope)
        if mapped_scope is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Unknown token scope: {token_scope}",
                headers={"WWW-Authenticate": "Bearer"},
            )

        for required_scope in security_scopes.scopes:
            if required_scope != mapped_scope:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Token missing required scope: {required_scope}",
                    headers={"WWW-Authenticate": "Bearer"},
                )


_jwt_bearer = JWTBearer()
_jwt_bearer_with_path = JWTBearer(path_param_name="task_instance_id")

# No scope check - for router-level auth
JWTBearerBaseDep = Security(_jwt_bearer, scopes=[])
# Execution scope - most endpoints
JWTBearerDep = Security(_jwt_bearer, scopes=[SCOPE_EXECUTION])
# Execution scope with path param validation
JWTBearerTIPathDep = Security(_jwt_bearer_with_path, scopes=[SCOPE_EXECUTION])
# Workload scope
JWTBearerWorkloadDep = Security(_jwt_bearer_with_path, scopes=[SCOPE_WORKLOAD])


async def get_team_name_dep(session: AsyncSessionDep, token=JWTBearerDep) -> str | None:
    """Return the team name associated to the task (if any)."""
    if not conf.getboolean("core", "multi_team"):
        return None

    stmt = (
        select(Team.name)
        .select_from(TaskInstance)
        .join(DagModel, DagModel.dag_id == TaskInstance.dag_id)
        .join(DagBundleModel, DagBundleModel.name == DagModel.bundle_name)
        .join(DagBundleModel.teams)
        .where(TaskInstance.id == str(token.id))
    )
    return await session.scalar(stmt)
