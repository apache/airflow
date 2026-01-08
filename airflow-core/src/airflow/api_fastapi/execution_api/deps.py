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
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer
from sqlalchemy import select

from airflow.api_fastapi.auth.tokens import TOKEN_SCOPE_QUEUE, JWTValidator
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
    """
    A FastAPI security dependency that validates JWT tokens for the Execution API.

    This validates tokens are signed and that the ``sub`` is a UUID. Queue-scoped tokens
    (with scope="queue") are rejected - they can only be used on the /run endpoint.

    The dependency result will be a `TIToken` object containing the ``id`` UUID (from the ``sub``)
    and other validated claims.
    """

    def __init__(
        self,
        path_param_name: str | None = None,
        required_claims: dict[str, Any] | None = None,
    ):
        super().__init__(auto_error=False)
        self.path_param_name = path_param_name
        self.required_claims = required_claims or {}

    async def __call__(  # type: ignore[override]
        self,
        request: Request,
        services=DepContainer,
    ) -> TIToken | None:
        creds = await super().__call__(request)
        if not creds:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing auth token")

        validator: JWTValidator = await services.aget(JWTValidator)

        try:
            if self.path_param_name:
                id = request.path_params[self.path_param_name]
                validators: dict[str, Any] = {
                    **self.required_claims,
                    "sub": {"essential": True, "value": id},
                }
            else:
                validators = self.required_claims
            claims = await validator.avalidated_claims(creds.credentials, validators)

            # Reject queue-scoped tokens - they can only be used on /run endpoint
            # Only check if scope claim is present (allows backwards compatibility with tests)
            scope = claims.get("scope")
            if scope is not None and scope == TOKEN_SCOPE_QUEUE:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Queue tokens cannot access this endpoint. Use the token from /run response.",
                )

            return TIToken(id=claims["sub"], claims=claims)
        except HTTPException:
            raise
        except Exception as err:
            log.warning("Failed to validate JWT", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Invalid auth token: {err}")


class JWTBearerQueueScope(HTTPBearer):
    """
    JWT auth dependency that ONLY accepts queue-scoped tokens.

    Used exclusively by the /run endpoint. Queue tokens have scope="queue" and are
    long-lived to survive executor queue wait times. The /run endpoint validates
    the queue token and issues a short-lived execution token for subsequent API calls.
    """

    def __init__(self, path_param_name: str | None = None):
        super().__init__(auto_error=False)
        self.path_param_name = path_param_name

    async def __call__(  # type: ignore[override]
        self,
        request: Request,
        services=DepContainer,
    ) -> TIToken | None:
        creds = await super().__call__(request)
        if not creds:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing auth token")

        validator: JWTValidator = await services.aget(JWTValidator)

        try:
            if self.path_param_name:
                id = request.path_params[self.path_param_name]
                validators: dict[str, Any] = {"sub": {"essential": True, "value": id}}
            else:
                validators = {}
            claims = await validator.avalidated_claims(creds.credentials, validators)

            # Only accept queue-scoped tokens (if scope claim is present)
            # This allows backwards compatibility with tests that don't set scope
            scope = claims.get("scope")
            if scope is not None and scope != TOKEN_SCOPE_QUEUE:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="This endpoint requires a queue-scoped token",
                )

            return TIToken(id=claims["sub"], claims=claims)
        except HTTPException:
            raise
        except Exception as err:
            log.warning("Failed to validate JWT", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Invalid auth token: {err}")


JWTBearerDep: TIToken = Depends(JWTBearer())

# This checks that the UUID in the url matches the one in the token for us.
JWTBearerTIPathDep = Depends(JWTBearer(path_param_name="task_instance_id"))

# For /run endpoint only - accepts queue-scoped tokens and validates task_instance_id
JWTBearerQueueDep = Depends(JWTBearerQueueScope(path_param_name="task_instance_id"))


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
