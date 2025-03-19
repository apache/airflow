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

from typing import Any, Optional

import structlog
import svcs
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.datamodels.token import TIToken

log = structlog.get_logger(logger_name=__name__)


# See https://github.com/fastapi/fastapi/issues/13056
async def _container(request: Request):
    async with svcs.Container(request.app.state.svcs_registry) as cont:
        yield cont


DepContainer: svcs.Container = Depends(_container)


class JWTBearer(HTTPBearer):
    """
    A FastAPI security dependency that validates JWT tokens using for the Execution API.

    This will validate the tokens are signed and that the ``sub`` is a UUID, but nothing deeper than that.

    The dependency result will be an `TIToken` object containing the ``id`` UUID (from the ``sub``) and other
    validated claims.
    """

    def __init__(
        self,
        path_param_name: Optional[str] = None,
        required_claims: Optional[dict[str, Any]] = None,
    ):
        super().__init__(auto_error=False)
        self.path_param_name = path_param_name
        self.required_claims = required_claims or {}

    async def __call__(  # type: ignore[override]
        self,
        request: Request,
        services=DepContainer,
    ) -> Optional[TIToken]:
        creds = await super().__call__(request)
        if not creds:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing auth token")

        validator: JWTValidator = await services.aget(JWTValidator)

        try:
            # Example: Validate "task_instance_id" component of the path matches the one in the token
            if self.path_param_name:
                id = request.path_params[self.path_param_name]
                validators: dict[str, Any] = {
                    **self.required_claims,
                    "sub": {"essential": True, "value": id},
                }
            else:
                validators = self.required_claims
            claims = await validator.avalidated_claims(creds.credentials, validators)
            return TIToken(id=claims["sub"], claims=claims)
        except Exception as err:
            log.warning(
                "Failed to validate JWT",
                exc_info=True,
                token=creds.credentials,
            )
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Invalid auth token: {err}")


JWTBearerDep: TIToken = Depends(JWTBearer())
