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

from functools import cache
from typing import TYPE_CHECKING, Annotated, Callable

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from jwt import ExpiredSignatureError, InvalidTokenError

from airflow.api_fastapi.app import get_auth_manager
from airflow.auth.managers.models.base_user import BaseUser
from airflow.auth.managers.models.resource_details import (
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.configuration import conf
from airflow.utils.jwt_signer import JWTSigner, get_signing_key

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@cache
def get_signer() -> JWTSigner:
    return JWTSigner(
        secret_key=get_signing_key("api", "auth_jwt_secret"),
        expiration_time_in_seconds=conf.getint("api", "auth_jwt_expiration_time"),
        audience="front-apis",
    )


def get_user(token_str: Annotated[str, Depends(oauth2_scheme)]) -> BaseUser:
    try:
        return get_auth_manager().get_user_from_token(token_str)
    except ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token Expired")
    except InvalidTokenError:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Forbidden")


async def get_user_with_exception_handling(request: Request) -> BaseUser | None:
    # Currently the UI does not support JWT authentication, this method defines a fallback if no token is provided by the UI.
    # We can remove this method when issue https://github.com/apache/airflow/issues/44884 is done.
    token_str = None

    # TODO remove try-except when authentication integrated everywhere, safeguard for non integrated clients and endpoints
    try:
        token_str = await oauth2_scheme(request)
    except HTTPException as e:
        if e.status_code == status.HTTP_401_UNAUTHORIZED:
            return None

    if not token_str:  # Handle None or empty token
        return None
    return get_user(token_str)


def requires_access_dag(method: ResourceMethod, access_entity: DagAccessEntity | None = None) -> Callable:
    def inner(
        user: Annotated[BaseUser, Depends(get_user)],
        dag_id: str | None = None,
    ) -> None:
        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_dag(
                method=method, access_entity=access_entity, details=DagDetails(id=dag_id), user=user
            )
        )

    return inner


def requires_access_pool(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: Annotated[BaseUser, Depends(get_user)],
    ) -> None:
        pool_name = request.path_params.get("pool_name")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_pool(
                method=method, details=PoolDetails(name=pool_name), user=user
            )
        )

    return inner


def requires_access_connection(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: Annotated[BaseUser, Depends(get_user)],
    ) -> None:
        connection_id = request.path_params.get("connection_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_connection(
                method=method, details=ConnectionDetails(conn_id=connection_id), user=user
            )
        )

    return inner


def requires_access_variable(method: ResourceMethod) -> Callable[[Request, BaseUser | None], None]:
    def inner(
        request: Request,
        user: Annotated[BaseUser | None, Depends(get_user)] = None,
    ) -> None:
        variable_key: str | None = request.path_params.get("variable_key")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_variable(
                method=method, details=VariableDetails(key=variable_key), user=user
            ),
        )

    return inner


def _requires_access(
    *,
    is_authorized_callback: Callable[[], bool],
) -> None:
    if not is_authorized_callback():
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Forbidden")
