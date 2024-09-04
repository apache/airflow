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

from typing import Callable, cast

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing_extensions import Annotated

from airflow.auth.managers.base_auth_manager import ResourceMethod
from airflow.auth.managers.models.base_user import BaseUser
from airflow.auth.managers.models.resource_details import DagAccessEntity, DagDetails, DatasetDetails
from airflow.providers.fab.auth_manager.api.auth.backend.basic_auth import auth_current_user
from airflow.providers.fab.auth_manager.models import User
from airflow.www.extensions.init_auth_manager import get_auth_manager

security = HTTPBasic()


def check_authentication(
    request: Request,
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
) -> User | None:
    """Check that the request has valid authorization information."""
    # TODO:
    #    - Handle other auth backends
    #    - Handle AUTH_ROLE_PUBLIC
    user = auth_current_user(credentials)
    if user is not None:
        return user

    # since this handler only checks authentication, not authorization,
    # we should always return 401
    raise HTTPException(401, headers={"WWW-Authenticate": "Basic"})


def _requires_access(
    *,
    is_authorized_callback: Callable[[], bool],
) -> None:
    if not is_authorized_callback():
        raise HTTPException(403, "Forbidden")


def requires_access_dataset(
    request: Request,
    uri: str | None = None,
    user: Annotated[BaseUser | None, Depends(check_authentication)] = None,
) -> None:
    _requires_access(
        is_authorized_callback=lambda: get_auth_manager().is_authorized_dataset(
            user=user,
            method=cast(ResourceMethod, request.method),
            details=DatasetDetails(uri=uri),
        )
    )


def requires_access_dag(access_entity: DagAccessEntity | None = None) -> Callable:
    def inner(
        request: Request,
        dag_id: str | None = None,
        user: Annotated[BaseUser | None, Depends(check_authentication)] = None,
    ) -> None:
        method = cast(ResourceMethod, request.method)

        def callback():
            access = get_auth_manager().is_authorized_dag(
                method=method, access_entity=access_entity, details=DagDetails(id=dag_id), user=user
            )

            # ``access`` means here:
            # - if a DAG id is provided (``dag_id`` not None): is the user authorized to access this DAG
            # - if no DAG id is provided: is the user authorized to access all DAGs
            if dag_id or access or access_entity:
                return access

            # No DAG id is provided, the user is not authorized to access all DAGs and authorization is done
            # on DAG level
            # If method is "GET", return whether the user has read access to any DAGs
            # If method is "PUT", return whether the user has edit access to any DAGs
            return (method == "GET" and any(get_auth_manager().get_permitted_dag_ids(methods=["GET"]))) or (
                method == "PUT" and any(get_auth_manager().get_permitted_dag_ids(methods=["PUT"]))
            )

        _requires_access(
            is_authorized_callback=callback,
        )

    return inner
