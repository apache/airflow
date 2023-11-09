#
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

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.resource_details import (
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        PoolDetails,
        VariableDetails,
    )


class IsAuthorizedConnectionRequest(TypedDict, total=False):
    """Represent the parameters of ``is_authorized_connection`` API in the auth manager."""

    method: ResourceMethod
    details: ConnectionDetails | None
    user: BaseUser | None


class IsAuthorizedDagRequest(TypedDict, total=False):
    """Represent the parameters of ``is_authorized_dag`` API in the auth manager."""

    method: ResourceMethod
    access_entity: DagAccessEntity | None
    details: DagDetails | None
    user: BaseUser | None


class IsAuthorizedPoolRequest(TypedDict, total=False):
    """Represent the parameters of ``is_authorized_pool`` API in the auth manager."""

    method: ResourceMethod
    details: PoolDetails | None
    user: BaseUser | None


class IsAuthorizedVariableRequest(TypedDict, total=False):
    """Represent the parameters of ``is_authorized_variable`` API in the auth manager."""

    method: ResourceMethod
    details: VariableDetails | None
    user: BaseUser | None
