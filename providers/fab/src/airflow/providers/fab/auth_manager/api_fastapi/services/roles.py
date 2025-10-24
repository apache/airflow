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

from typing import TYPE_CHECKING, cast

from starlette import status
from starlette.exceptions import HTTPException

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleIn, RoleOut

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride


class FABAuthManagerRoles:
    """Service layer for FAB Auth Manager role operations (create, validate, sync)."""

    @staticmethod
    def _check_action_and_resource(
        sm: FabAirflowSecurityManagerOverride,
        perms: list[tuple[str, str]],
    ) -> None:
        for action_name, resource_name in perms:
            if not sm.get_action(action_name):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"The specified action: {action_name!r} was not found",
                )
            if not sm.get_resource(resource_name):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"The specified resource: {resource_name!r} was not found",
                )

    @classmethod
    def create_role(cls, body: RoleIn) -> RoleOut:
        if not body or not body.name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Role name must be provided")

        security_manager = cast("FabAuthManager", get_auth_manager()).security_manager
        sm: FabAirflowSecurityManagerOverride = cast("FabAirflowSecurityManagerOverride", security_manager)

        existing = sm.find_role(name=body.name)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Role with name {body.name!r} already exists; please update with the PATCH endpoint",
            )

        perms: list[tuple[str, str]] = [(ar.action.name, ar.resource.name) for ar in (body.permissions or [])]

        cls._check_action_and_resource(sm, perms)

        sm.bulk_sync_roles([{"role": body.name, "perms": perms}])

        created = sm.find_role(name=body.name)
        if not created:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Role was not created due to an unexpected error.",
            )

        return RoleOut.model_validate(created)
