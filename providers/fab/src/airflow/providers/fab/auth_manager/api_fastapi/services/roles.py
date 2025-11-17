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

from typing import TYPE_CHECKING

from fastapi import HTTPException, status
from sqlalchemy import func, select

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import (
    RoleBody,
    RoleCollectionResponse,
    RoleResponse,
)
from airflow.providers.fab.auth_manager.api_fastapi.sorting import build_ordering
from airflow.providers.fab.auth_manager.models import Role
from airflow.providers.fab.www.utils import get_fab_auth_manager

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride


class FABAuthManagerRoles:
    """Service layer for FAB Auth Manager role operations (create, validate, sync)."""

    @staticmethod
    def _check_action_and_resource(
        security_manager: FabAirflowSecurityManagerOverride,
        perms: list[tuple[str, str]],
    ) -> None:
        for action_name, resource_name in perms:
            if not security_manager.get_action(action_name):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"The specified action: {action_name!r} was not found",
                )
            if not security_manager.get_resource(resource_name):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"The specified resource: {resource_name!r} was not found",
                )

    @classmethod
    def create_role(cls, body: RoleBody) -> RoleResponse:
        security_manager = get_fab_auth_manager().security_manager

        existing = security_manager.find_role(name=body.name)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Role with name {body.name!r} already exists; please update with the PATCH endpoint",
            )

        perms: list[tuple[str, str]] = [(ar.action.name, ar.resource.name) for ar in (body.permissions or [])]

        cls._check_action_and_resource(security_manager, perms)

        security_manager.bulk_sync_roles([{"role": body.name, "perms": perms}])

        created = security_manager.find_role(name=body.name)
        if not created:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Role was not created due to an unexpected error.",
            )

        return RoleResponse.model_validate(created)

    @classmethod
    def get_roles(cls, *, order_by: str, limit: int, offset: int) -> RoleCollectionResponse:
        security_manager = get_fab_auth_manager().security_manager
        session = security_manager.session

        total_entries = session.scalars(select(func.count(Role.id))).one()

        ordering = build_ordering(order_by, allowed={"name": Role.name, "role_id": Role.id})

        stmt = select(Role).order_by(ordering).offset(offset).limit(limit)
        roles = session.scalars(stmt).unique().all()

        return RoleCollectionResponse(
            roles=[RoleResponse.model_validate(r) for r in roles],
            total_entries=total_entries,
        )

    @classmethod
    def delete_role(cls, name: str) -> None:
        security_manager = get_fab_auth_manager().security_manager

        existing = security_manager.find_role(name=name)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Role with name {name!r} does not exist.",
            )
        security_manager.delete_role(existing)

    @classmethod
    def get_role(cls, name: str) -> RoleResponse:
        security_manager = get_fab_auth_manager().security_manager

        existing = security_manager.find_role(name=name)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Role with name {name!r} does not exist.",
            )
        return RoleResponse.model_validate(existing)

    @classmethod
    def patch_role(cls, body: RoleBody, name: str, update_mask: str | None = None) -> RoleResponse:
        security_manager = get_fab_auth_manager().security_manager

        existing = security_manager.find_role(name=name)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Role with name {name!r} does not exist.",
            )

        if update_mask:
            update_data = RoleResponse.model_validate(existing)

            for field in update_mask:
                if field == "actions":
                    update_data.permissions = body.permissions
                elif hasattr(body, field):
                    setattr(update_data, field, getattr(body, field))
                else:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"'{field}' in update_mask is unknown",
                    )
        else:
            update_data = RoleResponse(name=body.name, permissions=body.permissions or [])

        perms: list[tuple[str, str]] = [(ar.action.name, ar.resource.name) for ar in (body.permissions or [])]
        cls._check_action_and_resource(security_manager, perms)
        security_manager.bulk_sync_roles([{"role": name, "perms": perms}])

        new_name = update_data.name
        if new_name and new_name != existing.name:
            security_manager.update_role(role_id=existing.id, name=new_name)
        return RoleResponse.model_validate(update_data)
