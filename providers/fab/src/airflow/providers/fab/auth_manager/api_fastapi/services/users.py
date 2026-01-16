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

from fastapi import HTTPException, status

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import Role
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import UserBody, UserResponse
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.providers.fab.www.utils import get_fab_auth_manager


class FABAuthManagerUsers:
    """Service layer for FAB Auth Manager user operations (create, validate, sync)."""

    @staticmethod
    def _resolve_roles(
        sm: FabAirflowSecurityManagerOverride, role_refs: list[Role] | None
    ) -> tuple[list, list[str]]:
        seen = set()
        roles: list = []
        missing: list[str] = []
        for r in role_refs or []:
            if r.name in seen:
                continue
            seen.add(r.name)
            role = sm.find_role(r.name)
            (roles if role else missing).append(role or r.name)
        return roles, missing

    @classmethod
    def create_user(cls, body: UserBody) -> UserResponse:
        security_manager = get_fab_auth_manager().security_manager

        existing_username = security_manager.find_user(username=body.username)
        if existing_username:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Username `{body.username}` already exists. Use PATCH to update.",
            )

        existing_email = security_manager.find_user(email=body.email)
        if existing_email:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT, detail=f"The email `{body.email}` is already taken."
            )

        roles_to_add, missing_role_names = cls._resolve_roles(security_manager, body.roles)
        if missing_role_names:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown roles: {', '.join(repr(n) for n in missing_role_names)}",
            )
        if not roles_to_add:
            default_role = security_manager.find_role(security_manager.auth_user_registration_role)
            if default_role is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Default registration role is not configured or not found.",
                )
            roles_to_add.append(default_role)

        created = security_manager.add_user(
            username=body.username,
            email=body.email,
            first_name=body.first_name,
            last_name=body.last_name,
            role=roles_to_add,
            password=body.password.get_secret_value(),
        )
        if not created:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to add user `{body.username}`",
            )

        return UserResponse.model_validate(created)
