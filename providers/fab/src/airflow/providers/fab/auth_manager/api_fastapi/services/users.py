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
from sqlalchemy import func, select
from werkzeug.security import generate_password_hash

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import Role
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import (
    UserBody,
    UserCollectionResponse,
    UserPatchBody,
    UserResponse,
)
from airflow.providers.fab.auth_manager.api_fastapi.sorting import build_ordering
from airflow.providers.fab.auth_manager.models import User
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.providers.fab.www.utils import get_fab_auth_manager


class FABAuthManagerUsers:
    """Service layer for FAB Auth Manager user operations."""

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
    def get_user(cls, username: str) -> UserResponse:
        """Get a user by username."""
        security_manager = get_fab_auth_manager().security_manager
        user = security_manager.find_user(username=username)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The User with username `{username}` was not found",
            )
        return UserResponse.model_validate(user)

    @classmethod
    def get_users(cls, *, order_by: str, limit: int, offset: int) -> UserCollectionResponse:
        """Get users with pagination and ordering."""
        security_manager = get_fab_auth_manager().security_manager
        session = security_manager.session

        total_entries = session.scalars(select(func.count(User.id))).one()

        ordering = build_ordering(
            order_by,
            allowed={
                "id": User.id,
                "user_id": User.id,
                "first_name": User.first_name,
                "last_name": User.last_name,
                "username": User.username,
                "email": User.email,
                "active": User.active,
            },
        )

        stmt = select(User).order_by(ordering).offset(offset).limit(limit)
        users = session.scalars(stmt).unique().all()

        return UserCollectionResponse(
            users=[UserResponse.model_validate(u) for u in users],
            total_entries=total_entries,
        )

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
                status_code=status.HTTP_409_CONFLICT,
                detail=f"The email `{body.email}` is already taken.",
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

    @classmethod
    def update_user(cls, username: str, body: UserPatchBody, update_mask: str | None = None) -> UserResponse:
        """Update an existing user."""
        security_manager = get_fab_auth_manager().security_manager

        user = security_manager.find_user(username=username)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The User with username `{username}` was not found",
            )

        if body.username is not None and body.username != username:
            if security_manager.find_user(username=body.username):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"The username `{body.username}` already exists",
                )

        if body.email is not None and body.email != user.email:
            if security_manager.find_user(email=body.email):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"The email `{body.email}` already exists",
                )

        all_fields = {"username", "email", "first_name", "last_name", "roles", "password"}

        if update_mask is not None:
            fields_to_update = {f.strip() for f in update_mask.split(",") if f.strip()}
            invalid_fields = fields_to_update - all_fields
            if invalid_fields:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unknown update masks: {', '.join(repr(f) for f in invalid_fields)}",
                )
        else:
            fields_to_update = all_fields

        if "roles" in fields_to_update and body.roles is not None:
            roles_to_update, missing_role_names = cls._resolve_roles(security_manager, body.roles)
            if missing_role_names:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unknown roles: {', '.join(repr(n) for n in missing_role_names)}",
                )
            user.roles = roles_to_update

        if "password" in fields_to_update and body.password is not None:
            user.password = generate_password_hash(body.password.get_secret_value())

        if "username" in fields_to_update and body.username is not None:
            user.username = body.username
        if "email" in fields_to_update and body.email is not None:
            user.email = body.email
        if "first_name" in fields_to_update and body.first_name is not None:
            user.first_name = body.first_name
        if "last_name" in fields_to_update and body.last_name is not None:
            user.last_name = body.last_name

        security_manager.update_user(user)
        return UserResponse.model_validate(user)

    @classmethod
    def delete_user(cls, username: str) -> None:
        """Delete a user by username."""
        security_manager = get_fab_auth_manager().security_manager

        user = security_manager.find_user(username=username)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The User with username `{username}` was not found",
            )

        user.roles = []
        security_manager.session.delete(user)
        security_manager.session.commit()
