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

from http import HTTPStatus

from connexion import NoContent
from flask import request
from marshmallow import ValidationError
from sqlalchemy import asc, desc, func
from werkzeug.security import generate_password_hash

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound, Unknown
from airflow.api_connexion.parameters import check_limit, format_parameters
from airflow.api_connexion.schemas.user_schema import (
    UserCollection,
    user_collection_item_schema,
    user_collection_schema,
    user_schema,
)
from airflow.api_connexion.types import APIResponse, UpdateMask
from airflow.security import permissions
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.www.fab_security.sqla.models import Role, User


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_USER)])
def get_user(*, username: str) -> APIResponse:
    """Get a user."""
    ab_security_manager = get_airflow_app().appbuilder.sm
    user = ab_security_manager.find_user(username=username)
    if not user:
        raise NotFound(title="User not found", detail=f"The User with username `{username}` was not found")
    return user_collection_item_schema.dump(user)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_USER)])
@format_parameters({"limit": check_limit})
def get_users(*, limit: int, order_by: str = "id", offset: str | None = None) -> APIResponse:
    """Get users."""
    appbuilder = get_airflow_app().appbuilder
    session = appbuilder.get_session
    total_entries = session.query(func.count(User.id)).scalar()
    direction = desc if order_by.startswith("-") else asc
    to_replace = {"user_id": "id"}
    order_param = order_by.strip("-")
    order_param = to_replace.get(order_param, order_param)
    allowed_filter_attrs = [
        "id",
        "first_name",
        "last_name",
        "user_name",
        "email",
        "is_active",
        "role",
    ]
    if order_by not in allowed_filter_attrs:
        raise BadRequest(
            detail=f"Ordering with '{order_by}' is disallowed or "
            f"the attribute does not exist on the model"
        )

    query = session.query(User)
    users = query.order_by(direction(getattr(User, order_param))).offset(offset).limit(limit).all()

    return user_collection_schema.dump(UserCollection(users=users, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER)])
def post_user() -> APIResponse:
    """Create a new user."""
    try:
        data = user_schema.load(request.json)
    except ValidationError as e:
        raise BadRequest(detail=str(e.messages))

    security_manager = get_airflow_app().appbuilder.sm
    username = data["username"]
    email = data["email"]

    if security_manager.find_user(username=username):
        detail = f"Username `{username}` already exists. Use PATCH to update."
        raise AlreadyExists(detail=detail)
    if security_manager.find_user(email=email):
        detail = f"The email `{email}` is already taken."
        raise AlreadyExists(detail=detail)

    roles_to_add = []
    missing_role_names = []
    for role_data in data.pop("roles", ()):
        role_name = role_data["name"]
        role = security_manager.find_role(role_name)
        if role is None:
            missing_role_names.append(role_name)
        else:
            roles_to_add.append(role)
    if missing_role_names:
        detail = f"Unknown roles: {', '.join(repr(n) for n in missing_role_names)}"
        raise BadRequest(detail=detail)

    if not roles_to_add:  # No roles provided, use the F.A.B's default registered user role.
        roles_to_add.append(security_manager.find_role(security_manager.auth_user_registration_role))

    user = security_manager.add_user(role=roles_to_add, **data)
    if not user:
        detail = f"Failed to add user `{username}`."
        return Unknown(detail=detail)

    return user_schema.dump(user)


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER)])
def patch_user(*, username: str, update_mask: UpdateMask = None) -> APIResponse:
    """Update a user."""
    try:
        data = user_schema.load(request.json)
    except ValidationError as e:
        raise BadRequest(detail=str(e.messages))

    security_manager = get_airflow_app().appbuilder.sm

    user = security_manager.find_user(username=username)
    if user is None:
        detail = f"The User with username `{username}` was not found"
        raise NotFound(title="User not found", detail=detail)
    # Check unique username
    new_username = data.get("username")
    if new_username and new_username != username:
        if security_manager.find_user(username=new_username):
            raise AlreadyExists(detail=f"The username `{new_username}` already exists")

    # Check unique email
    email = data.get("email")
    if email and email != user.email:
        if security_manager.find_user(email=email):
            raise AlreadyExists(detail=f"The email `{email}` already exists")

    # Get fields to update.
    if update_mask is not None:
        masked_data = {}
        missing_mask_names = []
        for field in update_mask:
            field = field.strip()
            try:
                masked_data[field] = data[field]
            except KeyError:
                missing_mask_names.append(field)
        if missing_mask_names:
            detail = f"Unknown update masks: {', '.join(repr(n) for n in missing_mask_names)}"
            raise BadRequest(detail=detail)
        data = masked_data

    roles_to_update: list[Role] | None
    if "roles" in data:
        roles_to_update = []
        missing_role_names = []
        for role_data in data.pop("roles", ()):
            role_name = role_data["name"]
            role = security_manager.find_role(role_name)
            if role is None:
                missing_role_names.append(role_name)
            else:
                roles_to_update.append(role)
        if missing_role_names:
            detail = f"Unknown roles: {', '.join(repr(n) for n in missing_role_names)}"
            raise BadRequest(detail=detail)
    else:
        roles_to_update = None  # Don't change existing value.

    if "password" in data:
        user.password = generate_password_hash(data.pop("password"))
    if roles_to_update is not None:
        user.roles = roles_to_update
    for key, value in data.items():
        setattr(user, key, value)
    security_manager.update_user(user)

    return user_schema.dump(user)


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER)])
def delete_user(*, username: str) -> APIResponse:
    """Delete a user."""
    security_manager = get_airflow_app().appbuilder.sm

    user = security_manager.find_user(username=username)
    if user is None:
        detail = f"The User with username `{username}` was not found"
        raise NotFound(title="User not found", detail=detail)

    user.roles = []  # Clear foreign keys on this user first.
    security_manager.get_session.delete(user)
    security_manager.get_session.commit()

    return NoContent, HTTPStatus.NO_CONTENT
