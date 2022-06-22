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
from connexion import NoContent
from flask import current_app, request
from flask_appbuilder.security.sqla.models import User
from marshmallow import ValidationError
from sqlalchemy import func
from werkzeug.security import generate_password_hash

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound, Unknown
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.user_schema import (
    UserCollection,
    user_collection_item_schema,
    user_collection_schema,
    user_schema,
)
from airflow.security import permissions


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_USER)])
def get_user(username):
    """Get a user"""
    ab_security_manager = current_app.appbuilder.sm
    user = ab_security_manager.find_user(username=username)
    if not user:
        raise NotFound(title="User not found", detail=f"The User with username `{username}` was not found")
    return user_collection_item_schema.dump(user)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_USER)])
@format_parameters({'limit': check_limit})
def get_users(limit, order_by='id', offset=None):
    """Get users"""
    appbuilder = current_app.appbuilder
    session = appbuilder.get_session
    total_entries = session.query(func.count(User.id)).scalar()
    to_replace = {"user_id": "id"}
    allowed_filter_attrs = [
        "user_id",
        'id',
        "first_name",
        "last_name",
        "user_name",
        "email",
        "is_active",
        "role",
    ]
    query = session.query(User)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    users = query.offset(offset).limit(limit).all()

    return user_collection_schema.dump(UserCollection(users=users, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER)])
def post_user():
    """Create a new user"""
    try:
        data = user_schema.load(request.json)
    except ValidationError as e:
        raise BadRequest(detail=str(e.messages))

    security_manager = current_app.appbuilder.sm
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
def patch_user(username, update_mask=None):
    """Update a user"""
    try:
        data = user_schema.load(request.json)
    except ValidationError as e:
        raise BadRequest(detail=str(e.messages))

    security_manager = current_app.appbuilder.sm

    user = security_manager.find_user(username=username)
    if user is None:
        detail = f"The User with username `{username}` was not found"
        raise NotFound(title="User not found", detail=detail)
    # Check unique username
    new_username = data.get('username')
    if new_username and new_username != username:
        if security_manager.find_user(username=new_username):
            raise AlreadyExists(detail=f"The username `{new_username}` already exists")

    # Check unique email
    email = data.get('email')
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
def delete_user(username):
    """Delete a user"""
    security_manager = current_app.appbuilder.sm

    user = security_manager.find_user(username=username)
    if user is None:
        detail = f"The User with username `{username}` was not found"
        raise NotFound(title="User not found", detail=detail)

    user.roles = []  # Clear foreign keys on this user first.
    security_manager.get_session.delete(user)
    security_manager.get_session.commit()
    return NoContent, 204
