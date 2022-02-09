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

from typing import List, Optional, Tuple

from connexion import NoContent
from flask import current_app, request
from marshmallow import ValidationError
from sqlalchemy import func

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import AlreadyExists, BadRequest, NotFound
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.role_and_permission_schema import (
    ActionCollection,
    RoleCollection,
    action_collection_schema,
    role_collection_schema,
    role_schema,
)
from airflow.api_connexion.types import APIResponse, UpdateMask
from airflow.security import permissions
from airflow.www.fab_security.sqla.models import Action, Role
from airflow.www.security import AirflowSecurityManager


def _check_action_and_resource(sm: AirflowSecurityManager, perms: List[Tuple[str, str]]) -> None:
    """
    Checks if the action or resource exists and raise 400 if not

    This function is intended for use in the REST API because it raise 400
    """
    for action, resource in perms:
        if not sm.get_action(action):
            raise BadRequest(detail=f"The specified action: {action!r} was not found")
        if not sm.get_resource(resource):
            raise BadRequest(detail=f"The specified resource: {resource!r} was not found")


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE)])
def get_role(*, role_name: str) -> APIResponse:
    """Get role"""
    ab_security_manager = current_app.appbuilder.sm
    role = ab_security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"Role with name {role_name!r} was not found")
    return role_schema.dump(role)


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE)])
@format_parameters({"limit": check_limit})
def get_roles(*, order_by: str = "name", limit: int, offset: Optional[int] = None) -> APIResponse:
    """Get roles"""
    appbuilder = current_app.appbuilder
    session = appbuilder.get_session
    total_entries = session.query(func.count(Role.id)).scalar()
    to_replace = {"role_id": "id"}
    allowed_filter_attrs = ["role_id", "name"]
    query = session.query(Role)
    query = apply_sorting(query, order_by, to_replace, allowed_filter_attrs)
    roles = query.offset(offset).limit(limit).all()

    return role_collection_schema.dump(RoleCollection(roles=roles, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_ACTION)])
@format_parameters({'limit': check_limit})
def get_permissions(*, limit: int, offset: Optional[int] = None) -> APIResponse:
    """Get permissions"""
    session = current_app.appbuilder.get_session
    total_entries = session.query(func.count(Action.id)).scalar()
    query = session.query(Action)
    actions = query.offset(offset).limit(limit).all()
    return action_collection_schema.dump(ActionCollection(actions=actions, total_entries=total_entries))


@security.requires_access([(permissions.ACTION_CAN_DELETE, permissions.RESOURCE_ROLE)])
def delete_role(*, role_name: str) -> APIResponse:
    """Delete a role"""
    ab_security_manager = current_app.appbuilder.sm
    role = ab_security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"Role with name {role_name!r} was not found")
    ab_security_manager.delete_role(role_name=role_name)
    return NoContent, 204


@security.requires_access([(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE)])
def patch_role(*, role_name: str, update_mask: UpdateMask = None) -> APIResponse:
    """Update a role"""
    appbuilder = current_app.appbuilder
    security_manager = appbuilder.sm
    body = request.json
    try:
        data = role_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    role = security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"Role with name {role_name!r} was not found")
    if update_mask:
        update_mask = [i.strip() for i in update_mask]
        data_ = {}
        for field in update_mask:
            if field in data and not field == "permissions":
                data_[field] = data[field]
            elif field == "actions":
                data_["permissions"] = data['permissions']
            else:
                raise BadRequest(detail=f"'{field}' in update_mask is unknown")
        data = data_
    if "permissions" in data:
        perms = [(item["action"]["name"], item["resource"]["name"]) for item in data["permissions"] if item]
        _check_action_and_resource(security_manager, perms)
        security_manager.bulk_sync_roles([{"role": role_name, "perms": perms}])
    new_name = data.get("name")
    if new_name is not None and new_name != role.name:
        security_manager.update_role(role_id=role.id, name=new_name)
    return role_schema.dump(role)


@security.requires_access([(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_ROLE)])
def post_role() -> APIResponse:
    """Create a new role"""
    appbuilder = current_app.appbuilder
    security_manager = appbuilder.sm
    body = request.json
    try:
        data = role_schema.load(body)
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))
    role = security_manager.find_role(name=data['name'])
    if not role:
        perms = [(item['action']['name'], item['resource']['name']) for item in data['permissions'] if item]
        _check_action_and_resource(security_manager, perms)
        security_manager.bulk_sync_roles([{"role": data["name"], "perms": perms}])
        return role_schema.dump(role)
    detail = f"Role with name {role.name!r} already exists; please update with the PATCH endpoint"
    raise AlreadyExists(detail=detail)
