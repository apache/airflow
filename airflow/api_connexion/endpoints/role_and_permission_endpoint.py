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

from flask import current_app
from flask_appbuilder.security.sqla.models import (
    Permission,
    PermissionView,
    Role,
    User,
    assoc_permissionview_role,
    assoc_user_role,
)
from sqlalchemy import and_, func

from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.role_and_permission_schema import (
    ActionCollection,
    RoleCollection,
    action_collection_schema,
    role_collection_item_schema,
    role_collection_schema,
)


def get_role(role_name):
    """Get role"""
    ab_security_manager = current_app.appbuilder.sm
    role = ab_security_manager.find_role(name=role_name)
    if not role:
        raise NotFound(title="Role not found", detail=f"The role with name {role_name} was not found")
    return role_collection_item_schema.dump(role)


def get_roles(usernames=None, action_resource_ids=None, role_name=None, limit=None, offset=None):
    """Get roles"""
    appbuilder = current_app.appbuilder
    session = appbuilder.get_session
    total_entries = session.query(func.count(Role.id)).scalar()
    query = session.query(Role)

    if usernames:
        query = (
            query.join(
                assoc_user_role,
                and_(
                    Role.id == assoc_user_role.c.role_id,
                ),
            )
            .join(User)
            .filter(User.username.in_(usernames))
        )
    if action_resource_ids:
        query = query.join(
            assoc_permissionview_role, and_(Role.id == assoc_permissionview_role.c.role_id)
        ).filter(PermissionView.id.in_(action_resource_ids))
    if role_name:
        query = query.filter(func.lower(Role.name) == role_name.lower())

    roles = query.offset(offset).limit(limit).all()

    return role_collection_schema.dump(RoleCollection(roles=roles, total_entries=total_entries))


def get_permissions(limit=None, offset=None):
    """Get permissions"""
    session = current_app.appbuilder.get_session
    total_entries = session.query(func.count(Permission.id)).scalar()
    query = session.query(Permission)
    permissions = query.offset(offset).limit(limit).all()
    return action_collection_schema.dump(ActionCollection(actions=permissions, total_entries=total_entries))
