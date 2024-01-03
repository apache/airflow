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

"""Append dag pause on dag edit

Revision ID: bd59965b3fe5
Revises: 10b52ebd31f7
Create Date: 2023-04-23 13:23:39.037211

"""
from __future__ import annotations

from airflow.security import permissions
from airflow.www.app import cached_app

# revision identifiers, used by Alembic.
revision = "bd59965b3fe5"
down_revision = "10b52ebd31f7"
branch_labels = None
depends_on = None
airflow_version = "2.8.1"


def append_dag_pause():
    """Append dag can_pause on existing dag can_edit permission on roles.

    To avoid current roles that have can_edit permission to lose the ability
    of pausing dag.
    """
    appbuilder = cached_app(config={"FAB_UPDATE_PERMS": False}).appbuilder
    dag_pause_permission = appbuilder.sm.create_permission(
        permissions.ACTION_CAN_PAUSE, permissions.RESOURCE_DAG
    )

    for role in appbuilder.sm._get_all_roles_with_permissions().values():
        for permission in role.permissions:
            action_name = permission.action.name
            resource_name = permission.resource.name

            if action_name == permissions.ACTION_CAN_EDIT:
                # Only append pause to existing can_edit dag resource
                if resource_name == permissions.RESOURCE_DAG:
                    appbuilder.sm.add_permission_to_role(role, dag_pause_permission)
                elif resource_name.startswith(permissions.RESOURCE_DAG_PREFIX):
                    a_dag_pause_permission = appbuilder.sm.create_permission(
                        permissions.ACTION_CAN_PAUSE, resource_name
                    )
                    appbuilder.sm.add_permission_to_role(role, a_dag_pause_permission)


def remove_dag_pause():
    """Remove dag can_pause and restore dag can_edit permission on roles.

    Also clean up can_pause from permission table
    """
    appbuilder = cached_app(config={"FAB_UPDATE_PERMS": False}).appbuilder
    dag_edit_permission = appbuilder.sm.get_permission(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)

    for role in appbuilder.sm._get_all_roles_with_permissions().values():
        role_permissions = role.permissions.copy()
        for permission in role_permissions:
            action_name = permission.action.name
            resource_name = permission.resource.name

            if action_name == permissions.ACTION_CAN_PAUSE:
                # Restore dag edit permission on dag pause removal
                if resource_name == permissions.RESOURCE_DAG:
                    appbuilder.sm.remove_permission_from_role(role, permission)
                    appbuilder.sm.add_permission_to_role(role, dag_edit_permission)
                elif resource_name.startswith(permissions.RESOURCE_DAG_PREFIX):
                    a_dag_edit_permission = appbuilder.sm.create_permission(
                        permissions.ACTION_CAN_EDIT, resource_name
                    )
                    appbuilder.sm.remove_permission_from_role(role, permission)
                    appbuilder.sm.add_permission_to_role(role, a_dag_edit_permission)

    # Clean up can_pause permissions
    for permission in appbuilder.sm.get_all_permissions():
        action, resource = permission
        if action == permissions.ACTION_CAN_PAUSE and (
            resource == permissions.RESOURCE_DAG or resource.startswith(permissions.RESOURCE_DAG_PREFIX)
        ):
            appbuilder.sm.delete_permission(*permission)


def upgrade():
    """Apply Append dag pause on dag edit"""
    append_dag_pause()


def downgrade():
    """Unapply Append dag pause on dag edit"""
    remove_dag_pause()
