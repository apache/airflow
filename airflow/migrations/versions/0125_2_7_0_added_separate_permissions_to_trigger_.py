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

"""added separate permissions to trigger dag

Revision ID: 1e5d09a0751b
Revises: 98ae134e6fff
Create Date: 2023-05-08 18:17:27.058808

"""
from __future__ import annotations

from airflow.security import permissions
from airflow.www.app import cached_app

# revision identifiers, used by Alembic.
revision = "1e5d09a0751b"
down_revision = "98ae134e6fff"
branch_labels = None
depends_on = None
airflow_version = '2.7.0'


def add_trigger_permissions():
    """Create can_trigger permissions for roles with can_edit on DAG:<dag_name> and can_edit on Dags"""
    appbuilder = cached_app(config={"FAB_UPDATE_PERMS": False}).appbuilder
    # Get all roles with can_create dagrun. If role doesn't have can_create dagrun,
    # it can't trigger dag anyway so we don't need to add can_trigger permission
    for role in appbuilder.sm.get_all_roles():
        role_permissions = {
            (permission.action.name, permission.resource.name) for permission in role.permissions
        }
        if (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN) not in role_permissions:
            continue

        # Could contains `DAG:<dag_name>:cat_edit` permissions and `DAG:can_edit`
        resources_with_edit_dag_perms = set()
        # Role might have permissions to edit separate dags, if so,
        # set up a new trigger action for every dag.
        # We need to add can_trigger permissions for every dag if it already has permissions
        # to edit a dag and to create dagrun
        for action, resource in role_permissions:
            if action == permissions.ACTION_CAN_EDIT and appbuilder.sm.is_dag_resource(resource):
                resources_with_edit_dag_perms.add(resource)

        # role might have a permission to edit all dags, if so, set up a new trigger action for root dag
        if (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG) in role_permissions:
            resources_with_edit_dag_perms.add(permissions.RESOURCE_DAG)

        # add can_trigger permission for every selected resource
        for dag_resource in resources_with_edit_dag_perms:
            new_permission = appbuilder.sm.create_permission(permissions.ACTION_CAN_TRIGGER, dag_resource)
            appbuilder.sm.add_permission_to_role(role, new_permission)


def undo_trigger_permissions():
    # need to delete all permissions where can_trigger action is allowed
    appbuilder = cached_app(config={"FAB_UPDATE_PERMS": False}).appbuilder
    for role in appbuilder.sm.get_all_roles():
        role_permissions = {
            (permission.action.name, permission.resource.name) for permission in role.permissions
        }

        for action, resource in role_permissions:
            if action == permissions.ACTION_CAN_TRIGGER:
                trigger_permission = appbuilder.sm.get_permission(action, resource)
                appbuilder.sm.remove_permission_from_role(role, trigger_permission)

    for action, permission in appbuilder.sm.get_all_permissions():
        if action == permissions.ACTION_CAN_TRIGGER:
            appbuilder.sm.delete_permission(action, permission)


def upgrade():
    """Apply added separate permissions to trigger dag"""
    add_trigger_permissions()


def downgrade():
    """Unapply added separate permissions to trigger dag"""
    undo_trigger_permissions()
