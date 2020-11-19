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

"""Prefix DAG permissions.

Revision ID: 849da589634d
Revises: 03afc6b6f902
Create Date: 2020-10-01 17:25:10.006322

"""

from airflow.security import permissions
from airflow.www.app import cached_app

# revision identifiers, used by Alembic.
revision = '849da589634d'
down_revision = '03afc6b6f902'
branch_labels = None
depends_on = None


def upgrade():  # noqa: D103
    permissions = ['can_dag_read', 'can_dag_edit']
    view_menus = cached_app().appbuilder.sm.get_all_view_menu()
    convert_permissions(permissions, view_menus, upgrade_action, upgrade_dag_id)


def downgrade():  # noqa: D103
    permissions = ['can_read', 'can_edit']
    vms = cached_app().appbuilder.sm.get_all_view_menu()
    view_menus = [vm for vm in vms if (vm.name == permissions.RESOURCE_DAG or vm.name.startswith('DAG:'))]
    convert_permissions(permissions, view_menus, downgrade_action, downgrade_dag_id)


def upgrade_dag_id(dag_id):
    """Adds the 'DAG:' prefix to a DAG view if appropriate."""
    if dag_id == 'all_dags':
        return permissions.RESOURCE_DAG
    if dag_id.startswith("DAG:"):
        return dag_id
    return f"DAG:{dag_id}"


def downgrade_dag_id(dag_id):
    """Removes the 'DAG:' prefix from a DAG view name to return the DAG id."""
    if dag_id == permissions.RESOURCE_DAG:
        return 'all_dags'
    if dag_id.startswith("DAG:"):
        return dag_id[len("DAG:") :]
    return dag_id


def upgrade_action(action):
    """Converts the a DAG permission name from the old style to the new style."""
    if action == 'can_dag_read':
        return 'can_read'
    return 'can_edit'


def downgrade_action(action):
    """Converts the a DAG permission name from the old style to the new style."""
    if action == 'can_read':
        return 'can_dag_read'
    return 'can_dag_edit'


def convert_permissions(permissions, view_menus, convert_action, convert_dag_id):
    """Creates new empty role in DB"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    roles = appbuilder.sm.get_all_roles()
    views_to_remove = set()
    for permission_name in permissions:  # pylint: disable=too-many-nested-blocks
        for view_menu in view_menus:
            view_name = view_menu.name
            old_pvm = appbuilder.sm.find_permission_view_menu(permission_name, view_name)
            if not old_pvm:
                continue

            views_to_remove.add(view_name)
            new_permission_name = convert_action(permission_name)
            new_pvm = appbuilder.sm.add_permission_view_menu(new_permission_name, convert_dag_id(view_name))
            for role in roles:
                if appbuilder.sm.exist_permission_on_roles(view_name, permission_name, [role.id]):
                    appbuilder.sm.add_permission_role(role, new_pvm)
                    appbuilder.sm.del_permission_role(role, old_pvm)
                    print(f"DELETING: {role.name}  ---->   {view_name}.{permission_name}")
            appbuilder.sm.del_permission_view_menu(permission_name, view_name)
            print(f"DELETING: perm_view  ---->   {view_name}.{permission_name}")
    for view_name in views_to_remove:
        if appbuilder.sm.find_view_menu(view_name):
            appbuilder.sm.del_view_menu(view_name)
            print(f"DELETING: view_menu  ---->   {view_name}")

    if 'can_dag_read' in permissions:
        for permission_name in permissions:
            if appbuilder.sm.find_permission(permission_name):
                appbuilder.sm.del_permission(permission_name)
                print(f"DELETING: permission  ---->   {permission_name}")
