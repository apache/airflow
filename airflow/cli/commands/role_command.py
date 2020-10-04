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
#
"""Roles sub-commands"""
from tabulate import tabulate

from airflow.utils import cli as cli_utils
from airflow.www.app import cached_app


def roles_list(args):
    """Lists all existing roles"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    roles = appbuilder.sm.get_all_roles()
    print("Existing roles:\n")
    role_names = sorted([[r.name] for r in roles])
    msg = tabulate(role_names,
                   headers=['Role'],
                   tablefmt=args.output)
    print(msg)


@cli_utils.action_logging
def roles_create(args):
    """Creates new empty role in DB"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    for role_name in args.role:
        appbuilder.sm.add_role(role_name)

def prefixed_dag_id(dag_id):
    if dag_id == 'all_dags':
        return 'Dag'
    if dag_id.startswith("DAG:"):
        return dag_id
    return f"DAG:{dag_id}"

def raw_dag_id(dag_id):
    if dag_id == 'all_dags':
        return 'Dag'
    if dag_id.startswith("DAG:"):
        return dag_id[len("DAG:")]
    return f"DAG:{dag_id}"

def new_dag_action(old_action):
    if old_action == 'can_dag_read':
        return 'can_read'
    return 'can_edit'

def roles_upgrade(args):
    """Creates new empty role in DB"""
    appbuilder = cached_app().appbuilder  # pylint: disable=no-member
    roles = appbuilder.sm.get_all_roles()
    to_remove = []
    for role in roles:
        for permission in role.permissions:
            resource = permission.view_menu
            action = permission.permission
            # print(action.name)
            print(action.name)
            if action.name in ['can_dag_read', 'can_dag_edit']:
                to_remove.append((role, permission))
                new_action = new_dag_action(action.name)
                pv = appbuilder.sm.add_permission_view_menu(new_action, prefixed_dag_id(resource.name))
                appbuilder.sm.add_permission_role(role, pv)

                print(f"{role.name}: {resource.name}.{action.name} -> {pv.view_menu.name}.{new_action}")

    for (role, permission) in to_remove:
        # breakpoint()
        appbuilder.sm.del_permission_role(role, permission)
    # breakpoint()
    # breakpoint()
    for (role, permission) in to_remove:
        if appbuilder.sm.find_permission_view_menu(permission.permission.name, permission.view_menu.name):
        # breakpoint()
            appbuilder.sm.del_permission_view_menu(permission.permission.name, permission.view_menu.name)
    # breakpoint()
    for (role, permission) in to_remove:
        if appbuilder.sm.find_view_menu(permission.view_menu.name):
            appbuilder.sm.del_view_menu(permission.view_menu.name)

    roles = appbuilder.sm.get_all_roles()

def add_dag_perms():
    dags = ["example_bash_operator", "example_branch_dop_operator_v3", "example_branch_operator", "example_complex", "example_external_task_marker_child"]
    for dag in dags:
        cached_app().appbuilder.sm.sync_perm_for_dag(  # type: ignore  # pylint: disable=no-member
                dag, access_control={'Admin': ['can_dag_edit', 'can_dag_read']}
            )
        cached_app().appbuilder.sm.sync_perm_for_dag(  # type: ignore  # pylint: disable=no-member
                dag, access_control={'User': ['can_dag_read']}
            )

if __name__ == '__main__':
    # add_dag_perms()

    roles_upgrade({"output": None})
