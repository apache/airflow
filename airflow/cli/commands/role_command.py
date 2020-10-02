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
    outdated_dag_permissions = set()
    for role in roles:
        breakpoint()
        for permission in role.permissions:
            resource = permission.view_menu
            action = permission.permission
            print(action.name)
            if action.name in ['can_dag_read', 'can_dag_edit']:
                appbuilder.sm.del_permission_role(role, permission)
                appbuilder.sm.del_permission_view_menu(action.name, resource.name)
                if not appbuilder.sm.find_permissions_view_menu(resource):
                    appbuilder.sm.del_view_menu(resource.name)
                new_action = new_dag_action(action.name)
                pv = appbuilder.sm.add_permission_view_menu(new_action, prefixed_dag_id(resource.name))
                appbuilder.sm.add_permission_role(role, pv)
                # breakpoint()
                print(f"{role.name}: {resource.name}.{action.name} -> {pv.view_menu.name}.{new_action}")

if __name__ == '__main__':
    roles_upgrade({"output": None})
