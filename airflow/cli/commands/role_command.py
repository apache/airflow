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
"""Roles sub-commands."""
from __future__ import annotations

import collections
import itertools
import json
import os

from flask import Flask

from airflow.cli.simple_table import AirflowConsole
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.www.app import cached_app
from airflow.www.extensions.init_appbuilder import init_appbuilder
from airflow.www.fab_security.sqla.models import Action, Permission, Resource, Role
from airflow.www.security import EXISTING_ROLES, AirflowSecurityManager


@suppress_logs_and_warning
def roles_list(args):
    """Lists all existing roles."""
    flask_app = Flask(__name__)
    with flask_app.app_context():
        appbuilder = init_appbuilder(flask_app)
        roles = appbuilder.sm.get_all_roles()

    if not args.permission:
        AirflowConsole().print_as(
            data=sorted(r.name for r in roles), output=args.output, mapper=lambda x: {"name": x}
        )
        return

    permission_map: dict[tuple[str, str], list[str]] = collections.defaultdict(list)
    for role in roles:
        for permission in role.permissions:
            permission_map[(role.name, permission.resource.name)].append(permission.action.name)

    AirflowConsole().print_as(
        data=sorted(permission_map),
        output=args.output,
        mapper=lambda x: {"name": x[0], "resource": x[1], "action": ",".join(sorted(permission_map[x]))},
    )


@cli_utils.action_cli
@suppress_logs_and_warning
def roles_create(args):
    """Creates new empty role in DB."""
    appbuilder = cached_app().appbuilder
    for role_name in args.role:
        appbuilder.sm.add_role(role_name)
    print(f"Added {len(args.role)} role(s)")


@cli_utils.action_cli
@suppress_logs_and_warning
def roles_delete(args):
    """Deletes role in DB."""
    appbuilder = cached_app().appbuilder

    for role_name in args.role:
        role = appbuilder.sm.find_role(role_name)
        if not role:
            print(f"Role named '{role_name}' does not exist")
            exit(1)

    for role_name in args.role:
        appbuilder.sm.delete_role(role_name)
    print(f"Deleted {len(args.role)} role(s)")


def __roles_add_or_remove_permissions(args):
    asm: AirflowSecurityManager = cached_app().appbuilder.sm
    is_add: bool = args.subcommand.startswith("add")

    role_map = {}
    perm_map: dict[tuple[str, str], set[str]] = collections.defaultdict(set)
    for name in args.role:
        role: Role | None = asm.find_role(name)
        if not role:
            print(f"Role named '{name}' does not exist")
            exit(1)

        role_map[name] = role
        for permission in role.permissions:
            perm_map[(name, permission.resource.name)].add(permission.action.name)

    for name in args.resource:
        resource: Resource | None = asm.get_resource(name)
        if not resource:
            print(f"Resource named '{name}' does not exist")
            exit(1)

    for name in args.action or []:
        action: Action | None = asm.get_action(name)
        if not action:
            print(f"Action named '{name}' does not exist")
            exit(1)

    permission_count = 0
    for (role_name, resource_name, action_name) in list(
        itertools.product(args.role, args.resource, args.action or [None])
    ):
        res_key = (role_name, resource_name)
        if is_add and action_name not in perm_map[res_key]:
            perm: Permission | None = asm.create_permission(action_name, resource_name)
            asm.add_permission_to_role(role_map[role_name], perm)
            print(f"Added {perm} to role {role_name}")
            permission_count += 1
        elif not is_add and res_key in perm_map:
            for _action_name in perm_map[res_key] if action_name is None else [action_name]:
                perm: Permission | None = asm.get_permission(_action_name, resource_name)
                asm.remove_permission_from_role(role_map[role_name], perm)
                print(f"Deleted {perm} from role {role_name}")
                permission_count += 1

    print(f"{'Added' if is_add else 'Deleted'} {permission_count} permission(s)")


@cli_utils.action_cli
@suppress_logs_and_warning
def roles_add_perms(args):
    """Adds permissions to role in DB."""
    __roles_add_or_remove_permissions(args)


@cli_utils.action_cli
@suppress_logs_and_warning
def roles_del_perms(args):
    """Deletes permissions from role in DB."""
    __roles_add_or_remove_permissions(args)


@suppress_logs_and_warning
def roles_export(args):
    """
    Exports all the roles from the database to a file.

    Note, this function does not export the permissions associated for each role.
    Strictly, it exports the role names into the passed role json file.
    """
    appbuilder = cached_app().appbuilder
    roles = appbuilder.sm.get_all_roles()
    exporting_roles = [role.name for role in roles if role.name not in EXISTING_ROLES]
    filename = os.path.expanduser(args.file)
    kwargs = {} if not args.pretty else {"sort_keys": True, "indent": 4}
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(exporting_roles, f, **kwargs)
    print(f"{len(exporting_roles)} roles successfully exported to {filename}")


@cli_utils.action_cli
@suppress_logs_and_warning
def roles_import(args):
    """
    Import all the roles into the db from the given json file.

    Note, this function does not import the permissions for different roles and import them as well.
    Strictly, it imports the role names in the role json file passed.
    """
    json_file = args.file
    try:
        with open(json_file) as f:
            role_list = json.load(f)
    except FileNotFoundError:
        print(f"File '{json_file}' does not exist")
        exit(1)
    except ValueError as e:
        print(f"File '{json_file}' is not a valid JSON file. Error: {e}")
        exit(1)
    appbuilder = cached_app().appbuilder
    existing_roles = [role.name for role in appbuilder.sm.get_all_roles()]
    roles_to_import = [role for role in role_list if role not in existing_roles]
    for role_name in roles_to_import:
        appbuilder.sm.add_role(role_name)
    print(f"roles '{roles_to_import}' successfully imported")
