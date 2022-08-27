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
import json
import os

from airflow.cli.simple_table import AirflowConsole
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.www.app import cached_app
from airflow.www.security import EXISTING_ROLES


@suppress_logs_and_warning
def roles_list(args):
    """Lists all existing roles"""
    appbuilder = cached_app().appbuilder
    roles = appbuilder.sm.get_all_roles()
    AirflowConsole().print_as(
        data=sorted(r.name for r in roles), output=args.output, mapper=lambda x: {"name": x}
    )


@cli_utils.action_cli
@suppress_logs_and_warning
def roles_create(args):
    """Creates new empty role in DB"""
    appbuilder = cached_app().appbuilder
    for role_name in args.role:
        appbuilder.sm.add_role(role_name)
    print(f"Added {len(args.role)} role(s)")


@cli_utils.action_cli
@suppress_logs_and_warning
def roles_delete(args):
    """Deletes role in DB"""
    appbuilder = cached_app().appbuilder

    for role_name in args.role:
        role = appbuilder.sm.find_role(role_name)
        if not role:
            print(f"Role named '{role_name}' does not exist")
            exit(1)

    for role_name in args.role:
        appbuilder.sm.delete_role(role_name)
    print(f"Deleted {len(args.role)} role(s)")


@suppress_logs_and_warning
def roles_export(args):
    """
    Exports all the rules from the data base to a file.
    Note, this function does not export the permissions associated for each role.
    Strictly, it exports the role names into the passed role json file.
    """
    appbuilder = cached_app().appbuilder
    roles = appbuilder.sm.get_all_roles()
    exporting_roles = [role.name for role in roles if role.name not in EXISTING_ROLES]
    filename = os.path.expanduser(args.file)
    kwargs = {} if not args.pretty else {'sort_keys': True, 'indent': 4}
    with open(filename, 'w', encoding='utf-8') as f:
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
