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
"""User sub-commands."""
from __future__ import annotations

import functools
import getpass
import json
import os
import random
import string
from typing import Any

import re2
from marshmallow import Schema, fields, validate
from marshmallow.exceptions import ValidationError

from airflow.auth.managers.fab.cli_commands.utils import get_application_builder
from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import print_export_output
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


class UserSchema(Schema):
    """user collection item schema."""

    id = fields.Int()
    firstname = fields.Str(required=True)
    lastname = fields.Str(required=True)
    username = fields.Str(required=True)
    email = fields.Email(required=True)
    roles = fields.List(fields.Str, required=True, validate=validate.Length(min=1))


@suppress_logs_and_warning
@providers_configuration_loaded
def users_list(args):
    """List users at the command line."""
    with get_application_builder() as appbuilder:
        users = appbuilder.sm.get_all_users()
        fields = ["id", "username", "email", "first_name", "last_name", "roles"]

        AirflowConsole().print_as(
            data=users, output=args.output, mapper=lambda x: {f: x.__getattribute__(f) for f in fields}
        )


@cli_utils.action_cli(check_db=True)
@providers_configuration_loaded
def users_create(args):
    """Create new user in the DB."""
    with get_application_builder() as appbuilder:
        role = appbuilder.sm.find_role(args.role)
        if not role:
            valid_roles = appbuilder.sm.get_all_roles()
            raise SystemExit(f"{args.role} is not a valid role. Valid roles are: {valid_roles}")
        if args.use_random_password:
            password = "".join(random.choices(string.printable, k=16))
        elif args.password:
            password = args.password
        else:
            password = getpass.getpass("Password:")
            password_confirmation = getpass.getpass("Repeat for confirmation:")
            if password != password_confirmation:
                raise SystemExit("Passwords did not match")

        if appbuilder.sm.find_user(args.username):
            print(f"{args.username} already exist in the db")
            return
        user = appbuilder.sm.add_user(
            args.username, args.firstname, args.lastname, args.email, role, password
        )
        if user:
            print(f'User "{args.username}" created with role "{args.role}"')
        else:
            raise SystemExit("Failed to create user")


def _find_user(args):
    if not args.username and not args.email:
        raise SystemExit("Missing args: must supply one of --username or --email")

    if args.username and args.email:
        raise SystemExit("Conflicting args: must supply either --username or --email, but not both")

    with get_application_builder() as appbuilder:
        user = appbuilder.sm.find_user(username=args.username, email=args.email)
        if not user:
            raise SystemExit(f'User "{args.username or args.email}" does not exist')
    return user


@cli_utils.action_cli
@providers_configuration_loaded
def users_delete(args):
    """Delete user from DB."""
    user = _find_user(args)

    # Clear the associated user roles first.
    user.roles.clear()

    with get_application_builder() as appbuilder:
        if appbuilder.sm.del_register_user(user):
            print(f'User "{user.username}" deleted')
        else:
            raise SystemExit("Failed to delete user")


@cli_utils.action_cli
@providers_configuration_loaded
def users_manage_role(args, remove=False):
    """Delete or appends user roles."""
    user = _find_user(args)

    with get_application_builder() as appbuilder:
        role = appbuilder.sm.find_role(args.role)
        if not role:
            valid_roles = appbuilder.sm.get_all_roles()
            raise SystemExit(f'"{args.role}" is not a valid role. Valid roles are: {valid_roles}')

        if remove:
            if role not in user.roles:
                raise SystemExit(f'User "{user.username}" is not a member of role "{args.role}"')

            user.roles = [r for r in user.roles if r != role]
            appbuilder.sm.update_user(user)
            print(f'User "{user.username}" removed from role "{args.role}"')
        else:
            if role in user.roles:
                raise SystemExit(f'User "{user.username}" is already a member of role "{args.role}"')

            user.roles.append(role)
            appbuilder.sm.update_user(user)
            print(f'User "{user.username}" added to role "{args.role}"')


@providers_configuration_loaded
def users_export(args):
    """Export all users to the json file."""
    with get_application_builder() as appbuilder:
        users = appbuilder.sm.get_all_users()
        fields = ["id", "username", "email", "first_name", "last_name", "roles"]

        # In the User model the first and last name fields have underscores,
        # but the corresponding parameters in the CLI don't
        def remove_underscores(s):
            return re2.sub("_", "", s)

        users = [
            {
                remove_underscores(field): user.__getattribute__(field)
                if field != "roles"
                else [r.name for r in user.roles]
                for field in fields
            }
            for user in users
        ]

        with open(args.export, "w") as file:
            file.write(json.dumps(users, sort_keys=True, indent=4))
            print_export_output("users", users, file)


@cli_utils.action_cli
@providers_configuration_loaded
def users_import(args):
    """Import users from the json file."""
    json_file = getattr(args, "import")
    if not os.path.exists(json_file):
        raise SystemExit(f"File '{json_file}' does not exist")

    users_list = None
    try:
        with open(json_file) as file:
            users_list = json.loads(file.read())
    except ValueError as e:
        raise SystemExit(f"File '{json_file}' is not valid JSON. Error: {e}")

    users_created, users_updated = _import_users(users_list)
    if users_created:
        print("Created the following users:\n\t{}".format("\n\t".join(users_created)))

    if users_updated:
        print("Updated the following users:\n\t{}".format("\n\t".join(users_updated)))


def _import_users(users_list: list[dict[str, Any]]):
    with get_application_builder() as appbuilder:
        users_created = []
        users_updated = []

        try:
            UserSchema(many=True).load(users_list)
        except ValidationError as e:
            msg = []
            for row_num, failure in e.normalized_messages().items():
                msg.append(f"[Item {row_num}]")
                for key, value in failure.items():
                    msg.append(f"\t{key}: {value}")
            raise SystemExit(
                "Error: Input file didn't pass validation. See below:\n{}".format("\n".join(msg))
            )

        for user in users_list:
            roles = []
            for rolename in user["roles"]:
                role = appbuilder.sm.find_role(rolename)
                if not role:
                    valid_roles = appbuilder.sm.get_all_roles()
                    raise SystemExit(
                        f'Error: "{rolename}" is not a valid role. Valid roles are: {valid_roles}'
                    )
                roles.append(role)

            existing_user = appbuilder.sm.find_user(email=user["email"])
            if existing_user:
                print(f"Found existing user with email '{user['email']}'")
                if existing_user.username != user["username"]:
                    raise SystemExit(
                        f"Error: Changing the username is not allowed - "
                        f"please delete and recreate the user with email {user['email']!r}"
                    )

                existing_user.roles = roles
                existing_user.first_name = user["firstname"]
                existing_user.last_name = user["lastname"]
                appbuilder.sm.update_user(existing_user)
                users_updated.append(user["email"])
            else:
                print(f"Creating new user with email '{user['email']}'")
                appbuilder.sm.add_user(
                    username=user["username"],
                    first_name=user["firstname"],
                    last_name=user["lastname"],
                    email=user["email"],
                    role=roles,
                )

                users_created.append(user["email"])

        return users_created, users_updated


add_role = functools.partial(users_manage_role, remove=False)
remove_role = functools.partial(users_manage_role, remove=True)
