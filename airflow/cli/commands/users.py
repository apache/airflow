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
"""User sub-commands"""
import getpass
import json
import random
import re
import string
from typing import Any, Dict, List

import rich_click as click
from marshmallow import Schema, fields, validate
from marshmallow.exceptions import ValidationError
from rich.console import Console

from airflow.cli import airflow_cmd, click_output, click_verbose
from airflow.cli.simple_table import AirflowConsole
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning_click_compatible
from airflow.www.app import cached_app


class UserSchema(Schema):
    """user collection item schema"""

    id = fields.Int()
    firstname = fields.Str(required=True)
    lastname = fields.Str(required=True)
    username = fields.Str(required=True)
    email = fields.Email(required=True)
    roles = fields.List(fields.Str, required=True, validate=validate.Length(min=1))


click_email = click.option(
    '-e',
    '--email',
    metavar="EMAIL",
    help="Email of the user",
)
click_username = click.option(
    '-u',
    '--username',
    metavar="USERNAME",
    help="Username of the user",
)
click_role = click.option(
    '-r',
    '--role',
    metavar="ROLE",
    help="""
    Role of the user.

    Existing roles include: Admin, User, Op, Viewer, and Public.
    """,
)


@airflow_cmd.group('users')
def users():
    """Commands for managing users"""


@users.command('list')
@click.pass_context
@click_output
@click_verbose
@suppress_logs_and_warning_click_compatible
def list_(ctx, output, verbose):
    """Lists users at the command line"""
    appbuilder = cached_app().appbuilder
    users = appbuilder.sm.get_all_users()
    fields = ['id', 'username', 'email', 'first_name', 'last_name', 'roles']

    AirflowConsole().print_as(
        data=users, output=output, mapper=lambda x: {f: x.__getattribute__(f) for f in fields}
    )


@users.command('create')
@click.pass_context
@click.option(
    '-f', '--firstname', metavar='FIRSTNAME', required=True, type=str, help="First name of the user"
)
@click.option('-l', '--lastname', metavar='LASTNAME', required=True, type=str, help="Last name of the user")
@click.option('-e', '--email', metavar='EMAIL', required=True, type=str, help="Email of the user")
@click.option('-u', '--username', metavar='USERNAME', required=True, type=str, help="Username of the user")
@click.option(
    '-p',
    '--password',
    metavar='PASSWORD',
    is_flag=False,
    flag_value=False,
    help="Password of the user, required to create a user without --use-random-password",
)
@click.option(
    '--use-random-password',
    is_flag=True,
    default=False,
    help=(
        "Do not prompt for password. Use random string instead. Required to create a user without --password"
    ),
)
@click.option(
    '-r',
    '--role',
    metavar='ROLE',
    required=True,
    help="Role of the user, pre-existing roles include Admin, User, Op, Viewer, and Public",
)
@cli_utils.action_cli(check_db=True)
def create(ctx, firstname, lastname, email, username, password, use_random_password, role):  # noqa: D301
    """
    Creates new user in the DB

    \b
    Example
    To create a user with "Admin" role and username "admin":

    \b
    airflow users create \\
        --username admin \\
        --firstname FIRST_NAME \\
        --lastname LAST_NAME \\
        --role Admin \\
        --email admin@example.org
    """
    console = Console()
    appbuilder = cached_app().appbuilder
    role_ = appbuilder.sm.find_role(role)
    if not role_:
        valid_roles = appbuilder.sm.get_all_roles()
        raise SystemExit(f'{role} is not a valid role. Valid roles are: {valid_roles}')

    if password and use_random_password:
        raise SystemExit('You cannot specify both --password and --use-random-password')

    # Click's password_option isn't aware of the --use-random-password option, so we have to handle
    # setting passwords manually
    if use_random_password:
        password_ = ''.join(random.choice(string.printable) for _ in range(16))
    elif password:
        password_ = password
    else:
        password_ = getpass.getpass('Password:')
        password_confirmation = getpass.getpass('Repeat for confirmation:')
        if password_ != password_confirmation:
            raise SystemExit('Passwords did not match')

    if appbuilder.sm.find_user(username):
        console.print(f'{username} already exist in the db')
        return
    user = appbuilder.sm.add_user(username, firstname, lastname, email, role_, password_)
    if user:
        console.print(f'User "{username}" created with role "{role}"')
    else:
        raise SystemExit('Failed to create user')


def _find_user(username=None, email=None):
    if not username and not email:
        raise SystemExit('Missing args: must supply one of --username or --email')

    if username and email:
        raise SystemExit('Conflicting args: must supply either --username or --email, but not both')

    appbuilder = cached_app().appbuilder

    user = appbuilder.sm.find_user(username=username, email=email)
    if not user:
        raise SystemExit(f'User "{username or email}" does not exist')
    return user


@users.command('delete')
@click.pass_context
@click.option(
    '-e',
    '--email',
    metavar='EMAIL',
    help="Email of the user",
)
@click.option(
    '-u',
    '--username',
    metavar='USERNAME',
    help="Username of the user",
)
@cli_utils.action_cli
def delete(ctx, email, username):
    """Deletes user from DB"""
    user = _find_user(username=username, email=email)

    appbuilder = cached_app().appbuilder

    if appbuilder.sm.del_register_user(user):
        print(f'User "{user.username}" deleted')
    else:
        raise SystemExit('Failed to delete user')


@users.command('add-role')
@click.pass_context
@click_email
@click_username
@click_role
@cli_utils.action_cli
def add_role(ctx, email, username, role):
    """
    Grant a role to a user

    Exactly one of --email or --username must be specified.
    """
    return users_manage_role(email, username, role, remove=False)


@users.command('remove-role')
@click.pass_context
@click_email
@click_username
@click_role
@cli_utils.action_cli
def remove_role(ctx, email, username, role):
    """
    Revoke a role from a user

    Exactly one of --email or --username must be specified.
    """
    return users_manage_role(email, username, role, remove=True)


def users_manage_role(email, username, role, remove=False):
    """Deletes or appends user roles"""
    console = Console()

    user = _find_user(username=username, email=email)

    appbuilder = cached_app().appbuilder

    found_role = appbuilder.sm.find_role(role)
    if not found_role:
        valid_roles = appbuilder.sm.get_all_roles()
        raise SystemExit(f'"{role}" is not a valid role. Valid roles are: {valid_roles}')

    if remove:
        if found_role not in user.roles:
            raise SystemExit(f'User "{user.username}" is not a member of role "{found_role}"')

        user.roles = [r for r in user.roles if r != found_role]
        appbuilder.sm.update_user(user)
        console.print(f'User "{user.username}" removed from role "{found_role}"')
    else:
        if found_role in user.roles:
            raise SystemExit(f'User "{user.username}" is already a member of role "{found_role}"')

        user.roles.append(found_role)
        appbuilder.sm.update_user(user)
        console.print(f'User "{user.username}" added to role "{found_role}"')


@users.command('export')
@click.pass_context
@click.argument('FILEPATH', type=click.Path(exists=True))
@cli_utils.action_cli
def export(ctx, filepath):  # noqa: D301, D412
    """
    Exports all users to the json file

    Arguments:

    FILEPATH    Export users from this JSON file.

    \b
                Example format:
                [
                    {
                        "email": "foo@bar.org",
                        "firstname": "Jon",
                        "lastname": "Doe",
                        "roles": ["Public"],
                        "username": "jdoe"
                    }
                ]
    """
    console = Console()

    appbuilder = cached_app().appbuilder
    all_users = appbuilder.sm.get_all_users()
    fields = ['id', 'username', 'email', 'first_name', 'last_name', 'roles']

    # In the User model the first and last name fields have underscores,
    # but the corresponding parameters in the CLI don't
    def remove_underscores(s):
        return re.sub("_", "", s)

    users_ = [
        {
            remove_underscores(field): user.__getattribute__(field)
            if field != 'roles'
            else [r.name for r in user.roles]
            for field in fields
        }
        for user in all_users
    ]

    with open(filepath, 'w') as file:
        file.write(json.dumps(users_, sort_keys=True, indent=4))
        console.print(f"{len(users)} users successfully exported to {file.name}")


@users.command('import')
@click.pass_context
@click.argument('FILEPATH', type=click.Path(exists=True))
@cli_utils.action_cli
def import_(ctx, filepath):  # noqa: D301, D412
    """
    Imports users from a JSON file

    Arguments:

    FILEPATH    Import users from this JSON file.

    \b
                Example format:
                [
                    {
                        "email": "foo@bar.org",
                        "firstname": "Jon",
                        "lastname": "Doe",
                        "roles": ["Public"],
                        "username": "jdoe"
                    }
                ]
    """
    console = Console()

    users_list = None
    try:
        with open(filepath) as file:
            users_list = json.loads(file.read())
    except ValueError as e:
        raise SystemExit(f"File '{filepath}' is not valid JSON. Error: {e}")

    users_created, users_updated = _import_users(users_list, console=console)
    if users_created:
        console.print("Created the following users:")
        for user in users_created:
            console.print(f"\t{user}")

    if users_updated:
        console.print("Updated the following users:")
        for user in users_updated:
            console.print(f"\t{user}")


def _import_users(users_list: List[Dict[str, Any]], console=None):
    appbuilder = cached_app().appbuilder
    users_created = []
    users_updated = []

    try:
        UserSchema(many=True).load(users_list)
    except ValidationError as e:
        msg = ["Error: Input file didn't pass validation. See below:"]
        for row_num, failure in e.normalized_messages().items():
            msg.append(f'[Item {row_num}]')
            for key, value in failure.items():
                msg.append(f'\t{key}: {value}')
        raise SystemExit('\n'.join(msg))

    for user in users_list:

        roles = []
        for rolename in user['roles']:
            role = appbuilder.sm.find_role(rolename)
            if not role:
                valid_roles = appbuilder.sm.get_all_roles()
                raise SystemExit(f'Error: "{rolename}" is not a valid role. Valid roles are: {valid_roles}')

            roles.append(role)

        existing_user = appbuilder.sm.find_user(email=user['email'])
        if existing_user:
            console.print(f"Found existing user with email '{user['email']}'")
            if existing_user.username != user['username']:
                raise SystemExit(
                    f"Error: Changing the username is not allowed - please delete and recreate the user with"
                    f" email {user['email']!r}"
                )

            existing_user.roles = roles
            existing_user.first_name = user['firstname']
            existing_user.last_name = user['lastname']
            appbuilder.sm.update_user(existing_user)
            users_updated.append(user['email'])
        else:
            console.print(f"Creating new user with email '{user['email']}'")
            appbuilder.sm.add_user(
                username=user['username'],
                first_name=user['firstname'],
                last_name=user['lastname'],
                email=user['email'],
                role=roles,
            )

            users_created.append(user['email'])

    return users_created, users_updated
