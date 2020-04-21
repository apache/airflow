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
import functools
import getpass
import json
import os
import random
import re
import string

from tabulate import tabulate

from airflow.utils import cli as cli_utils
from airflow.www.app import cached_appbuilder


def users_list(args):
    """Lists users at the command line"""
    appbuilder = cached_appbuilder()
    users = appbuilder.sm.get_all_users()
    fields = ['id', 'username', 'email', 'first_name', 'last_name', 'roles']
    users = [[user.__getattribute__(field) for field in fields] for user in users]
    msg = tabulate(users, [field.capitalize().replace('_', ' ') for field in fields],
                   tablefmt=args.output)
    print(msg)


@cli_utils.action_logging
def users_create(args):
    """Creates new user in the DB"""
    appbuilder = cached_appbuilder()
    role = appbuilder.sm.find_role(args.role)
    if not role:
        valid_roles = appbuilder.sm.get_all_roles()
        raise SystemExit('{} is not a valid role. Valid roles are: {}'.format(args.role, valid_roles))

    if args.use_random_password:
        password = ''.join(random.choice(string.printable) for _ in range(16))
    elif args.password:
        password = args.password
    else:
        password = getpass.getpass('Password:')
        password_confirmation = getpass.getpass('Repeat for confirmation:')
        if password != password_confirmation:
            raise SystemExit('Passwords did not match!')

    if appbuilder.sm.find_user(args.username):
        print('{} already exist in the db'.format(args.username))
        return
    user = appbuilder.sm.add_user(args.username, args.firstname, args.lastname,
                                  args.email, role, password)
    if user:
        print('{} user {} created.'.format(args.role, args.username))
    else:
        raise SystemExit('Failed to create user.')


@cli_utils.action_logging
def users_delete(args):
    """Deletes user from DB"""
    appbuilder = cached_appbuilder()

    try:
        user = next(u for u in appbuilder.sm.get_all_users()
                    if u.username == args.username)
    except StopIteration:
        raise SystemExit('{} is not a valid user.'.format(args.username))

    if appbuilder.sm.del_register_user(user):
        print('User {} deleted.'.format(args.username))
    else:
        raise SystemExit('Failed to delete user.')


@cli_utils.action_logging
def users_manage_role(args, remove=False):
    """Deletes or appends user roles"""
    if not args.username and not args.email:
        raise SystemExit('Missing args: must supply one of --username or --email')

    if args.username and args.email:
        raise SystemExit('Conflicting args: must supply either --username'
                         ' or --email, but not both')

    appbuilder = cached_appbuilder()
    user = (appbuilder.sm.find_user(username=args.username) or
            appbuilder.sm.find_user(email=args.email))
    if not user:
        raise SystemExit('User "{}" does not exist'.format(
            args.username or args.email))

    role = appbuilder.sm.find_role(args.role)
    if not role:
        valid_roles = appbuilder.sm.get_all_roles()
        raise SystemExit('{} is not a valid role. Valid roles are: {}'.format(args.role, valid_roles))

    if remove:
        if role in user.roles:
            user.roles = [r for r in user.roles if r != role]
            appbuilder.sm.update_user(user)
            print('User "{}" removed from role "{}".'.format(
                user,
                args.role))
        else:
            raise SystemExit('User "{}" is not a member of role "{}".'.format(
                user,
                args.role))
    else:
        if role in user.roles:
            raise SystemExit('User "{}" is already a member of role "{}".'.format(
                user,
                args.role))
        else:
            user.roles.append(role)
            appbuilder.sm.update_user(user)
            print('User "{}" added to role "{}".'.format(
                user,
                args.role))


def users_export(args):
    """Exports all users to the json file"""
    appbuilder = cached_appbuilder()
    users = appbuilder.sm.get_all_users()
    fields = ['id', 'username', 'email', 'first_name', 'last_name', 'roles']

    # In the User model the first and last name fields have underscores,
    # but the corresponding parameters in the CLI don't
    def remove_underscores(s):
        return re.sub("_", "", s)

    users = [
        {remove_underscores(field): user.__getattribute__(field)
            if field != 'roles' else [r.name for r in user.roles]
            for field in fields}
        for user in users
    ]

    with open(args.export, 'w') as file:
        file.write(json.dumps(users, sort_keys=True, indent=4))
        print("{} users successfully exported to {}".format(len(users), file.name))


@cli_utils.action_logging
def users_import(args):
    """Imports users from the json file"""
    json_file = getattr(args, 'import')
    if not os.path.exists(json_file):
        print("File '{}' does not exist")
        exit(1)

    users_list = None  # pylint: disable=redefined-outer-name
    try:
        with open(json_file, 'r') as file:
            users_list = json.loads(file.read())
    except ValueError as e:
        print("File '{}' is not valid JSON. Error: {}".format(json_file, e))
        exit(1)

    users_created, users_updated = _import_users(users_list)
    if users_created:
        print("Created the following users:\n\t{}".format(
            "\n\t".join(users_created)))

    if users_updated:
        print("Updated the following users:\n\t{}".format(
            "\n\t".join(users_updated)))


def _import_users(users_list):  # pylint: disable=redefined-outer-name
    appbuilder = cached_appbuilder()
    users_created = []
    users_updated = []

    for user in users_list:
        roles = []
        for rolename in user['roles']:
            role = appbuilder.sm.find_role(rolename)
            if not role:
                valid_roles = appbuilder.sm.get_all_roles()
                print("Error: '{}' is not a valid role. Valid roles are: {}".format(rolename, valid_roles))
                exit(1)
            else:
                roles.append(role)

        required_fields = ['username', 'firstname', 'lastname',
                           'email', 'roles']
        for field in required_fields:
            if not user.get(field):
                print("Error: '{}' is a required field, but was not "
                      "specified".format(field))
                exit(1)

        existing_user = appbuilder.sm.find_user(email=user['email'])
        if existing_user:
            print("Found existing user with email '{}'".format(user['email']))
            existing_user.roles = roles
            existing_user.first_name = user['firstname']
            existing_user.last_name = user['lastname']

            if existing_user.username != user['username']:
                print("Error: Changing the username is not allowed - "
                      "please delete and recreate the user with "
                      "email '{}'".format(user['email']))
                exit(1)

            appbuilder.sm.update_user(existing_user)
            users_updated.append(user['email'])
        else:
            print("Creating new user with email '{}'".format(user['email']))
            appbuilder.sm.add_user(
                username=user['username'],
                first_name=user['firstname'],
                last_name=user['lastname'],
                email=user['email'],
                role=roles[0],  # add_user() requires exactly 1 role
            )

            if len(roles) > 1:
                new_user = appbuilder.sm.find_user(email=user['email'])
                new_user.roles = roles
                appbuilder.sm.update_user(new_user)

            users_created.append(user['email'])

    return users_created, users_updated


add_role = functools.partial(users_manage_role, remove=False)
remove_role = functools.partial(users_manage_role, remove=True)
