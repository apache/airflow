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
from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

from airflow.cli.cli_config import (
    ARG_DB_FROM_REVISION,
    ARG_DB_FROM_VERSION,
    ARG_DB_REVISION__DOWNGRADE,
    ARG_DB_REVISION__UPGRADE,
    ARG_DB_SKIP_INIT,
    ARG_DB_SQL_ONLY,
    ARG_DB_VERSION__DOWNGRADE,
    ARG_DB_VERSION__UPGRADE,
    ARG_OUTPUT,
    ARG_VERBOSE,
    ARG_YES,
    ActionCommand,
    Arg,
    lazy_load_command,
)

if TYPE_CHECKING:
    import argparse

############
# # ARGS # #
############

# users
ARG_USERNAME = Arg(("-u", "--username"), help="Username of the user", required=True, type=str)
ARG_USERNAME_OPTIONAL = Arg(("-u", "--username"), help="Username of the user", type=str)
ARG_FIRSTNAME = Arg(("-f", "--firstname"), help="First name of the user", required=True, type=str)
ARG_LASTNAME = Arg(("-l", "--lastname"), help="Last name of the user", required=True, type=str)
ARG_ROLE = Arg(
    ("-r", "--role"),
    help="Role of the user. Existing roles include Admin, User, Op, Viewer, and Public",
    required=True,
    type=str,
)
ARG_EMAIL = Arg(("-e", "--email"), help="Email of the user", required=True, type=str)
ARG_EMAIL_OPTIONAL = Arg(("-e", "--email"), help="Email of the user", type=str)
ARG_PASSWORD = Arg(
    ("-p", "--password"),
    help="Password of the user, required to create a user without --use-random-password",
    type=str,
)
ARG_USE_RANDOM_PASSWORD = Arg(
    ("--use-random-password",),
    help="Do not prompt for password. Use random string instead."
    " Required to create a user without --password ",
    default=False,
    action="store_true",
)
ARG_USER_IMPORT = Arg(
    ("import",),
    metavar="FILEPATH",
    help="Import users from JSON file. Example format::\n"
    + textwrap.indent(
        textwrap.dedent(
            """
            [
                {
                    "email": "foo@bar.org",
                    "firstname": "Jon",
                    "lastname": "Doe",
                    "roles": ["Public"],
                    "username": "jondoe"
                }
            ]"""
        ),
        " " * 4,
    ),
)
ARG_USER_EXPORT = Arg(("export",), metavar="FILEPATH", help="Export all users to JSON file")

# roles
ARG_CREATE_ROLE = Arg(("-c", "--create"), help="Create a new role", action="store_true")
ARG_LIST_ROLES = Arg(("-l", "--list"), help="List roles", action="store_true")
ARG_ROLES = Arg(("role",), help="The name of a role", nargs="*")
ARG_PERMISSIONS = Arg(("-p", "--permission"), help="Show role permissions", action="store_true")
ARG_ROLE_RESOURCE = Arg(("-r", "--resource"), help="The name of permissions", nargs="*", required=True)
ARG_ROLE_ACTION = Arg(("-a", "--action"), help="The action of permissions", nargs="*")
ARG_ROLE_ACTION_REQUIRED = Arg(("-a", "--action"), help="The action of permissions", nargs="*", required=True)

ARG_ROLE_IMPORT = Arg(("file",), help="Import roles from JSON file", nargs=None)
ARG_ROLE_EXPORT = Arg(("file",), help="Export all roles to JSON file", nargs=None)
ARG_ROLE_EXPORT_FMT = Arg(
    ("-p", "--pretty"),
    help="Format output JSON file by sorting role names and indenting by 4 spaces",
    action="store_true",
)

# sync-perm
ARG_INCLUDE_DAGS = Arg(
    ("--include-dags",), help="If passed, DAG specific permissions will also be synced.", action="store_true"
)

# permissions cleanup
ARG_DRY_RUN = Arg(
    ("--dry-run",), help="Show what would be cleaned up without making any changes.", action="store_true"
)
ARG_DAG_ID_OPTIONAL = Arg(
    ("-d", "--dag-id"), help="Optional: Clean up permissions for specific DAG ID only", type=str
)

################
# # COMMANDS # #
################

USERS_COMMANDS = (
    ActionCommand(
        name="list",
        help="List users",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.user_command.users_list"),
        args=(ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="create",
        help="Create a user",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.user_command.users_create"),
        args=(
            ARG_ROLE,
            ARG_USERNAME,
            ARG_EMAIL,
            ARG_FIRSTNAME,
            ARG_LASTNAME,
            ARG_PASSWORD,
            ARG_USE_RANDOM_PASSWORD,
            ARG_VERBOSE,
        ),
        epilog=(
            "examples:\n"
            'To create an user with "Admin" role and username equals to "admin", run:\n'
            "\n"
            "    $ airflow users create \\\n"
            "          --username admin \\\n"
            "          --firstname FIRST_NAME \\\n"
            "          --lastname LAST_NAME \\\n"
            "          --role Admin \\\n"
            "          --email admin@example.org"
        ),
    ),
    ActionCommand(
        name="reset-password",
        help="Reset a user's password",
        func=lazy_load_command(
            "airflow.providers.fab.auth_manager.cli_commands.user_command.user_reset_password"
        ),
        args=(
            ARG_USERNAME_OPTIONAL,
            ARG_EMAIL_OPTIONAL,
            ARG_PASSWORD,
            ARG_USE_RANDOM_PASSWORD,
            ARG_VERBOSE,
        ),
        epilog=(
            "examples:\n"
            'To reset an user with username equals to "admin", run:\n'
            "\n"
            "    $ airflow users reset-password \\\n"
            "          --username admin"
        ),
    ),
    ActionCommand(
        name="delete",
        help="Delete a user",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.user_command.users_delete"),
        args=(ARG_USERNAME_OPTIONAL, ARG_EMAIL_OPTIONAL, ARG_VERBOSE),
    ),
    ActionCommand(
        name="add-role",
        help="Add role to a user",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.user_command.add_role"),
        args=(ARG_USERNAME_OPTIONAL, ARG_EMAIL_OPTIONAL, ARG_ROLE, ARG_VERBOSE),
    ),
    ActionCommand(
        name="remove-role",
        help="Remove role from a user",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.user_command.remove_role"),
        args=(ARG_USERNAME_OPTIONAL, ARG_EMAIL_OPTIONAL, ARG_ROLE, ARG_VERBOSE),
    ),
    ActionCommand(
        name="import",
        help="Import users",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.user_command.users_import"),
        args=(ARG_USER_IMPORT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="export",
        help="Export all users",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.user_command.users_export"),
        args=(ARG_USER_EXPORT, ARG_VERBOSE),
    ),
)
ROLES_COMMANDS = (
    ActionCommand(
        name="list",
        help="List roles",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.role_command.roles_list"),
        args=(ARG_PERMISSIONS, ARG_OUTPUT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="create",
        help="Create role",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.role_command.roles_create"),
        args=(ARG_ROLES, ARG_VERBOSE),
    ),
    ActionCommand(
        name="delete",
        help="Delete role",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.role_command.roles_delete"),
        args=(ARG_ROLES, ARG_VERBOSE),
    ),
    ActionCommand(
        name="add-perms",
        help="Add roles permissions",
        func=lazy_load_command(
            "airflow.providers.fab.auth_manager.cli_commands.role_command.roles_add_perms"
        ),
        args=(ARG_ROLES, ARG_ROLE_RESOURCE, ARG_ROLE_ACTION_REQUIRED, ARG_VERBOSE),
    ),
    ActionCommand(
        name="del-perms",
        help="Delete roles permissions",
        func=lazy_load_command(
            "airflow.providers.fab.auth_manager.cli_commands.role_command.roles_del_perms"
        ),
        args=(ARG_ROLES, ARG_ROLE_RESOURCE, ARG_ROLE_ACTION, ARG_VERBOSE),
    ),
    ActionCommand(
        name="export",
        help="Export roles (without permissions) from db to JSON file",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.role_command.roles_export"),
        args=(ARG_ROLE_EXPORT, ARG_ROLE_EXPORT_FMT, ARG_VERBOSE),
    ),
    ActionCommand(
        name="import",
        help="Import roles (without permissions) from JSON file to db",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.role_command.roles_import"),
        args=(ARG_ROLE_IMPORT, ARG_VERBOSE),
    ),
)

SYNC_PERM_COMMAND = ActionCommand(
    name="sync-perm",
    help="Update permissions for existing roles and optionally DAGs",
    func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.sync_perm_command.sync_perm"),
    args=(ARG_INCLUDE_DAGS, ARG_VERBOSE),
)

PERMISSIONS_CLEANUP_COMMAND = ActionCommand(
    name="permissions-cleanup",
    help="Clean up DAG permissions in Flask-AppBuilder tables",
    description=(
        "Clean up DAG-specific permissions. By default, cleans up orphaned permissions "
        "for deleted DAGs. Use --dag-id to clean up permissions for a specific DAG."
    ),
    func=lazy_load_command(
        "airflow.providers.fab.auth_manager.cli_commands.permissions_command.permissions_cleanup"
    ),
    args=(ARG_DAG_ID_OPTIONAL, ARG_DRY_RUN, ARG_YES, ARG_VERBOSE),
    epilog=(
        "examples:\n"
        "To see what orphaned permissions would be cleaned up:\n"
        "    $ airflow fab-auth-manager permissions-cleanup --dry-run\n"
        "To clean up all orphaned permissions:\n"
        "    $ airflow fab-auth-manager permissions-cleanup\n"
        "To clean up permissions for specific DAG:\n"
        "    $ airflow fab-auth-manager permissions-cleanup --dag-id my_dag\n"
        "To clean up without confirmation:\n"
        "    $ airflow fab-auth-manager permissions-cleanup --yes"
    ),
)

DB_COMMANDS = (
    ActionCommand(
        name="migrate",
        help="Migrates the FAB metadata database to the latest version",
        description=(
            "Migrate the schema of the FAB metadata database. "
            "Create the database if it does not exist "
            "To print but not execute commands, use option ``--show-sql-only``. "
            "If using options ``--from-revision`` or ``--from-version``, you must also use "
            "``--show-sql-only``, because if actually *running* migrations, we should only "
            "migrate from the *current* Alembic revision."
        ),
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.db_command.migratedb"),
        args=(
            ARG_DB_REVISION__UPGRADE,
            ARG_DB_VERSION__UPGRADE,
            ARG_DB_SQL_ONLY,
            ARG_DB_FROM_REVISION,
            ARG_DB_FROM_VERSION,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="downgrade",
        help="Downgrade the schema of the FAB metadata database.",
        description=(
            "Downgrade the schema of the FAB metadata database. "
            "You must provide either `--to-revision` or `--to-version`. "
            "To print but not execute commands, use option `--show-sql-only`. "
            "If using options `--from-revision` or `--from-version`, you must also use `--show-sql-only`, "
            "because if actually *running* migrations, we should only migrate from the *current* Alembic "
            "revision."
        ),
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.db_command.downgrade"),
        args=(
            ARG_DB_REVISION__DOWNGRADE,
            ARG_DB_VERSION__DOWNGRADE,
            ARG_DB_SQL_ONLY,
            ARG_YES,
            ARG_DB_FROM_REVISION,
            ARG_DB_FROM_VERSION,
            ARG_VERBOSE,
        ),
    ),
    ActionCommand(
        name="reset",
        help="Burn down and rebuild the FAB metadata database",
        func=lazy_load_command("airflow.providers.fab.auth_manager.cli_commands.db_command.resetdb"),
        args=(ARG_YES, ARG_DB_SKIP_INIT, ARG_VERBOSE),
    ),
)


def get_fab_cli_commands():
    """Return CLI commands for FAB auth manager."""
    import packaging.version

    from airflow import __version__ as airflow_version
    from airflow.cli.cli_config import GroupCommand

    commands = [
        GroupCommand(
            name="users",
            help="Manage users",
            subcommands=USERS_COMMANDS,
        ),
        GroupCommand(
            name="roles",
            help="Manage roles",
            subcommands=ROLES_COMMANDS,
        ),
        SYNC_PERM_COMMAND,  # not in a command group
        PERMISSIONS_CLEANUP_COMMAND,  # single command for permissions cleanup
    ]
    # If Airflow version is 3.0.0 or higher, add the fab-db command group
    if packaging.version.parse(
        packaging.version.parse(airflow_version).base_version
    ) >= packaging.version.parse("3.0.0"):
        commands.append(GroupCommand(name="fab-db", help="Manage FAB", subcommands=DB_COMMANDS))
    return commands


def get_parser() -> argparse.ArgumentParser:
    """
    Generate documentation; used by Sphinx argparse.

    :meta private:
    """
    from airflow.cli.cli_parser import AirflowHelpFormatter, DefaultHelpParser, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in get_fab_cli_commands():
        _add_command(subparsers, group_command)
    return parser
