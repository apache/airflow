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

import argparse
import getpass

from airflow.cli.cli_config import (
    ActionCommand,
    Arg,
    lazy_load_command,
)


class Password(argparse.Action):
    """Custom action to prompt for password input."""

    def __call__(self, parser, namespace, values, option_string=None):
        if values is None:
            values = getpass.getpass(prompt="Password: ")
        setattr(namespace, self.dest, values)


############
# # ARGS # #
############

ARG_USERNAME = Arg(
    ("--username",),
    help="Username associated to the user used to create resources",
)
ARG_PASSWORD = Arg(
    ("--password",),
    help="Password associated to the user used to create resources. If not provided, you will be prompted to enter it.",
    action=Password,
    nargs="?",
    dest="password",
    type=str,
)
ARG_USER_REALM = Arg(
    ("--user-realm",), help="Realm name where the user used to create resources is", default="master"
)
ARG_CLIENT_ID = Arg(("--client-id",), help="ID of the client used to create resources", default="admin-cli")
ARG_DRY_RUN = Arg(
    ("--dry-run",),
    help="Perform a dry run without creating any resources",
    action="store_true",
)


################
# # COMMANDS # #
################

KEYCLOAK_AUTH_MANAGER_COMMANDS = (
    ActionCommand(
        name="create-scopes",
        help="Create scopes in Keycloak",
        func=lazy_load_command("airflow.providers.keycloak.auth_manager.cli.commands.create_scopes_command"),
        args=(ARG_USERNAME, ARG_PASSWORD, ARG_USER_REALM, ARG_CLIENT_ID, ARG_DRY_RUN),
    ),
    ActionCommand(
        name="create-resources",
        help="Create resources in Keycloak",
        func=lazy_load_command(
            "airflow.providers.keycloak.auth_manager.cli.commands.create_resources_command"
        ),
        args=(ARG_USERNAME, ARG_PASSWORD, ARG_USER_REALM, ARG_CLIENT_ID, ARG_DRY_RUN),
    ),
    ActionCommand(
        name="create-permissions",
        help="Create permissions in Keycloak",
        func=lazy_load_command(
            "airflow.providers.keycloak.auth_manager.cli.commands.create_permissions_command"
        ),
        args=(ARG_USERNAME, ARG_PASSWORD, ARG_USER_REALM, ARG_CLIENT_ID, ARG_DRY_RUN),
    ),
    ActionCommand(
        name="create-all",
        help="Create all entities (scopes, resources and permissions) in Keycloak",
        func=lazy_load_command("airflow.providers.keycloak.auth_manager.cli.commands.create_all_command"),
        args=(ARG_USERNAME, ARG_PASSWORD, ARG_USER_REALM, ARG_CLIENT_ID, ARG_DRY_RUN),
    ),
)


def get_keycloak_cli_commands():
    """Return CLI commands for Keycloak auth manager."""
    from airflow.cli.cli_config import GroupCommand

    return [
        GroupCommand(
            name="keycloak-auth-manager",
            help="Manage resources used by Keycloak auth manager",
            subcommands=KEYCLOAK_AUTH_MANAGER_COMMANDS,
        ),
    ]
