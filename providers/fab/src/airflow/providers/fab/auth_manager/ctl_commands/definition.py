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
"""
FAB provider commands exposed through ``airflowctl``.

Discovered via the ``ctl:`` field in ``providers/fab/provider.yaml``, which
the breeze pyproject toolchain folds into the runtime ``get_provider_info()``
payload. ``airflowctl`` then imports each entry and calls it to build the
command tree at parser-construction time.

Every command maps 1:1 to a route under
``airflow.providers.fab.auth_manager.api_fastapi.routes`` — no read-modify-write
composition, no batch helpers. From v3 onward those routes are the only
contract ``airflowctl`` is willing to depend on, so anything not directly
backed by a single route is intentionally omitted.

All handlers in this module talk to the FAB auth-manager Public API
(``/auth/fab/v1/``) through the ``airflowctl`` HTTP client. They never import
``airflow-core`` models or hit the metadata DB directly, in line with AIP-94
(see ``contributing-docs/27_cli_implementation_guide.rst``).
"""

from __future__ import annotations

import argparse

from airflowctl.ctl.cli_config import (
    ARG_OUTPUT,
    ActionCommand,
    Arg,
    CLICommand,
    GroupCommand,
    lazy_load_command,
)

# ----- shared args -----

ARG_LIMIT = Arg(
    flags=("--limit",),
    type=int,
    default=100,
    help="Maximum number of items to return.",
)
ARG_OFFSET = Arg(
    flags=("--offset",),
    type=int,
    default=0,
    help="Number of items to skip before starting to collect results.",
)
ARG_ORDER_BY = Arg(
    flags=("--order-by",),
    type=str,
    default="id",
    dest="order_by",
    help="Field to order by. Prefix with '-' for descending.",
)
ARG_UPDATE_MASK = Arg(
    flags=("--update-mask",),
    type=str,
    default=None,
    dest="update_mask",
    help=(
        "Comma-separated list of fields to update. "
        "If omitted, derived from whichever optional flags you passed."
    ),
)

# ----- user args -----

ARG_USERNAME_POS = Arg(flags=("username",), type=str, help="Username of the user.")
ARG_USER_EMAIL_REQ = Arg(flags=("-e", "--email"), type=str, required=True, help="Email of the user.")
ARG_USER_FIRSTNAME_REQ = Arg(
    flags=("-f", "--firstname"),
    type=str,
    required=True,
    help="First name of the user.",
)
ARG_USER_LASTNAME_REQ = Arg(
    flags=("-l", "--lastname"),
    type=str,
    required=True,
    help="Last name of the user.",
)
ARG_USER_PASSWORD_REQ = Arg(
    flags=("-p", "--password"),
    type=str,
    required=True,
    help="Password of the user.",
)
ARG_USER_ROLES_OPTIONAL = Arg(
    flags=("-r", "--role"),
    type=str,
    nargs="+",
    default=None,
    help="One or more role names to assign.",
)

# Optional variants for ``users update`` (every field optional).
ARG_USER_EMAIL_OPT = Arg(flags=("--email",), type=str, default=None, help="New email.")
ARG_USER_FIRSTNAME_OPT = Arg(flags=("--firstname",), type=str, default=None, help="New first name.")
ARG_USER_LASTNAME_OPT = Arg(flags=("--lastname",), type=str, default=None, help="New last name.")
ARG_USER_PASSWORD_OPT = Arg(flags=("--password",), type=str, default=None, help="New password.")
ARG_USER_ROLES_OPT = Arg(
    flags=("--role",),
    type=str,
    nargs="+",
    default=None,
    help="Replace the user's roles with this list.",
)

# ----- role args -----

ARG_ROLE_NAME_POS = Arg(flags=("name",), type=str, help="Role name.")
ARG_ROLE_NEW_NAME = Arg(
    flags=("--new-name",),
    type=str,
    default=None,
    dest="new_name",
    help="Rename the role to this value.",
)
ARG_ROLE_ACTIONS_JSON = Arg(
    flags=("--actions",),
    type=str,
    default=None,
    help=(
        "JSON array of action-resource pairs, e.g. "
        '\'[{"action": {"name": "can_read"}, "resource": {"name": "Dag"}}]\'.'
    ),
)


# ----- users group -----

USERS_COMMANDS = (
    ActionCommand(
        name="list",
        help="List FAB users (GET /fab/v1/users).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.users_command.list_users"),
        args=(ARG_LIMIT, ARG_OFFSET, ARG_ORDER_BY, ARG_OUTPUT),
    ),
    ActionCommand(
        name="get",
        help="Get a FAB user by username (GET /fab/v1/users/{username}).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.users_command.get_user"),
        args=(ARG_USERNAME_POS, ARG_OUTPUT),
    ),
    ActionCommand(
        name="create",
        help="Create a FAB user (POST /fab/v1/users).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.users_command.create_user"),
        args=(
            ARG_USERNAME_POS,
            ARG_USER_EMAIL_REQ,
            ARG_USER_FIRSTNAME_REQ,
            ARG_USER_LASTNAME_REQ,
            ARG_USER_PASSWORD_REQ,
            ARG_USER_ROLES_OPTIONAL,
            ARG_OUTPUT,
        ),
    ),
    ActionCommand(
        name="update",
        help="Update a FAB user (PATCH /fab/v1/users/{username}).",
        description=(
            "Update a FAB user. Only the fields you pass are sent; ``update_mask`` is "
            "derived automatically from those fields. Use ``--update-mask`` to override."
        ),
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.users_command.update_user"),
        args=(
            ARG_USERNAME_POS,
            ARG_USER_EMAIL_OPT,
            ARG_USER_FIRSTNAME_OPT,
            ARG_USER_LASTNAME_OPT,
            ARG_USER_PASSWORD_OPT,
            ARG_USER_ROLES_OPT,
            ARG_UPDATE_MASK,
            ARG_OUTPUT,
        ),
    ),
    ActionCommand(
        name="delete",
        help="Delete a FAB user (DELETE /fab/v1/users/{username}).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.users_command.delete_user"),
        args=(ARG_USERNAME_POS,),
    ),
)

# ----- roles group -----

ROLES_COMMANDS = (
    ActionCommand(
        name="list",
        help="List FAB roles (GET /fab/v1/roles).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.roles_command.list_roles"),
        args=(ARG_LIMIT, ARG_OFFSET, ARG_ORDER_BY, ARG_OUTPUT),
    ),
    ActionCommand(
        name="get",
        help="Get a FAB role by name (GET /fab/v1/roles/{name}).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.roles_command.get_role"),
        args=(ARG_ROLE_NAME_POS, ARG_OUTPUT),
    ),
    ActionCommand(
        name="create",
        help="Create a FAB role (POST /fab/v1/roles).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.roles_command.create_role"),
        args=(ARG_ROLE_NAME_POS, ARG_ROLE_ACTIONS_JSON, ARG_OUTPUT),
    ),
    ActionCommand(
        name="update",
        help="Update a FAB role (PATCH /fab/v1/roles/{name}).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.roles_command.update_role"),
        args=(ARG_ROLE_NAME_POS, ARG_ROLE_NEW_NAME, ARG_ROLE_ACTIONS_JSON, ARG_UPDATE_MASK, ARG_OUTPUT),
    ),
    ActionCommand(
        name="delete",
        help="Delete a FAB role (DELETE /fab/v1/roles/{name}).",
        func=lazy_load_command("airflow.providers.fab.auth_manager.ctl_commands.roles_command.delete_role"),
        args=(ARG_ROLE_NAME_POS,),
    ),
)

# ----- permissions group -----

PERMISSIONS_COMMANDS = (
    ActionCommand(
        name="list",
        help="List FAB permissions (GET /fab/v1/permissions).",
        func=lazy_load_command(
            "airflow.providers.fab.auth_manager.ctl_commands.permissions_command.list_permissions"
        ),
        args=(ARG_LIMIT, ARG_OFFSET, ARG_ORDER_BY, ARG_OUTPUT),
    ),
)


def get_fab_airflowctl_commands() -> list[CLICommand]:
    """
    Return the FAB provider's airflowctl commands.

    Referenced from ``providers/fab/provider.yaml`` under the ``ctl:`` field.
    """
    return [
        GroupCommand(
            name="users",
            help="Manage FAB users via the auth-manager Public API.",
            subcommands=USERS_COMMANDS,
        ),
        GroupCommand(
            name="roles",
            help="Manage FAB roles via the auth-manager Public API.",
            subcommands=ROLES_COMMANDS,
        ),
        GroupCommand(
            name="permissions",
            help="Inspect FAB permissions via the auth-manager Public API.",
            subcommands=PERMISSIONS_COMMANDS,
        ),
    ]


def get_parser() -> argparse.ArgumentParser:
    """
    Build a parser scoped to FAB's airflowctl commands for documentation rendering.

    Used by ``sphinx-argparse`` from ``providers/fab/docs/airflowctl-ref.rst``;
    not used at runtime. Mirrors the ``get_parser`` helper in
    ``airflow.providers.fab.cli.definition`` (which serves the core ``airflow``
    CLI ref).

    :meta private:
    """
    from airflowctl.ctl.cli_config import GroupCommandParser
    from airflowctl.ctl.cli_parser import (
        AirflowHelpFormatter,
        DefaultHelpParser,
        _add_command,
    )

    parser = DefaultHelpParser(prog="airflowctl", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for command in get_fab_airflowctl_commands():
        _add_command(
            subparsers,
            GroupCommandParser.from_group_command(command) if isinstance(command, GroupCommand) else command,
        )
    return parser
