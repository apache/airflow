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
from __future__ import annotations

__all__ = ["get_fab_cli_commands"]


def get_fab_cli_commands():
    """Return CLI commands for FAB auth manager."""
    import packaging.version

    from airflow import __version__ as airflow_version
    from airflow.cli.cli_config import GroupCommand
    from airflow.providers.fab.auth_manager.cli_commands.definition import (
        DB_COMMANDS,
        PERMISSIONS_CLEANUP_COMMAND,
        ROLES_COMMANDS,
        SYNC_PERM_COMMAND,
        USERS_COMMANDS,
    )

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

