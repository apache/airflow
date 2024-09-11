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

from typing import TYPE_CHECKING, Iterable

from airflow.cli.cli_parser import GroupCommand, airflow_commands
from airflow.cli.simple_table import AirflowConsole, SimpleTable
from airflow.utils.cli import suppress_logs_and_warning

if TYPE_CHECKING:
    from airflow.cli.cli_parser import ActionCommand


@suppress_logs_and_warning
def cheat_sheet(args):
    """Display cheat-sheet."""
    display_commands_index()


def display_commands_index():
    """Display list of all commands."""

    def display_recursive(
        prefix: list[str],
        commands: Iterable[GroupCommand | ActionCommand],
        help_msg: str | None = None,
    ):
        actions: list[ActionCommand] = []
        groups: list[GroupCommand] = []
        for command in commands:
            if isinstance(command, GroupCommand):
                groups.append(command)
            else:
                actions.append(command)

        console = AirflowConsole()
        if actions:
            table = SimpleTable(title=help_msg or "Miscellaneous commands")
            table.add_column(width=46)
            table.add_column()
            for action_command in sorted(actions, key=lambda d: d.name):
                table.add_row(" ".join([*prefix, action_command.name]), action_command.help)
            console.print(table)

        if groups:
            for group_command in sorted(groups, key=lambda d: d.name):
                group_prefix = [*prefix, group_command.name]
                display_recursive(group_prefix, group_command.subcommands, group_command.help)

    display_recursive(["airflow"], airflow_commands)
