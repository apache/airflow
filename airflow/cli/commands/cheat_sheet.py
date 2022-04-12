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
from typing import List, Optional

import rich_click as click

from airflow.cli import airflow_cmd, click_verbose
from airflow.cli.simple_table import AirflowConsole, SimpleTable
from airflow.utils.cli import suppress_logs_and_warning_click_compatible


@airflow_cmd.command("cheat-sheet")
@click_verbose
@suppress_logs_and_warning_click_compatible
def cheat_sheet(verbose):
    """Display cheat-sheet"""
    display_commands_index()


def display_commands_index():
    def display_recursive(
        prefix: List[str],
        command_group: click.Group,
        help_msg: Optional[str] = None,
        help_msg_length: int = 88,
    ):
        actions: List[click.Command] = []
        groups: List[click.Group] = []
        for command in command_group.commands.values():
            if isinstance(command, click.Group):
                groups.append(command)
            else:
                actions.append(command)

        console = AirflowConsole()
        if actions:
            table = SimpleTable(title=help_msg or "Miscellaneous commands")
            table.add_column(width=40)
            table.add_column()
            for action_command in sorted(actions, key=lambda d: d.name):
                help_str = action_command.get_short_help_str(limit=help_msg_length)
                table.add_row(" ".join([*prefix, action_command.name]), help_str)
            console.print(table)

        if groups:
            for group_command in sorted(groups, key=lambda d: d.name):
                group_prefix = [*prefix, group_command.name]
                help_str = group_command.get_short_help_str(limit=help_msg_length)
                display_recursive(group_prefix, group_command, help_str)

    display_recursive(["airflow"], airflow_cmd)
