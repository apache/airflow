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

import contextlib
from typing import Iterable, List, Union

from termcolor import cprint

from airflow.cli.cli_parser import ActionCommand, GroupCommand, airflow_commands
from airflow.utils.helpers import partition
from airflow.utils.platform import is_terminal_support_colors
from airflow.utils.process_utils import patch_environ

ANSI_COLORS_DISABLED = "ANSI_COLORS_DISABLED"
"""Environment variable disable using rich output. It is supported by termcolor library"""


def cheat_sheet(args):
    """Display cheat-sheet."""
    with contextlib.ExitStack() as exit_stack:
        if not is_terminal_support_colors():
            exit_stack.enter_context(patch_environ({ANSI_COLORS_DISABLED: "1"}))
        cprint("List of all commands:".upper(), attrs=["bold", "underline"])
        print()
        display_commands_index()


def display_commands_index():
    """Display list of all commands."""

    def display_recursive(prefix: List[str], commands: Iterable[Union[GroupCommand, ActionCommand]]):
        actions: List[ActionCommand]
        groups: List[GroupCommand]
        actions_tter, groups_iter = partition(lambda x: isinstance(x, GroupCommand), commands)
        actions, groups = list(actions_tter), list(groups_iter)

        if actions:
            for action_command in sorted(actions, key=lambda d: d.name):
                print("  ", end="")
                cprint(" ".join([*prefix, action_command.name]), attrs=["bold"], end="")
                print(f" - {action_command.help}")
            print()

        if groups:
            for group_command in sorted(groups, key=lambda d: d.name):
                group_prefix = [*prefix, group_command.name]
                # print(bold(" ".join(group_prefix)), end="")
                cprint(group_command.help, attrs=["bold", "underline"])
                print()
                display_recursive(group_prefix, group_command.subcommands)

            print()

    display_recursive(["airflow"], airflow_commands)
