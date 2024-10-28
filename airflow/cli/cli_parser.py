#!/usr/bin/env python
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
"""
Produce a CLI parser object from Airflow CLI command configuration.

.. seealso:: :mod:`airflow.cli.cli_config`
"""

from __future__ import annotations

import argparse
import logging
import sys
from argparse import Action
from collections import Counter
from functools import lru_cache
from typing import TYPE_CHECKING, Iterable

import lazy_object_proxy
from rich_argparse import RawTextRichHelpFormatter, RichHelpFormatter

from airflow.cli.cli_config import (
    DAG_CLI_DICT,
    ActionCommand,
    DefaultHelpParser,
    GroupCommand,
    core_commands,
)
from airflow.cli.utils import CliConflictError
from airflow.exceptions import AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.utils.helpers import partition
from airflow.www.extensions.init_auth_manager import get_auth_manager_cls

if TYPE_CHECKING:
    from airflow.cli.cli_config import (
        Arg,
        CLICommand,
    )

airflow_commands = (
    core_commands.copy()
)  # make a copy to prevent bad interactions in tests

log = logging.getLogger(__name__)


for executor_name in ExecutorLoader.get_executor_names():
    try:
        executor, _ = ExecutorLoader.import_executor_cls(executor_name)
        airflow_commands.extend(executor.get_cli_commands())
    except Exception:
        log.exception("Failed to load CLI commands from executor: %s", executor_name)
        log.error(
            "Ensure all dependencies are met and try again. If using a Celery based executor install "
            "a 3.3.0+ version of the Celery provider. If using a Kubernetes executor, install a "
            "7.4.0+ version of the CNCF provider"
        )
        # Do not re-raise the exception since we want the CLI to still function for
        # other commands.

try:
    auth_mgr = get_auth_manager_cls()
    airflow_commands.extend(auth_mgr.get_cli_commands())
except Exception as e:
    log.warning("cannot load CLI commands from auth manager: %s", e)
    log.warning(
        "Authentication manager is not configured and webserver will not be able to start."
    )
    # do not re-raise for the same reason as above
    if len(sys.argv) > 1 and sys.argv[1] == "webserver":
        log.exception(e)
        sys.exit(1)


ALL_COMMANDS_DICT: dict[str, CLICommand] = {sp.name: sp for sp in airflow_commands}


# Check if sub-commands are defined twice, which could be an issue.
if len(ALL_COMMANDS_DICT) < len(airflow_commands):
    dup = {k for k, v in Counter([c.name for c in airflow_commands]).items() if v > 1}
    raise CliConflictError(
        f"The following CLI {len(dup)} command(s) are defined more than once: {sorted(dup)}\n"
        f"This can be due to an Executor or Auth Manager redefining core airflow CLI commands."
    )


class AirflowHelpFormatter(RichHelpFormatter):
    """
    Custom help formatter to display help message.

    It displays simple commands and groups of commands in separate sections.
    """

    def _iter_indented_subactions(self, action: Action):
        if isinstance(action, argparse._SubParsersAction):
            self._indent()
            subactions = action._get_subactions()
            action_subcommands, group_subcommands = partition(
                lambda d: isinstance(ALL_COMMANDS_DICT[d.dest], GroupCommand), subactions
            )
            yield Action([], f"\n{' ':{self._current_indent}}Groups", nargs=0)
            self._indent()
            yield from group_subcommands
            self._dedent()

            yield Action([], f"\n{' ':{self._current_indent}}Commands:", nargs=0)
            self._indent()
            yield from action_subcommands
            self._dedent()
            self._dedent()
        else:
            yield from super()._iter_indented_subactions(action)


class LazyRichHelpFormatter(RawTextRichHelpFormatter):
    """
    Custom help formatter to display help message.

    It resolves lazy help string before printing it using rich.
    """

    def add_argument(self, action: Action) -> None:
        if isinstance(action.help, lazy_object_proxy.Proxy):
            action.help = str(action.help)
        return super().add_argument(action)


@lru_cache(maxsize=None)
def get_parser(dag_parser: bool = False) -> argparse.ArgumentParser:
    """Create and returns command line argument parser."""
    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    subparsers.required = True

    command_dict = DAG_CLI_DICT if dag_parser else ALL_COMMANDS_DICT
    for _, sub in sorted(command_dict.items()):
        _add_command(subparsers, sub)
    return parser


def _sort_args(args: Iterable[Arg]) -> Iterable[Arg]:
    """Sort subcommand optional args, keep positional args."""

    def get_long_option(arg: Arg):
        """Get long option from Arg.flags."""
        return arg.flags[0] if len(arg.flags) == 1 else arg.flags[1]

    positional, optional = partition(lambda x: x.flags[0].startswith("-"), args)
    yield from positional
    yield from sorted(optional, key=lambda x: get_long_option(x).lower())


def _add_command(subparsers: argparse._SubParsersAction, sub: CLICommand) -> None:
    if isinstance(sub, ActionCommand) and sub.hide:
        sub_proc = subparsers.add_parser(sub.name, epilog=sub.epilog)
    else:
        sub_proc = subparsers.add_parser(
            sub.name,
            help=sub.help,
            description=sub.description or sub.help,
            epilog=sub.epilog,
        )
    sub_proc.formatter_class = LazyRichHelpFormatter

    if isinstance(sub, GroupCommand):
        _add_group_command(sub, sub_proc)
    elif isinstance(sub, ActionCommand):
        _add_action_command(sub, sub_proc)
    else:
        raise AirflowException("Invalid command definition.")


def _add_action_command(sub: ActionCommand, sub_proc: argparse.ArgumentParser) -> None:
    for arg in _sort_args(sub.args):
        arg.add_to_parser(sub_proc)
    sub_proc.set_defaults(func=sub.func)


def _add_group_command(sub: GroupCommand, sub_proc: argparse.ArgumentParser) -> None:
    subcommands = sub.subcommands
    sub_subparsers = sub_proc.add_subparsers(dest="subcommand", metavar="COMMAND")
    sub_subparsers.required = True
    for command in sorted(subcommands, key=lambda x: x.name):
        _add_command(sub_subparsers, command)
