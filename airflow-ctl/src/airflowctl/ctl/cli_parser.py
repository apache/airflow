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

.. seealso:: :mod:`airflowctl.ctl.cli_config`
"""

from __future__ import annotations

import argparse
import logging
from argparse import Action
from collections.abc import Iterable
from functools import cache
from typing import TYPE_CHECKING

import lazy_object_proxy
from rich_argparse import HelpPreviewAction, RawTextRichHelpFormatter, RichHelpFormatter

from airflowctl.ctl.cli_config import (
    ActionCommand,
    DefaultHelpParser,
    GroupCommand,
    GroupCommandParser,
    add_auth_token_to_all_commands,
    core_commands,
)
from airflowctl.exceptions import AirflowCtlException
from airflowctl.utils.helpers import partition

if TYPE_CHECKING:
    from airflowctl.ctl.cli_config import (
        Arg,
        CLICommand,
    )

log = logging.getLogger(__name__)

#: Entry-point group used by every Airflow provider to expose its
#: ``get_provider_info`` function. ``airflowctl`` reuses it: each provider's
#: ``get_provider_info()`` returns a dict whose ``ctl`` key (when present)
#: lists the import paths of factories that produce airflowctl commands.
#: This mirrors how core Airflow's ``ProvidersManager._discover_cli_command``
#: consumes the ``cli`` key — see ``contributing-docs/27_cli_implementation_guide.rst``
#: for the AIP-94 direction.
APACHE_AIRFLOW_PROVIDER_ENTRY_POINT_GROUP = "apache_airflow_provider"

#: Key inside each provider's ``get_provider_info()`` dict that lists the
#: airflowctl command factory paths. Mirrors the ``cli`` key consumed by
#: core Airflow's CLI.
PROVIDER_INFO_CTL_KEY = "ctl"


def _discover_provider_commands() -> list[CLICommand]:
    """
    Discover provider-contributed airflowctl commands.

    Walks the ``apache_airflow_provider`` entry-point group, calls each
    provider's ``get_provider_info()`` to read its ``ctl`` field, and
    lazily imports the factory functions listed there. Each factory is a
    zero-argument callable returning ``list[CLICommand]``.

    A failure on one provider logs a warning and the loop continues, so a
    broken provider cannot take down ``airflowctl``.
    """
    from importlib.metadata import entry_points

    from airflowctl.utils.module_loading import import_string

    discovered: list[CLICommand] = []
    try:
        eps = entry_points(group=APACHE_AIRFLOW_PROVIDER_ENTRY_POINT_GROUP)
    except Exception as exc:
        log.warning("Failed to enumerate %s entry points: %s", APACHE_AIRFLOW_PROVIDER_ENTRY_POINT_GROUP, exc)
        return discovered

    for ep in eps:
        try:
            get_provider_info = ep.load()
            info = get_provider_info()
        except Exception as exc:
            log.warning("Failed to load provider info from %s (%s): %s", ep.name, ep.value, exc)
            continue
        if not isinstance(info, dict):
            log.warning(
                "Provider %s returned %s from get_provider_info, expected dict; skipping",
                ep.name,
                type(info).__name__,
            )
            continue
        ctl_paths = info.get(PROVIDER_INFO_CTL_KEY) or []
        if not isinstance(ctl_paths, list):
            log.warning(
                "Provider %s returned %s for %r, expected list[str]; skipping",
                ep.name,
                type(ctl_paths).__name__,
                PROVIDER_INFO_CTL_KEY,
            )
            continue
        for path in ctl_paths:
            try:
                factory = import_string(path)
                commands = factory()
            except Exception as exc:
                log.warning("Failed to load ctl factory %r from provider %s: %s", path, ep.name, exc)
                continue
            if not isinstance(commands, list):
                log.warning(
                    "ctl factory %r returned %s, expected list[CLICommand]; skipping",
                    path,
                    type(commands).__name__,
                )
                continue
            discovered.extend(commands)
    return discovered


airflow_commands: list[CLICommand] = list(core_commands)
# Provider-contributed commands need the same ``--api-token`` arg as core
# commands. ``add_auth_token_to_all_commands`` is idempotent, so this is safe
# even though ``core_commands`` was already decorated in ``cli_config``.
airflow_commands.extend(add_auth_token_to_all_commands(_discover_provider_commands()))


ALL_COMMANDS_DICT: dict[str, CLICommand] = {
    sp.name: GroupCommandParser.from_group_command(sp) if isinstance(sp, GroupCommand) else sp
    for sp in airflow_commands
}


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
                lambda d: isinstance(ALL_COMMANDS_DICT[d.dest], GroupCommandParser), subactions
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


def add_preview_action(parser: argparse.ArgumentParser) -> None:
    """Add preview action to parser."""
    parser.add_argument(
        "--preview",
        action=HelpPreviewAction,
    )


@cache
def get_parser() -> argparse.ArgumentParser:
    """Create and returns command line argument parser."""
    parser = DefaultHelpParser(prog="airflowctl", formatter_class=AirflowHelpFormatter)
    add_preview_action(parser)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    subparsers.required = True

    for _, sub in sorted(ALL_COMMANDS_DICT.items()):
        _add_command(
            subparsers, GroupCommandParser.from_group_command(sub) if isinstance(sub, GroupCommand) else sub
        )

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
            sub.name, help=sub.help, description=sub.description or sub.help, epilog=sub.epilog
        )
    add_preview_action(sub_proc)
    sub_proc.formatter_class = LazyRichHelpFormatter

    if isinstance(sub, GroupCommandParser):
        _add_group_command(sub, sub_proc)
    elif isinstance(sub, ActionCommand):
        _add_action_command(sub, sub_proc)
    else:
        raise AirflowCtlException("Invalid command definition.")


def _add_action_command(sub: ActionCommand, sub_proc: argparse.ArgumentParser) -> None:
    for arg in _sort_args(sub.args):
        arg.add_to_parser(sub_proc)
    sub_proc.set_defaults(func=sub.func)


def _add_group_command(sub: GroupCommandParser, sub_proc: argparse.ArgumentParser) -> None:
    subcommands = sub.subcommands
    sub_subparsers = sub_proc.add_subparsers(dest="subcommand", metavar="COMMAND")
    sub_subparsers.required = True
    for command in sorted(subcommands, key=lambda x: x.name):
        _add_command(sub_subparsers, command)
