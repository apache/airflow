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
import os
from argparse import Action
from collections import Counter
from collections.abc import Iterable
from functools import cache
from typing import TYPE_CHECKING

import lazy_object_proxy
from rich_argparse import RawTextRichHelpFormatter, RichHelpFormatter

from airflow._shared.module_loading import import_string
from airflow.cli.cli_config import (
    DAG_CLI_DICT,
    ActionCommand,
    DefaultHelpParser,
    GroupCommand,
    core_commands,
)
from airflow.cli.utils import CliConflictError
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.executors.executor_loader import ExecutorLoader
from airflow.providers_manager import ProvidersManager
from airflow.utils.helpers import partition

if TYPE_CHECKING:
    from airflow.cli.cli_config import (
        Arg,
        CLICommand,
    )

airflow_commands = core_commands.copy()  # make a copy to prevent bad interactions in tests

log = logging.getLogger(__name__)

# Executors and auth managers shipped in airflow-core define no CLI commands of their own. Skipping them
# by module prefix keeps the default configuration from importing any executor or auth manager class.
_CORE_EXECUTORS_PACKAGE = "airflow.executors."
_CORE_AUTH_MANAGERS_PACKAGE = "airflow.api_fastapi.auth.managers."


def _exclude_registered_commands(
    commands: Iterable[CLICommand], registered_names: set[str], source: str
) -> list[CLICommand]:
    """
    Drop commands whose name is already registered, and record the names of the commands kept.

    A subclass of a provider executor or auth manager inherits ``get_cli_commands``, so the same command can
    come from a provider ``cli`` section and from an imported class, or from two imported classes. Only the
    first one is kept, otherwise the duplicate is reported as a conflict.
    """
    result: list[CLICommand] = []
    for command in commands:
        if command.name in registered_names:
            log.debug("Skipping CLI command '%s' from %s: already registered.", command.name, source)
            continue
        registered_names.add(command.name)
        result.append(command)
    return result


# AIRFLOW_PACKAGE_NAME is set when generating docs and we don't want to load provider commands when generating airflow-core CLI docs
if not os.environ.get("AIRFLOW_PACKAGE_NAME", None):
    providers_manager = ProvidersManager()
    # Load CLI commands from providers
    try:
        for cli_function in providers_manager.cli_command_functions:
            try:
                airflow_commands.extend(cli_function())
            except Exception:
                log.exception("Failed to load CLI commands from provider function: %s", cli_function.__name__)
                log.error("Ensure all dependencies are met and try again.")
                # Do not re-raise the exception since we want the CLI to still function for
                # other commands.
    except Exception as e:
        log.warning("Failed to load CLI commands from providers: %s", e)
        # do not re-raise for the same reason as above

    # Core commands are left out so that a class redefining one still fails the conflict check below
    registered_command_names = {command.name for command in airflow_commands} - {
        command.name for command in core_commands
    }

    WARNING_TEMPLATE = """
Please define the 'cli' section in the 'get_provider_info' for custom {component} to avoid this warning.
For community providers, please update to the version that support 'cli' section.
For more details, see https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/cli-commands.html

Providers with {component} missing 'cli' section in 'get_provider_info': {not_defined_cli_dict}
    """

    # compat loading for executors that do not register CLI commands through a provider "cli" section:
    # older providers, and custom executors configured by module path without being packaged as a provider
    try:
        # warn about providers that still rely on compat loading; "without check" avoids importing them here
        executors_not_defined_cli = {
            executor_name: executor_provider
            for executor_name, executor_provider in providers_manager.executor_without_check
            if executor_provider not in providers_manager.cli_command_providers
        }
        if executors_not_defined_cli:
            log.warning(
                WARNING_TEMPLATE.format(
                    component="executors", not_defined_cli_dict=str(executors_not_defined_cli)
                )
            )
        executors_defined_cli = {
            executor_name for executor_name, _ in providers_manager.executor_without_check
        } - executors_not_defined_cli.keys()

        for executor_name in ExecutorLoader.get_executor_names(validate_teams=False):
            if executor_name.module_path in executors_defined_cli or executor_name.module_path.startswith(
                _CORE_EXECUTORS_PACKAGE
            ):
                log.debug(
                    "Skipping loading for '%s' as its CLI commands are registered elsewhere.",
                    executor_name.module_path,
                )
                continue

            try:
                executor, _ = ExecutorLoader.import_executor_cls(executor_name)
                airflow_commands.extend(
                    _exclude_registered_commands(
                        executor.get_cli_commands(), registered_command_names, executor_name.module_path
                    )
                )
            except Exception:
                log.exception("Failed to load CLI commands from executor: %s", executor_name)
                log.error(
                    "Ensure all dependencies are met and try again. If using a Celery based executor install "
                    "a 3.3.0+ version of the Celery provider. If using a Kubernetes executor, install a "
                    "7.4.0+ version of the CNCF provider"
                )
                # Do not re-raise the exception since we want the CLI to still function for
                # other commands.

    except Exception as e:
        log.warning(
            "Failed to load CLI commands from executors that didn't define `get_cli_commands` in `.cli.definition`: %s",
            e,
        )

    # compat loading for auth managers, following the same rules as for executors
    try:
        # warn about providers that still rely on compat loading; "without check" avoids importing them here
        auth_managers_not_defined_cli = {
            auth_manager_name: auth_manager_provider
            for auth_manager_name, auth_manager_provider in providers_manager.auth_manager_without_check
            if auth_manager_provider not in providers_manager.cli_command_providers
        }
        if auth_managers_not_defined_cli:
            log.warning(
                WARNING_TEMPLATE.format(
                    component="auth manager", not_defined_cli_dict=str(auth_managers_not_defined_cli)
                )
            )
        auth_managers_defined_cli = {
            auth_manager_name for auth_manager_name, _ in providers_manager.auth_manager_without_check
        } - auth_managers_not_defined_cli.keys()

        auth_manager_cls_path = conf.get(section="core", key="auth_manager")

        if not auth_manager_cls_path:
            raise AirflowConfigException(
                "No auth manager defined in the config. Please specify one using section/key [core/auth_manager]."
            )

        if auth_manager_cls_path not in auth_managers_defined_cli and not auth_manager_cls_path.startswith(
            _CORE_AUTH_MANAGERS_PACKAGE
        ):
            try:
                auth_manager_cls = import_string(auth_manager_cls_path)
                auth_manager = auth_manager_cls()
                airflow_commands.extend(
                    _exclude_registered_commands(
                        auth_manager.get_cli_commands(), registered_command_names, auth_manager_cls_path
                    )
                )
            except Exception:
                log.exception("Failed to load CLI commands from auth manager: %s", auth_manager_cls_path)
                log.error("Ensure all dependencies are met and try again.")
                # Do not re-raise the exception since we want the CLI to still function for
                # other commands.
    except Exception as e:
        log.warning(
            "Failed to load CLI commands from auth managers that didn't define `get_cli_commands` in `.cli.definition`: %s",
            e,
        )

ALL_COMMANDS_DICT: dict[str, CLICommand] = {sp.name: sp for sp in airflow_commands}


# Check if sub-commands are defined twice, which could be an issue.
if len(ALL_COMMANDS_DICT) < len(airflow_commands):
    dup = {k for k, v in Counter([c.name for c in airflow_commands]).items() if v > 1}
    raise CliConflictError(
        f"The following CLI {len(dup)} command(s) are defined more than once: {sorted(dup)}\n"
        f"This can be due to a Provider redefining core airflow CLI commands."
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


@cache
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
            sub.name, help=sub.help, description=sub.description or sub.help, epilog=sub.epilog
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
