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
"""Explicit configuration and definition of Airflow CLI commands."""

from __future__ import annotations

import argparse
import ast
import getpass
import inspect
import os
import textwrap
from argparse import Namespace
from collections.abc import Iterable
from functools import partial
from pathlib import Path
from typing import Any, Callable, NamedTuple, Union

import rich

import airflowctl.api.datamodels.generated as generated_datamodels
from airflowctl.api.client import NEW_API_CLIENT, Client, ClientKind, provide_api_client
from airflowctl.api.operations import BaseOperations, ServerResponseError
from airflowctl.exceptions import (
    AirflowCtlConnectionException,
    AirflowCtlCredentialNotFoundException,
    AirflowCtlNotFoundException,
)
from airflowctl.utils.module_loading import import_string

BUILD_DOCS = "BUILDING_AIRFLOW_DOCS" in os.environ


def lazy_load_command(import_path: str) -> Callable:
    """Create a lazy loader for command."""
    _, _, name = import_path.rpartition(".")

    def command(*args, **kwargs):
        func = import_string(import_path)
        return func(*args, **kwargs)

    command.__name__ = name

    return command


def safe_call_command(function: Callable, args: Iterable[Arg]) -> None:
    try:
        function(args)
    except AirflowCtlCredentialNotFoundException as e:
        rich.print(f"command failed due to {e}")
    except AirflowCtlConnectionException as e:
        rich.print(f"command failed due to {e}")
    except AirflowCtlNotFoundException as e:
        rich.print(f"command failed due to {e}")


class DefaultHelpParser(argparse.ArgumentParser):
    """CustomParser to display help message."""

    def _check_value(self, action, value):
        """Override _check_value and check conditionally added command."""
        super()._check_value(action, value)

    def error(self, message):
        """Override error and use print_help instead of print_usage."""
        self.print_help()
        self.exit(2, f"\n{self.prog} command error: {message}, see help above.\n")


# Used in Arg to enable `None` as a distinct value from "not passed"
_UNSET = object()


class Arg:
    """Class to keep information about command line argument."""

    def __init__(
        self,
        flags=_UNSET,
        help=_UNSET,
        action=_UNSET,
        default=_UNSET,
        nargs=_UNSET,
        type=_UNSET,
        choices=_UNSET,
        required=_UNSET,
        metavar=_UNSET,
        dest=_UNSET,
    ):
        self.flags = flags
        self.kwargs = {}
        for k, v in locals().items():
            if k not in ("self", "flags") and v is not _UNSET:
                self.kwargs[k] = v

    def add_to_parser(self, parser: argparse.ArgumentParser):
        """Add this argument to an ArgumentParser."""
        if "metavar" in self.kwargs and "type" not in self.kwargs:
            if self.kwargs["metavar"] == "DIRPATH":

                def type(x):
                    return self._is_valid_directory(parser, x)

                self.kwargs["type"] = type
        parser.add_argument(*self.flags, **self.kwargs)

    def _is_valid_directory(self, parser, arg):
        if not os.path.isdir(arg):
            parser.error(f"The directory '{arg}' does not exist!")
        return arg


def positive_int(*, allow_zero):
    """Define a positive int type for an argument."""

    def _check(value):
        try:
            value = int(value)
            if allow_zero and value == 0:
                return value
            if value > 0:
                return value
        except ValueError:
            pass
        raise argparse.ArgumentTypeError(f"invalid positive int value: '{value}'")

    return _check


def string_list_type(val):
    """Parse comma-separated list and returns list of string (strips whitespace)."""
    return [x.strip() for x in val.split(",")]


def string_lower_type(val):
    """Lower arg."""
    if not val:
        return
    return val.strip().lower()


class Password(argparse.Action):
    """Custom action to prompt for password input."""

    def __call__(self, parser, namespace, values, option_string=None):
        values = getpass.getpass()
        setattr(namespace, self.dest, values)


# Authentication arguments
ARG_AUTH_URL = Arg(
    flags=("--api-url",),
    type=str,
    default="http://localhost:8080",
    dest="api_url",
    help="The URL of the metadata database API",
)
ARG_AUTH_TOKEN = Arg(
    flags=("--api-token",),
    type=str,
    dest="api_token",
    help="The token to use for authentication",
)
ARG_AUTH_ENVIRONMENT = Arg(
    flags=("-e", "--env"),
    type=str,
    default="production",
    help="The environment to run the command in",
)
ARG_AUTH_USERNAME = Arg(
    flags=("--username",),
    type=str,
    dest="username",
    help="The username to use for authentication",
)
ARG_AUTH_PASSWORD = Arg(
    flags=("--password",),
    type=str,
    dest="password",
    help="The password to use for authentication",
    action=Password,
    nargs="?",
)
ARG_VARIABLE_IMPORT = Arg(
    flags=("file",),
    metavar="file",
    help="Import variables from JSON file",
)
ARG_VARIABLE_ACTION_ON_EXISTING_KEY = Arg(
    flags=("-a", "--action-on-existing-key"),
    type=str,
    default="overwrite",
    help="Action to take if we encounter a variable key that already exists.",
    choices=("overwrite", "fail", "skip"),
)
ARG_VARIABLE_EXPORT = Arg(
    flags=("file",),
    metavar="file",
    help="Export all variables to JSON file",
)

ARG_OUTPUT = Arg(
    flags=("-o", "--output"),
    type=str,
    default="json",
    help="Output format. Only json format is supported (default: json)",
)

# Pool Commands Args
ARG_POOL_FILE = Arg(
    ("file",),
    metavar="FILEPATH",
    help="Pools JSON file. Example format::\n"
    + textwrap.indent(
        textwrap.dedent(
            """
            [
                {
                    "name": "pool_1",
                    "slots": 5,
                    "description": "",
                    "include_deferred": true,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "open_slots": 5,
                    "deferred_slots": 0
                }
            ]"""
        ),
        " " * 4,
    ),
)

# Config arguments
ARG_CONFIG_SECTION = Arg(
    flags=("--section",),
    type=str,
    dest="section",
    help="The section of the configuration",
)
ARG_CONFIG_OPTION = Arg(
    flags=("--option",),
    type=str,
    dest="option",
    help="The option of the configuration",
)
ARG_CONFIG_IGNORE_SECTION = Arg(
    flags=("--ignore-section",),
    type=str,
    dest="ignore_section",
    help="The configuration section being ignored",
)
ARG_CONFIG_IGNORE_OPTION = Arg(
    flags=("--ignore-option",),
    type=str,
    dest="ignore_option",
    help="The configuration option being ignored",
)
ARG_CONFIG_VERBOSE = Arg(
    flags=(
        "-v",
        "--verbose",
    ),
    help="Enables detailed output, including the list of ignored sections and options",
    default=False,
    action="store_true",
)


class ActionCommand(NamedTuple):
    """Single CLI command."""

    name: str
    help: str
    func: Callable
    args: Iterable[Arg]
    description: str | None = None
    epilog: str | None = None
    hide: bool = False


class GroupCommand(NamedTuple):
    """ClI command with subcommands."""

    name: str
    help: str
    subcommands: Iterable
    api_operation: dict | None = None
    description: str | None = None
    epilog: str | None = None


class GroupCommandParser(NamedTuple):
    """ClI command with subcommands."""

    name: str
    help: str
    subcommands: Iterable
    description: str | None = None
    epilog: str | None = None

    @classmethod
    def from_group_command(cls, group_command: GroupCommand) -> GroupCommandParser:
        """Create GroupCommandParser from GroupCommand."""
        return cls(
            name=group_command.name,
            help=group_command.help,
            subcommands=group_command.subcommands,
            description=group_command.description,
            epilog=group_command.epilog,
        )


CLICommand = Union[ActionCommand, GroupCommand, GroupCommandParser]


class CommandFactory:
    """Factory class that creates 1-1 mapping with airflowctl/api/operations."""

    datamodels_extended_map: dict[str, list[str]]
    operations: list[dict]
    args_map: dict[tuple, list[Arg]]
    func_map: dict[tuple, Callable]
    commands_map: dict[str, list[ActionCommand]]
    group_commands_list: list[CLICommand]

    def __init__(self, file_path: str | Path | None = None):
        self.datamodels_extended_map = {}
        self.func_map = {}
        self.operations = []
        self.args_map = {}
        self.commands_map = {}
        self.group_commands_list = []
        self.file_path = inspect.getfile(BaseOperations) if file_path is None else file_path
        # Exclude parameters that are not needed for CLI from datamodels
        self.excluded_parameters = ["schema_"]

    def _inspect_operations(self) -> None:
        """Parse file and return matching Operation Method with details."""

        def get_function_details(node: ast.FunctionDef, parent_node: ast.ClassDef) -> dict:
            """Extract function name, arguments, and return annotation."""
            func_name = node.name
            args = []
            return_annotation: str = ""

            for arg in node.args.args:
                arg_name = arg.arg
                arg_type = ast.unparse(arg.annotation) if arg.annotation else "Any"
                if arg_name != "self":
                    args.append({arg_name: arg_type})

            if node.returns:
                return_annotation = [
                    t.strip()
                    # TODO change this while removing Python 3.9 support
                    for t in ast.unparse(node.returns).split("|")
                    if t.strip() != ServerResponseError.__name__
                ].pop()

            return {
                "name": func_name,
                "parameters": args,
                "return_type": return_annotation,
                "parent": parent_node,
            }

        with open(self.file_path, encoding="utf-8") as file:
            tree = ast.parse(file.read(), filename=self.file_path)

        exclude_operation_names = ["LoginOperations", "VersionOperations"]
        exclude_method_names = [
            "error",
            "__init__",
            "__init_subclass__",
            "_check_flag_and_exit_if_server_response_error",
            # Excluding bulk operation. Out of scope for CLI. Should use implemented commands.
            "bulk",
        ]
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ClassDef)
                and "Operations" in node.name
                and node.name not in exclude_operation_names
                and node.body
            ):
                for child in node.body:
                    if isinstance(child, ast.FunctionDef) and child.name not in exclude_method_names:
                        self.operations.append(get_function_details(node=child, parent_node=node))

    @staticmethod
    def _sanitize_arg_parameter_key(parameter_key: str) -> str:
        return parameter_key.replace("_", "-")

    @staticmethod
    def _sanitize_method_param_key(parameter_key: str) -> str:
        return parameter_key.replace("-", "_")

    @staticmethod
    def _is_primitive_type(type_name: str) -> bool:
        primitive_types = {
            "int",
            "float",
            "bool",
            "str",
            "bytes",
            "list",
            "dict",
            "tuple",
            "set",
            "datetime.datetime",
        }
        return type_name in primitive_types

    @staticmethod
    def _create_arg(
        arg_flags: tuple,
        arg_type: type,
        arg_help: str,
        arg_action: argparse.BooleanOptionalAction | None,
        arg_dest: str | None = None,
        arg_default: Any | None = None,
    ) -> Arg:
        return Arg(
            flags=arg_flags,
            type=arg_type,
            dest=arg_dest,
            help=arg_help,
            default=arg_default,
            action=arg_action,
        )

    def _create_arg_for_non_primitive_type(
        self,
        parameter_type: str,
        parameter_key: str,
    ) -> list[Arg]:
        """Create Arg for non-primitive type Pydantic."""
        parameter_type_map = getattr(generated_datamodels, parameter_type)
        commands = []
        if parameter_type_map not in self.datamodels_extended_map.keys():
            self.datamodels_extended_map[parameter_type] = []
        for field, field_type in parameter_type_map.model_fields.items():
            if field in self.excluded_parameters:
                continue
            self.datamodels_extended_map[parameter_type].append(field)
            if type(field_type.annotation) is type:
                commands.append(
                    self._create_arg(
                        arg_flags=("--" + self._sanitize_arg_parameter_key(field),),
                        arg_type=field_type.annotation,
                        arg_action=argparse.BooleanOptionalAction if field_type.annotation is bool else None,  # type: ignore
                        arg_help=f"{field} for {parameter_key} operation",
                        arg_default=False if field_type.annotation is bool else None,
                    )
                )
            else:
                try:
                    annotation = field_type.annotation.__args__[0]
                except AttributeError:
                    annotation = field_type.annotation

                commands.append(
                    self._create_arg(
                        arg_flags=("--" + self._sanitize_arg_parameter_key(field),),
                        arg_type=annotation,
                        arg_action=argparse.BooleanOptionalAction if annotation is bool else None,  # type: ignore
                        arg_help=f"{field} for {parameter_key} operation",
                        arg_default=False if annotation is bool else None,
                    )
                )
        return commands

    def _create_args_map_from_operation(self):
        """Create Arg from Operation Method checking for parameters and return types."""
        for operation in self.operations:
            args = []
            for parameter in operation.get("parameters"):
                for parameter_key, parameter_type in parameter.items():
                    if self._is_primitive_type(type_name=parameter_type):
                        args.append(
                            self._create_arg(
                                arg_flags=("--" + self._sanitize_arg_parameter_key(parameter_key),),
                                arg_type=type(parameter_type),
                                arg_action=argparse.BooleanOptionalAction
                                if type(parameter_type) is bool
                                else None,
                                arg_help=f"{parameter_key} for {operation.get('name')} operation in {operation.get('parent').name}",
                                arg_default=False if type(parameter_type) is bool else None,
                            )
                        )
                    else:
                        args.extend(
                            self._create_arg_for_non_primitive_type(
                                parameter_type=parameter_type, parameter_key=parameter_key
                            )
                        )
            self.args_map[(operation.get("name"), operation.get("parent").name)] = args

    def _create_func_map_from_operation(self):
        """Create function map from Operation Method checking for parameters and return types."""

        @provide_api_client(kind=ClientKind.CLI)
        def _get_func(args: Namespace, api_operation: dict, api_client: Client = NEW_API_CLIENT, **kwargs):
            import importlib

            imported_operation = importlib.import_module("airflowctl.api.operations")
            operation_class_object = getattr(imported_operation, api_operation["parent"].name)
            operation_class = operation_class_object(client=api_client)
            operation_method_object = getattr(operation_class, api_operation["name"])

            # Walk through all args and create a dictionary such as args.abc -> {"abc": "value"}
            method_params = {}
            datamodel = None
            args_dict = vars(args)
            for parameter in api_operation["parameters"]:
                for parameter_key, parameter_type in parameter.items():
                    if self._is_primitive_type(type_name=parameter_type):
                        method_params[self._sanitize_method_param_key(parameter_key)] = args_dict[
                            parameter_key
                        ]
                    else:
                        datamodel = getattr(generated_datamodels, parameter_type)
                        for expanded_parameter in self.datamodels_extended_map[parameter_type]:
                            if expanded_parameter in self.excluded_parameters:
                                continue
                            if expanded_parameter in args_dict.keys():
                                method_params[self._sanitize_method_param_key(expanded_parameter)] = (
                                    args_dict[expanded_parameter]
                                )

            if datamodel:
                method_params = datamodel.model_validate(method_params)
                rich.print(operation_method_object(method_params))
            else:
                rich.print(operation_method_object(**method_params))

        for operation in self.operations:
            self.func_map[(operation.get("name"), operation.get("parent").name)] = partial(
                _get_func, api_operation=operation
            )

    def _create_group_commands_from_operation(self):
        """Create GroupCommand from Operation Methods."""
        for operation in self.operations:
            operation_name = operation["name"]
            operation_group_name = operation["parent"].name
            if operation_group_name not in self.commands_map:
                self.commands_map[operation_group_name] = []
            self.commands_map[operation_group_name].append(
                ActionCommand(
                    name=operation["name"].replace("_", "-"),
                    help=f"Perform {operation_name} operation",
                    func=self.func_map[(operation_name, operation_group_name)],
                    args=self.args_map[(operation_name, operation_group_name)],
                )
            )

        for group_name, action_commands in self.commands_map.items():
            self.group_commands_list.append(
                GroupCommand(
                    name=group_name.replace("Operations", "").lower(),
                    help=f"Perform {group_name.replace('Operations', '')} operations",
                    subcommands=action_commands,
                )
            )

    @property
    def group_commands(self) -> list[CLICommand]:
        """List of GroupCommands generated for airflowctl."""
        self._inspect_operations()
        self._create_args_map_from_operation()
        self._create_func_map_from_operation()
        self._create_group_commands_from_operation()

        return self.group_commands_list


def merge_commands(
    base_commands: list[CLICommand], commands_will_be_merged: list[CLICommand]
) -> list[CLICommand]:
    """
    Merge group commands with existing commands which extends base_commands with will_be_merged commands.

    Args:
        base_commands: List of base commands to be extended.
        commands_will_be_merged: List of group commands to be merged with base_commands.

    Returns:
        List of merged commands.
    """
    merge_command_map = {}
    new_commands: list[CLICommand] = []
    for command in commands_will_be_merged:
        if isinstance(command, ActionCommand):
            new_commands.append(command)
        if isinstance(command, GroupCommand):
            merge_command_map[command.name] = command
    merged_commands = []
    # Common commands
    for command in base_commands:
        if command.name in merge_command_map.keys():
            merged_command = merge_command_map[command.name]
            if isinstance(command, GroupCommand):
                # Merge common group command with existing group command
                current_subcommands = list(command.subcommands)
                current_subcommands.extend(list(merged_command.subcommands))
                new_commands.append(
                    GroupCommand(
                        name=command.name,
                        help=command.help,
                        subcommands=current_subcommands,
                        api_operation=merged_command.api_operation,
                        description=merged_command.description,
                        epilog=command.epilog,
                    )
                )
            elif isinstance(command, ActionCommand):
                new_commands.append(merged_command)
            merged_commands.append(command.name)
        else:
            new_commands.append(command)
    # Discrete commands
    new_commands.extend(
        [
            merged_command
            for merged_command in merge_command_map.values()
            if merged_command.name not in merged_commands
        ]
    )
    return new_commands


command_factory = CommandFactory()

AUTH_COMMANDS = (
    ActionCommand(
        name="login",
        help="Login to the metadata database for personal usage. JWT Token must be provided via parameter.",
        description="Login to the metadata database",
        func=lazy_load_command("airflowctl.ctl.commands.auth_command.login"),
        args=(ARG_AUTH_URL, ARG_AUTH_TOKEN, ARG_AUTH_ENVIRONMENT, ARG_AUTH_USERNAME, ARG_AUTH_PASSWORD),
    ),
)

POOL_COMMANDS = (
    ActionCommand(
        name="import",
        help="Import pools",
        func=lazy_load_command("airflowctl.ctl.commands.pool_command.import_"),
        args=(ARG_POOL_FILE,),
    ),
    ActionCommand(
        name="export",
        help="Export all pools",
        func=lazy_load_command("airflowctl.ctl.commands.pool_command.export"),
        args=(
            ARG_POOL_FILE,
            ARG_OUTPUT,
        ),
    ),
)

CONFIG_COMMANDS = (
    ActionCommand(
        name="lint",
        help="Lint options for the configuration changes while migrating from Airflow 2 to Airflow 3",
        description="Lint options for the configuration changes while migrating from Airflow 2 to Airflow 3",
        func=lazy_load_command("airflowctl.ctl.commands.config_command.lint"),
        args=(
            ARG_CONFIG_SECTION,
            ARG_CONFIG_OPTION,
            ARG_CONFIG_IGNORE_SECTION,
            ARG_CONFIG_IGNORE_OPTION,
            ARG_CONFIG_VERBOSE,
        ),
    ),
)

VARIABLE_COMMANDS = (
    ActionCommand(
        name="import",
        help="Import variables",
        func=lazy_load_command("airflowctl.ctl.commands.variable_command.import_"),
        args=(ARG_VARIABLE_IMPORT, ARG_VARIABLE_ACTION_ON_EXISTING_KEY),
    ),
    ActionCommand(
        name="export",
        help="Export all variables",
        func=lazy_load_command("airflowctl.ctl.commands.variable_command.export"),
        args=(ARG_VARIABLE_EXPORT,),
    ),
)

core_commands: list[CLICommand] = [
    GroupCommand(
        name="auth",
        help="Manage authentication for CLI. "
        "Either pass token from environment variable/parameter or pass username and password.",
        subcommands=AUTH_COMMANDS,
    ),
    GroupCommand(
        name="pools",
        help="Manage Airflow pools",
        subcommands=POOL_COMMANDS,
    ),
    ActionCommand(
        name="version",
        help="Show version information",
        description="Show version information",
        func=lazy_load_command("airflowctl.ctl.commands.version_command.version_info"),
        args=(),
    ),
    GroupCommand(
        name="variables",
        help="Manage Airflow variables",
        subcommands=VARIABLE_COMMANDS,
    ),
    GroupCommand(
        name="config",
        help="View, lint and update configurations.",
        subcommands=CONFIG_COMMANDS,
    ),
]
# Add generated group commands
core_commands = merge_commands(
    base_commands=command_factory.group_commands, commands_will_be_merged=core_commands
)
