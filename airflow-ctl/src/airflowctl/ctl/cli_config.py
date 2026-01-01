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
import datetime
import getpass
import inspect
import os
from argparse import Namespace
from collections.abc import Callable, Iterable
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Any, NamedTuple

import httpx
import rich

import airflowctl.api.datamodels.generated as generated_datamodels
from airflowctl.api.client import NEW_API_CLIENT, Client, ClientKind, provide_api_client
from airflowctl.api.operations import BaseOperations, ServerResponseError
from airflowctl.ctl.console_formatting import AirflowConsole
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
    import sys

    if os.getenv("AIRFLOW_CLI_DEBUG_MODE") == "true":
        rich.print(
            "[yellow]Debug mode is enabled. Please be aware that your credentials are not secure.\n"
            "Please unset AIRFLOW_CLI_DEBUG_MODE or set it to false.[/yellow]"
        )

    try:
        function(args)
    except (
        AirflowCtlCredentialNotFoundException,
        AirflowCtlConnectionException,
        AirflowCtlNotFoundException,
    ) as e:
        rich.print(f"command failed due to {e}")
        sys.exit(1)
    except (httpx.RemoteProtocolError, httpx.ReadError) as e:
        rich.print(f"[red]Remote protocol error: {e}[/red]")
        if "Server disconnected without sending a response." in str(e):
            rich.print(
                f"[red]Server response error: {e}. "
                "Please check if the server is running and the API URL is correct.[/red]"
            )
    except httpx.ReadTimeout as e:
        rich.print(f"[red]Read timeout error: {e}[/red]")
        if "timed out" in str(e):
            rich.print("[red]Please check if the server is running and the API ready to accept calls.[/red]")
    except ServerResponseError as e:
        rich.print(f"Server response error: {e}")
        if "Client error message:" in str(e):
            rich.print(
                "[red]Client error, [/red] "
                "Please check the command and its parameters. "
                "If you need help, run the command with --help."
            )


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
        if values is None:
            values = getpass.getpass()
        setattr(namespace, self.dest, values)


# Common Positional Arguments
ARG_FILE = Arg(
    flags=("file",),
    metavar="FILEPATH",
    help="File path to read from for import commands.",
)
ARG_OUTPUT = Arg(
    (
        "--output",
        "-o",
    ),
    help="Output format. Allowed values: json, yaml, plain, table (default: json)",
    metavar="(table, json, yaml, plain)",
    choices=("table", "json", "yaml", "plain"),
    default="json",
    type=str,
)

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

# Dag Commands Args
ARG_DAG_ID = Arg(
    flags=("dag_id",),
    type=str,
    help="The DAG ID of the DAG to pause or unpause",
)

# Variable Commands Args
ARG_VARIABLE_ACTION_ON_EXISTING_KEY = Arg(
    flags=("-a", "--action-on-existing-key"),
    type=str,
    default="overwrite",
    help="Action to take if we encounter a variable key that already exists.",
    choices=("overwrite", "fail", "skip"),
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

# Version Command Args
ARG_REMOTE = Arg(
    flags=("--remote",),
    help="Fetch the Airflow version in remote server, otherwise only shows the local airflowctl version",
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


CLICommand = ActionCommand | GroupCommand | GroupCommandParser


class CommandFactory:
    """Factory class that creates 1-1 mapping with airflowctl/api/operations."""

    datamodels_extended_map: dict[str, list[str]]
    operations: list[dict]
    args_map: dict[tuple, list[Arg]]
    func_map: dict[tuple, Callable]
    commands_map: dict[str, list[ActionCommand]]
    group_commands_list: list[CLICommand]
    output_command_list: list[str]
    exclude_operation_names: list[str]
    exclude_method_names: list[str]

    def __init__(self, file_path: str | Path | None = None):
        self.datamodels_extended_map = {}
        self.func_map = {}
        self.operations = []
        self.args_map = {}
        self.commands_map = {}
        self.group_commands_list = []
        self.file_path = inspect.getfile(BaseOperations) if file_path is None else file_path
        # Excluded Lists are in Class Level for further usage and avoid searching them
        # Exclude parameters that are not needed for CLI from datamodels
        self.excluded_parameters = ["schema_"]
        # This list is used to determine if the command/operation needs to output data
        self.output_command_list = ["list", "get", "create", "delete", "update", "trigger"]
        self.exclude_operation_names = ["LoginOperations", "VersionOperations", "BaseOperations"]
        self.exclude_method_names = [
            "error",
            "__init__",
            "__init_subclass__",
            "_check_flag_and_exit_if_server_response_error",
            # Excluding bulk operation. Out of scope for CLI. Should use implemented commands.
            "bulk",
        ]
        self.excluded_output_keys = [
            "total_entries",
        ]

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

        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ClassDef)
                and "Operations" in node.name
                and node.name not in self.exclude_operation_names
                and node.body
            ):
                for child in node.body:
                    if isinstance(child, ast.FunctionDef) and child.name not in self.exclude_method_names:
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
    def _python_type_from_string(type_name: str | type) -> type | Callable:
        """
        Return the corresponding Python *type* for a primitive type name string.

        This helper is used when generating ``argparse`` CLI arguments from the
        OpenAPI-derived operation signatures. Without this mapping the CLI would
        incorrectly assume every primitive parameter is a *string*, potentially
        leading to type errors or unexpected behaviour when invoking the REST
        API.
        """
        if "|" in str(type_name):
            type_name = [t.strip() for t in str(type_name).split("|") if t.strip() != "None"].pop()
        mapping: dict[str, type | Callable] = {
            "int": int,
            "float": float,
            "bool": bool,
            "str": str,
            "bytes": bytes,
            "list": list,
            "dict": dict,
            "tuple": tuple,
            "set": set,
            "datetime.datetime": datetime.datetime,
            "dict[str, typing.Any]": dict,
        }
        # Default to ``str`` to preserve previous behaviour for any unrecognised
        # type names while still allowing the CLI to function.
        if isinstance(type_name, type):
            type_name = type_name.__name__
        return mapping.get(str(type_name), str)

    @staticmethod
    def _create_arg(
        arg_flags: tuple,
        arg_type: type | Callable,
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
                        arg_type=self._python_type_from_string(field_type.annotation),
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
                        arg_type=self._python_type_from_string(annotation),
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
                        is_bool = parameter_type == "bool"
                        args.append(
                            self._create_arg(
                                arg_flags=("--" + self._sanitize_arg_parameter_key(parameter_key),),
                                arg_type=self._python_type_from_string(parameter_type),
                                arg_action=argparse.BooleanOptionalAction if is_bool else None,
                                arg_help=f"{parameter_key} for {operation.get('name')} operation in {operation.get('parent').name}",
                                arg_default=False if is_bool else None,
                            )
                        )
                    else:
                        args.extend(
                            self._create_arg_for_non_primitive_type(
                                parameter_type=parameter_type, parameter_key=parameter_key
                            )
                        )

            if any(operation.get("name").startswith(cmd) for cmd in self.output_command_list):
                args.extend([ARG_OUTPUT, ARG_AUTH_ENVIRONMENT])

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
            datamodel_param_name = None
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
                            if parameter_key not in method_params:
                                method_params[parameter_key] = {}
                                datamodel_param_name = parameter_key
                            if expanded_parameter in self.excluded_parameters:
                                continue
                            if expanded_parameter in args_dict.keys():
                                method_params[parameter_key][
                                    self._sanitize_method_param_key(expanded_parameter)
                                ] = args_dict[expanded_parameter]

            if datamodel:
                if datamodel_param_name:
                    method_params[datamodel_param_name] = datamodel.model_validate(
                        method_params[datamodel_param_name]
                    )
                else:
                    method_params = datamodel.model_validate(method_params)
                method_output = operation_method_object(**method_params)
            else:
                method_output = operation_method_object(**method_params)

            def convert_to_dict(obj: Any, api_operation_name: str) -> dict | Any:
                """Recursively convert an object to a dictionary or list of dictionaries."""
                if hasattr(obj, "model_dump"):
                    return obj.model_dump(mode="json")
                # Handle delete operation which returns a string of the deleted entity name
                if isinstance(obj, str):
                    return {"operation": api_operation_name, "entity": obj}
                return obj

            def check_operation_and_collect_list_of_dict(dict_obj: dict) -> list:
                """Check if the object is a nested dictionary and collect list of dictionaries."""

                def is_dict_nested(obj: dict) -> bool:
                    """Check if the object is a nested dictionary."""
                    return any(isinstance(i, dict) or isinstance(i, list) for i in obj.values())

                if is_dict_nested(dict_obj):
                    iteration_dict = dict_obj.copy()
                    for key, value in iteration_dict.items():
                        if key in self.excluded_output_keys:
                            del dict_obj[key]
                            continue
                        if isinstance(value, Enum):
                            dict_obj[key] = value.value
                        if isinstance(value, list):
                            dict_obj[key] = value
                        if isinstance(value, dict):
                            dict_obj[key] = check_operation_and_collect_list_of_dict(value)

                # If dict_obj only have single key return value instead of list
                # This can happen since we are excluding some keys from user such as total_entries from list operations
                if len(dict_obj) == 1:
                    return dict_obj[next(iter(dict_obj.keys()))]
                # If not nested, return the object as a list which the result should be already a dict
                return [dict_obj]

            AirflowConsole().print_as(
                data=check_operation_and_collect_list_of_dict(
                    convert_to_dict(method_output, api_operation["name"])
                ),
                output=args.output,
            )

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

CONNECTION_COMMANDS = (
    ActionCommand(
        name="import",
        help="Import connections from a file exported with local CLI.",
        func=lazy_load_command("airflowctl.ctl.commands.connection_command.import_"),
        args=(Arg(flags=("file",), metavar="FILEPATH", help="Connections JSON file"),),
    ),
)

DAG_COMMANDS = (
    ActionCommand(
        name="pause",
        help="Pause a Dag",
        func=lazy_load_command("airflowctl.ctl.commands.dag_command.pause"),
        args=(
            ARG_DAG_ID,
            ARG_OUTPUT,
        ),
    ),
    ActionCommand(
        name="unpause",
        help="Unpause a Dag",
        func=lazy_load_command("airflowctl.ctl.commands.dag_command.unpause"),
        args=(
            ARG_DAG_ID,
            ARG_OUTPUT,
        ),
    ),
)

POOL_COMMANDS = (
    ActionCommand(
        name="import",
        help="Import pools",
        func=lazy_load_command("airflowctl.ctl.commands.pool_command.import_"),
        args=(ARG_FILE,),
    ),
    ActionCommand(
        name="export",
        help="Export all pools",
        func=lazy_load_command("airflowctl.ctl.commands.pool_command.export"),
        args=(
            ARG_FILE,
            ARG_OUTPUT,
        ),
    ),
)

VARIABLE_COMMANDS = (
    ActionCommand(
        name="import",
        help="Import variables from a file exported with local CLI.",
        func=lazy_load_command("airflowctl.ctl.commands.variable_command.import_"),
        args=(ARG_FILE, ARG_VARIABLE_ACTION_ON_EXISTING_KEY),
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
        name="config",
        help="View, lint and update configurations.",
        subcommands=CONFIG_COMMANDS,
    ),
    GroupCommand(
        name="connections",
        help="Manage Airflow connections",
        subcommands=CONNECTION_COMMANDS,
    ),
    GroupCommand(
        name="dags",
        help="Manage Airflow Dags",
        subcommands=DAG_COMMANDS,
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
        args=(
            ARG_AUTH_ENVIRONMENT,
            ARG_REMOTE,
        ),
    ),
    GroupCommand(
        name="variables",
        help="Manage Airflow variables",
        subcommands=VARIABLE_COMMANDS,
    ),
]
# Add generated group commands
core_commands = merge_commands(
    base_commands=command_factory.group_commands, commands_will_be_merged=core_commands
)
