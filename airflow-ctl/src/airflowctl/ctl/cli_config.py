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
import inspect
import os
from collections.abc import Iterable
from typing import Callable, NamedTuple, Union

import airflowctl.api.datamodels.generated as generated_datamodels
from airflow.sdk.api.client import ServerResponseError
from airflowctl.api.client import Client, provide_api_client
from airflowctl.api.operations import BaseOperations
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
    description: str | None = None
    epilog: str | None = None


CLICommand = Union[ActionCommand, GroupCommand]


class AirflowCtlCommandFactory:
    """Factory class that creates 1-1 mapping with api operations."""

    operations: list[dict] = []
    args_map: dict[str, list[Arg]] = {}
    func_map: dict[str, Callable] = {}
    commands_map: dict[str, list[ActionCommand]] = {}
    group_commands: list[GroupCommand] = []

    def __init__(self):
        self.create_commands()

    @staticmethod
    def inspect_operations() -> list:
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

        file_path = inspect.getfile(BaseOperations)

        with open(file_path, encoding="utf-8") as file:
            tree = ast.parse(file.read(), filename=file_path)

        functions = []
        exclude_method_names = ["error", "__init__", "__init_subclass__"]
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and "Operations" in node.name:
                for child in node.body:
                    if isinstance(child, ast.FunctionDef) and child.name not in exclude_method_names:
                        functions.append(get_function_details(node=child, parent_node=node))
        return functions

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

    def create_args_map_from_operation(self):
        """Create Arg from Operation Method checking for parameters and return types."""
        for operation in self.operations:
            args = []
            for parameter in operation.get("parameters"):
                for parameter_key, parameter_type in parameter.items():
                    if self._is_primitive_type(type_name=parameter_type):
                        args.append(
                            Arg(
                                flags=("--" + parameter_key,),
                                type=type(parameter_type),
                                help=f"{parameter_key} for {operation.get('name')} operation in {operation.get('parent').name}",
                            )
                        )
                    else:
                        parameter_type = getattr(generated_datamodels, parameter_type)
                        for field, field_type in parameter_type.__fields__.items():
                            if "Optional" not in str(field_type):
                                args.append(
                                    Arg(
                                        flags=("--" + field,),
                                        type=field_type.annotation,
                                        dest=field,
                                        help=f"{parameter_type.__name__}:{field} for {operation.get('name')} operation in {operation.get('parent').name}",
                                    )
                                )
                            else:
                                args.append(
                                    Arg(
                                        flags=("--" + field,),
                                        type=field_type.annotation,
                                        help=f"{parameter_type.__name__}:{field} for {operation.get('name')} operation in {operation.get('parent').name}",
                                        default=None,
                                    )
                                )
            self.args_map[f"{operation.get('name')}-{operation.get('parent').name}"] = args

    def create_func_map_from_operation(self):
        """Create function map from Operation Method checking for parameters and return types."""
        for operation in self.operations:

            @provide_api_client
            def _get_func(cli_api_client: Client, **kwargs):
                operation_class = getattr(cli_api_client, operation.get("parent_node").name)
                operation_method = getattr(operation_class, operation.get("name"))
                return operation_method(**kwargs)

            self.func_map[f"{operation.get('name')}-{operation.get('parent').name}"] = _get_func

    def create_commands(self):
        """Create commands from Operation Method."""
        self.operations = self.inspect_operations()
        self.create_args_map_from_operation()
        self.create_func_map_from_operation()

        for operation in self.operations:
            operation_name = operation.get("name")
            operation_group_name = operation.get("parent").name
            if operation_group_name not in self.commands_map:
                self.commands_map[operation_group_name] = []
            self.commands_map[operation_group_name].append(
                ActionCommand(
                    name=operation.get("name").replace("_", "-"),
                    help=f"Perform {operation_name} operation",
                    func=self.func_map.get(f"{operation_name}-{operation_group_name}"),
                    args=self.args_map.get(f"{operation_name}-{operation_group_name}"),
                )
            )

        for group_name, action_commands in self.commands_map.items():
            self.group_commands.append(
                GroupCommand(
                    name=group_name.replace("Operations", "").lower(),
                    help=f"Perform {group_name.replace('Operations', '')} operations",
                    subcommands=action_commands,
                )
            )


airflow_ctl_command_factory = AirflowCtlCommandFactory()


AUTH_COMMANDS = (
    ActionCommand(
        name="login",
        help="Login to the metadata database for personal usage. JWT Token must be provided via parameter.",
        description="Login to the metadata database",
        func=lazy_load_command("airflowctl.ctl.commands.auth_command.login"),
        args=(ARG_AUTH_URL, ARG_AUTH_TOKEN, ARG_AUTH_ENVIRONMENT),
    ),
)


core_commands: list[CLICommand] = [
    GroupCommand(
        name="auth",
        help="Manage authentication for CLI. Please acquire a token from the api-server first. "
        "You need to pass the token to subcommand to use `login`.",
        subcommands=AUTH_COMMANDS,
    ),
]

core_commands.extend(airflow_ctl_command_factory.group_commands)
