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

import datetime
from argparse import BooleanOptionalAction
from textwrap import dedent
from typing import Any

import pytest

from airflowctl.api.datamodels.generated import ReprocessBehavior
from airflowctl.ctl.cli_config import ActionCommand, CommandFactory, GroupCommand, merge_commands


@pytest.fixture
def no_op_method():
    """
    No operation method to be used as a placeholder for the actual method.
    """

    def no_op():
        pass

    return no_op


@pytest.fixture(scope="module")
def test_args():
    return [
        (
            "--dag-id",
            {
                "help": "Argument Type: <class 'str'>, dag_id for backfill operation",
                "action": None,
                "default": None,
                "type": str,
                "dest": None,
            },
        ),
        (
            "--from-date",
            {
                "help": "Argument Type: <class 'datetime.datetime'>, from_date for backfill operation",
                "action": None,
                "default": None,
                "type": datetime.datetime,
                "dest": None,
            },
        ),
        (
            "--to-date",
            {
                "help": "Argument Type: <class 'datetime.datetime'>, to_date for backfill operation",
                "action": None,
                "default": None,
                "type": datetime.datetime,
                "dest": None,
            },
        ),
        (
            "--run-backwards",
            {
                "help": "Argument Type: <class 'bool'>, run_backwards for backfill operation",
                "action": BooleanOptionalAction,
                "default": False,
                "type": bool,
                "dest": None,
            },
        ),
        (
            "--dag-run-conf",
            {
                "help": "Argument Type: dict[str, typing.Any], dag_run_conf for backfill operation",
                "action": None,
                "default": None,
                "type": dict[str, Any],
                "dest": None,
            },
        ),
        (
            "--reprocess-behavior",
            {
                "help": "Argument Type: <enum 'ReprocessBehavior'>, reprocess_behavior for backfill operation",
                "action": None,
                "default": None,
                "type": ReprocessBehavior,
                "dest": None,
            },
        ),
        (
            "--max-active-runs",
            {
                "help": "Argument Type: <class 'int'>, max_active_runs for backfill operation",
                "action": None,
                "default": None,
                "type": int,
                "dest": None,
            },
        ),
    ]


class TestCommandFactory:
    @classmethod
    def _save_temp_operations_py(cls, temp_file: str, file_content) -> None:
        """
        Save a temporary operations.py file with a simple Command Class to test the command factory.
        """
        with open(temp_file, "w") as f:
            f.write(dedent(file_content))

    def test_command_factory(self, no_op_method, test_args):
        """
        Test the command factory.
        """
        # Create temporary file with pytest and write simple Command Class(check airflow-ctl/src/airflowctl/api/operations.py) to file
        # to test the command factory
        # Create a temporary file
        temp_file = "test_command.py"
        self._save_temp_operations_py(
            temp_file=temp_file,
            file_content="""
                class NotAnOperation:
                    def test_method(self):
                        '''I am not included in the command factory.'''
                        pass

                class BackfillsOperations(BaseOperations):
                    def create(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
                        try:
                            self.response = self.client.post("backfills", data=backfill.model_dump())
                            return BackfillResponse.model_validate_json(self.response.content)
                        except ServerResponseError as e:
                            raise e
            """,
        )

        command_factory = CommandFactory(file_path=temp_file)
        generated_group_commands = command_factory.group_commands

        for generated_group_command in generated_group_commands:
            assert isinstance(generated_group_command, GroupCommand)
            assert generated_group_command.name == "backfills"
            assert generated_group_command.help == "Perform Backfills operations"
            for sub_command in generated_group_command.subcommands:
                assert sub_command.name == "create"
                for arg, test_arg in zip(sub_command.args, test_args):
                    assert arg.flags[0] == test_arg[0]
                    assert arg.kwargs["help"] == test_arg[1]["help"]
                    assert arg.kwargs["action"] == test_arg[1]["action"]
                    assert arg.kwargs["default"] == test_arg[1]["default"]
                    assert arg.kwargs["type"] == test_arg[1]["type"]
                    assert arg.kwargs["dest"] == test_arg[1]["dest"]


class TestCliConfigMethods:
    def test_merge_commands(self, no_op_method):
        """Test the merge_commands method."""
        # Create two Command objects with different names and help texts
        action_commands_1 = (
            ActionCommand(
                name="subcommand1",
                help="This is command 1",
                func=no_op_method,
                args=(),
            ),
            ActionCommand(
                name="subcommand2",
                help="This is command 2",
                func=no_op_method,
                args=(),
            ),
        )
        action_commands_2 = (
            ActionCommand(
                name="subcommand3",
                help="This is command 3",
                func=no_op_method,
                args=(),
            ),
            ActionCommand(
                name="subcommand4",
                help="This is command 4",
                func=no_op_method,
                args=(),
            ),
        )
        command_list_1 = [
            GroupCommand(
                name="command1",
                help="This is command 1",
                subcommands=action_commands_1,
            ),
            GroupCommand(
                name="command2",
                help="This is command 2",
                subcommands=action_commands_2,
            ),
        ]
        command_list_2 = [
            GroupCommand(
                name="command1",
                help="This is command 1 new help",
                description="This is command 1 new description",
                subcommands=action_commands_2,
            ),
            GroupCommand(
                name="command4",
                help="This is command 4",
                subcommands=action_commands_1,
            ),
        ]

        # Merge the commands
        merged_command = merge_commands(base_commands=command_list_1, commands_will_be_merged=command_list_2)
        merged_command_names = [command.name for command in merged_command]
        assert "command1" in merged_command_names
        assert "command2" in merged_command_names
        assert "command3" not in merged_command_names
        assert "command4" in merged_command_names

        for command in merged_command:
            if command.name == "command1":
                # assert command.help == "This is command 1 new help"
                # assert command.description == "This is command 1 new description"
                sub_command_names = [sc.name for sc in list(command.subcommands)]
                print(f"sub_command_names: {sub_command_names}")
                assert "subcommand1" in sub_command_names
                assert "subcommand2" in sub_command_names
                assert "subcommand3" in sub_command_names
                assert "subcommand4" in sub_command_names
