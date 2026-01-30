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

from argparse import BooleanOptionalAction
from textwrap import dedent

import pytest

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
def test_args_create():
    return [
        (
            "--dag-id",
            {
                "help": "dag_id for backfill operation",
                "action": None,
                "default": None,
                "type": str,
                "dest": None,
            },
        ),
        (
            "--from-date",
            {
                "help": "from_date for backfill operation",
                "action": None,
                "default": None,
                "type": str,
                "dest": None,
            },
        ),
        (
            "--to-date",
            {
                "help": "to_date for backfill operation",
                "action": None,
                "default": None,
                "type": str,
                "dest": None,
            },
        ),
        (
            "--run-backwards",
            {
                "help": "run_backwards for backfill operation",
                "action": BooleanOptionalAction,
                "default": False,
                "type": bool,
                "dest": None,
            },
        ),
        (
            "--dag-run-conf",
            {
                "help": "dag_run_conf for backfill operation",
                "action": None,
                "default": None,
                "type": dict,
                "dest": None,
            },
        ),
        (
            "--reprocess-behavior",
            {
                "help": "reprocess_behavior for backfill operation",
                "action": None,
                "default": None,
                "type": str,
                "dest": None,
            },
        ),
        (
            "--max-active-runs",
            {
                "help": "max_active_runs for backfill operation",
                "action": None,
                "default": None,
                "type": int,
                "dest": None,
            },
        ),
    ]


"""
    help="Output format. Allowed values: json, yaml, plain, table (default: json)",
    metavar="(table, json, yaml, plain)",
    choices=("table", "json", "yaml", "plain"),
    default="json",
"""


@pytest.fixture(scope="module")
def test_args_list():
    return [
        (
            "--output",
            {
                "help": "Output format. Allowed values: json, yaml, plain, table (default: json)",
                "default": "json",
                "type": str,
                "dest": None,
            },
        ),
    ]


@pytest.fixture(scope="module")
def test_args_get():
    return [
        (
            "--backfill-id",
            {
                "help": "backfill_id for get operation in BackfillsOperations",
                "default": None,
                "type": str,
                "dest": None,
            },
        ),
        (
            "--output",
            {
                "help": "Output format. Allowed values: json, yaml, plain, table (default: json)",
                "default": "json",
                "type": str,
                "dest": None,
            },
        ),
    ]


@pytest.fixture(scope="module")
def test_args_delete():
    return [
        (
            "--backfill-id",
            {
                "help": "backfill_id for delete operation in BackfillsOperations",
                "default": None,
                "type": str,
                "dest": None,
            },
        ),
        (
            "--output",
            {
                "help": "Output format. Allowed values: json, yaml, plain, table (default: json)",
                "default": "json",
                "type": str,
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

    def teardown_method(self):
        """
        Remove the temporary file after the test.
        """
        try:
            import os

            os.remove("test_command.py")
        except FileNotFoundError:
            pass

    def test_command_factory(
        self, no_op_method, test_args_create, test_args_list, test_args_get, test_args_delete
    ):
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
                            self.response = self.client.post("backfills", json=backfill.model_dump(mode="json"))
                            return BackfillResponse.model_validate_json(self.response.content)
                        except ServerResponseError as e:
                            raise e
                    def list(self) -> BackfillListResponse:
                        params = {"dag_id": dag_id} if dag_id else {}
                        self.response = self.client.get("backfills", params=params)
                        return BackfillListResponse.model_validate_json(self.response.content)
                    def get(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
                        self.response = self.client.get(f"backfills/{backfill_id}")
                        return BackfillResponse.model_validate_json(self.response.content)
                    def delete(self, backfill_id: str) -> ServerResponseError | None:
                        self.response = self.client.delete(f"backfills/{backfill_id}")
                        return None
            """,
        )

        command_factory = CommandFactory(file_path=temp_file)
        generated_group_commands = command_factory.group_commands

        for generated_group_command in generated_group_commands:
            assert isinstance(generated_group_command, GroupCommand)
            assert generated_group_command.name == "backfills"
            assert generated_group_command.help == "Perform Backfills operations"
            for sub_command in generated_group_command.subcommands:
                if sub_command.name == "create":
                    for arg, test_arg in zip(sub_command.args, test_args_create):
                        assert arg.flags[0] == test_arg[0]
                        assert arg.kwargs["help"] == test_arg[1]["help"]
                        assert arg.kwargs["action"] == test_arg[1]["action"]
                        assert arg.kwargs["default"] == test_arg[1]["default"]
                        assert arg.kwargs["type"] == test_arg[1]["type"]
                        assert arg.kwargs["dest"] == test_arg[1]["dest"]
                        print(arg.flags)
                elif sub_command.name == "list":
                    for arg, test_arg in zip(sub_command.args, test_args_list):
                        assert arg.flags[0] == test_arg[0]
                        assert arg.kwargs["help"] == test_arg[1]["help"]
                        assert arg.kwargs["default"] == test_arg[1]["default"]
                        assert arg.kwargs["type"] == test_arg[1]["type"]
                elif sub_command.name == "get":
                    for arg, test_arg in zip(sub_command.args, test_args_get):
                        assert arg.flags[0] == test_arg[0]
                        assert arg.kwargs["help"] == test_arg[1]["help"]
                        assert arg.kwargs["default"] == test_arg[1]["default"]
                        assert arg.kwargs["type"] == test_arg[1]["type"]
                elif sub_command.name == "delete":
                    for arg, test_arg in zip(sub_command.args, test_args_delete):
                        assert arg.flags[0] == test_arg[0]
                        assert arg.kwargs["help"] == test_arg[1]["help"]
                        assert arg.kwargs["default"] == test_arg[1]["default"]
                        assert arg.kwargs["type"] == test_arg[1]["type"]


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

    def test_trigger_dag_run_defaults_logical_date_to_now(self):
        """Test that trigger command defaults logical_date to now when not provided."""
        from datetime import datetime, timezone

        from airflowctl.api.datamodels.generated import TriggerDAGRunPostBody

        # Simulate the logic in _get_func from cli_config.py
        # This is the actual code path that runs when user doesn't provide --logical-date

        # Step 1: Simulate CLI args being parsed (logical_date=None)
        method_params = {
            "trigger_dag_run": {
                "dag_run_id": None,
                "data_interval_start": None,
                "data_interval_end": None,
                "logical_date": None,  # User did not provide --logical-date
                "run_after": None,
                "conf": None,
                "note": None,
                "partition_key": None,
            }
        }

        # Step 2: Apply the defaulting logic (from cli_config.py lines 622-630)
        datamodel = TriggerDAGRunPostBody
        datamodel_param_name = "trigger_dag_run"

        if (
            datamodel.__name__ == "TriggerDAGRunPostBody"
            and "logical_date" in method_params[datamodel_param_name]
            and method_params[datamodel_param_name]["logical_date"] is None
        ):
            method_params[datamodel_param_name]["logical_date"] = datetime.now(timezone.utc)

        # Step 3: Create the Pydantic model (what happens in the actual code)
        trigger_body = datamodel.model_validate(method_params[datamodel_param_name])

        # Step 4: Verify logical_date was set to now
        assert trigger_body.logical_date is not None, "logical_date should be defaulted to now"
        assert isinstance(trigger_body.logical_date, datetime)

        # Verify it's close to current time (within 5 seconds)
        time_diff = abs((datetime.now(timezone.utc) - trigger_body.logical_date).total_seconds())
        assert time_diff < 5, f"logical_date should be close to now, but diff is {time_diff} seconds"

        # Also verify timezone is UTC
        assert trigger_body.logical_date.tzinfo is not None, "logical_date should have timezone info"

    def test_apply_datamodel_defaults_trigger_dag_run_with_none(self):
        """Test _apply_datamodel_defaults sets logical_date to now when None for TriggerDAGRunPostBody."""
        from datetime import datetime, timezone

        from airflowctl.api.datamodels.generated import TriggerDAGRunPostBody

        command_factory = CommandFactory()

        # Test with logical_date=None
        params = {"logical_date": None, "conf": {}}
        result = command_factory._apply_datamodel_defaults(TriggerDAGRunPostBody, params)

        assert result["logical_date"] is not None, "logical_date should be defaulted to now"
        assert isinstance(result["logical_date"], datetime)

        # Verify it's close to current time (within 5 seconds)
        time_diff = abs((datetime.now(timezone.utc) - result["logical_date"]).total_seconds())
        assert time_diff < 5, f"logical_date should be close to now, but diff is {time_diff} seconds"

        # Verify timezone is UTC
        assert result["logical_date"].tzinfo is not None, "logical_date should have timezone info"

    def test_apply_datamodel_defaults_trigger_dag_run_with_value(self):
        """Test _apply_datamodel_defaults preserves existing logical_date for TriggerDAGRunPostBody."""
        from datetime import datetime, timezone

        from airflowctl.api.datamodels.generated import TriggerDAGRunPostBody

        command_factory = CommandFactory()

        # Test with an existing logical_date value
        specific_date = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        params = {"logical_date": specific_date, "conf": {}}
        result = command_factory._apply_datamodel_defaults(TriggerDAGRunPostBody, params)

        # Should preserve the provided value, not override it
        assert result["logical_date"] == specific_date, "logical_date should not be changed when already set"

    def test_apply_datamodel_defaults_trigger_dag_run_without_logical_date(self):
        """Test _apply_datamodel_defaults doesn't add logical_date if not present."""
        from airflowctl.api.datamodels.generated import TriggerDAGRunPostBody

        command_factory = CommandFactory()

        # Test without logical_date key
        params = {"conf": {}}
        result = command_factory._apply_datamodel_defaults(TriggerDAGRunPostBody, params)

        # Should not add logical_date if it wasn't in params
        assert "logical_date" not in result, "logical_date should not be added if not originally present"

    def test_apply_datamodel_defaults_other_datamodel(self):
        """Test _apply_datamodel_defaults doesn't modify params for other datamodels."""
        from airflowctl.api.datamodels.generated import BackfillPostBody

        command_factory = CommandFactory()

        # Test with a different datamodel (BackfillPostBody)
        params = {"dag_id": "test_dag", "from_date": None}
        result = command_factory._apply_datamodel_defaults(BackfillPostBody, params)

        # Should return params unchanged for other datamodels
        assert result == params, "Params should be unchanged for non-TriggerDAGRunPostBody datamodels"
