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

from collections.abc import Generator
from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.operators.ssm import SsmGetCommandInvocationOperator, SsmRunCommandOperator

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

COMMAND_ID = "test_command_id"
DOCUMENT_NAME = "test_ssm_custom_document"
INSTANCE_IDS = ["test_instance_id_1", "test_instance_id_2"]


class TestSsmRunCommandOperator:
    @pytest.fixture
    def mock_conn(self) -> Generator[SsmHook, None, None]:
        with mock.patch.object(SsmHook, "conn") as _conn:
            _conn.send_command.return_value = {
                "Command": {
                    "CommandId": COMMAND_ID,
                    "InstanceIds": INSTANCE_IDS,
                }
            }
            yield _conn

    def setup_method(self):
        self.operator = SsmRunCommandOperator(
            task_id="test_run_command_operator",
            document_name=DOCUMENT_NAME,
            run_command_kwargs={"InstanceIds": INSTANCE_IDS},
        )
        self.operator.defer = mock.MagicMock()

    @pytest.mark.parametrize(
        ("wait_for_completion", "deferrable"),
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(SsmHook, "get_waiter")
    def test_run_command_wait_combinations(self, mock_get_waiter, wait_for_completion, deferrable, mock_conn):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        command_id = self.operator.execute({})

        assert command_id == COMMAND_ID
        mock_conn.send_command.assert_called_once_with(DocumentName=DOCUMENT_NAME, InstanceIds=INSTANCE_IDS)
        assert mock_get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    def test_template_fields(self):
        validate_template_fields(self.operator)

    def test_deferrable_with_region(self, mock_conn):
        """Test that deferrable mode properly passes region and other AWS parameters to trigger."""
        self.operator.deferrable = True
        self.operator.region_name = "us-west-2"
        self.operator.verify = False
        self.operator.botocore_config = {"retries": {"max_attempts": 5}}

        command_id = self.operator.execute({})

        assert command_id == COMMAND_ID
        mock_conn.send_command.assert_called_once_with(DocumentName=DOCUMENT_NAME, InstanceIds=INSTANCE_IDS)

        # Verify defer was called with correct trigger parameters
        self.operator.defer.assert_called_once()
        call_args = self.operator.defer.call_args
        trigger = call_args[1]["trigger"]  # Get the trigger from kwargs

        # Verify the trigger has the correct parameters
        assert trigger.command_id == COMMAND_ID
        assert trigger.region_name == "us-west-2"
        assert trigger.verify is False
        assert trigger.botocore_config == {"retries": {"max_attempts": 5}}
        assert trigger.aws_conn_id == self.operator.aws_conn_id


class TestSsmGetCommandInvocationOperator:
    @pytest.fixture
    def mock_hook(self) -> Generator[mock.MagicMock, None, None]:
        with mock.patch.object(SsmGetCommandInvocationOperator, "hook") as _hook:
            yield _hook

    def setup_method(self):
        self.command_id = "test-command-id-123"
        self.instance_id = "i-1234567890abcdef0"
        self.operator = SsmGetCommandInvocationOperator(
            task_id="test_get_command_invocation",
            command_id=self.command_id,
            instance_id=self.instance_id,
        )

    def test_execute_with_specific_instance(self, mock_hook):
        # Mock response for specific instance
        mock_invocation_details = {
            "Status": "Success",
            "ResponseCode": 0,
            "StandardOutputContent": "Hello World",
            "StandardErrorContent": "",
            "ExecutionStartDateTime": "2023-01-01T12:00:00Z",
            "ExecutionEndDateTime": "2023-01-01T12:00:05Z",
            "DocumentName": "AWS-RunShellScript",
            "Comment": "Test command",
        }
        mock_hook.get_command_invocation.return_value = mock_invocation_details

        result = self.operator.execute({})

        # Verify hook was called correctly
        mock_hook.get_command_invocation.assert_called_once_with(self.command_id, self.instance_id)

        # Verify returned data structure - should use standardized format with invocations array
        expected_result = {
            "command_id": self.command_id,
            "invocations": [
                {
                    "instance_id": self.instance_id,
                    "status": "Success",
                    "response_code": 0,
                    "standard_output": "Hello World",
                    "standard_error": "",
                    "execution_start_time": "2023-01-01T12:00:00Z",
                    "execution_end_time": "2023-01-01T12:00:05Z",
                    "document_name": "AWS-RunShellScript",
                    "comment": "Test command",
                }
            ],
        }
        assert result == expected_result

    def test_execute_all_instances(self, mock_hook):
        # Setup operator without instance_id to get all instances
        operator = SsmGetCommandInvocationOperator(
            task_id="test_get_all_invocations",
            command_id=self.command_id,
        )

        # Mock list_command_invocations response
        mock_invocations = [
            {"InstanceId": "i-111"},
            {"InstanceId": "i-222"},
        ]
        mock_hook.list_command_invocations.return_value = {"CommandInvocations": mock_invocations}

        # Mock get_command_invocation responses
        mock_invocation_details_1 = {
            "Status": "Success",
            "ResponseCode": 0,
            "StandardOutputContent": "Output 1",
            "StandardErrorContent": "",
            "ExecutionStartDateTime": "2023-01-01T12:00:00Z",
            "ExecutionEndDateTime": "2023-01-01T12:00:05Z",
            "DocumentName": "AWS-RunShellScript",
            "Comment": "",
        }
        mock_invocation_details_2 = {
            "Status": "Failed",
            "ResponseCode": 1,
            "StandardOutputContent": "",
            "StandardErrorContent": "Error occurred",
            "ExecutionStartDateTime": "2023-01-01T12:00:00Z",
            "ExecutionEndDateTime": "2023-01-01T12:00:10Z",
            "DocumentName": "AWS-RunShellScript",
            "Comment": "",
        }

        mock_hook.get_command_invocation.side_effect = [
            mock_invocation_details_1,
            mock_invocation_details_2,
        ]

        result = operator.execute({})

        # Verify hook calls
        mock_hook.list_command_invocations.assert_called_once_with(self.command_id)
        assert mock_hook.get_command_invocation.call_count == 2
        mock_hook.get_command_invocation.assert_any_call(self.command_id, "i-111")
        mock_hook.get_command_invocation.assert_any_call(self.command_id, "i-222")

        # Verify returned data structure
        expected_result = {
            "command_id": self.command_id,
            "invocations": [
                {
                    "instance_id": "i-111",
                    "status": "Success",
                    "response_code": 0,
                    "standard_output": "Output 1",
                    "standard_error": "",
                    "execution_start_time": "2023-01-01T12:00:00Z",
                    "execution_end_time": "2023-01-01T12:00:05Z",
                    "document_name": "AWS-RunShellScript",
                    "comment": "",
                },
                {
                    "instance_id": "i-222",
                    "status": "Failed",
                    "response_code": 1,
                    "standard_output": "",
                    "standard_error": "Error occurred",
                    "execution_start_time": "2023-01-01T12:00:00Z",
                    "execution_end_time": "2023-01-01T12:00:10Z",
                    "document_name": "AWS-RunShellScript",
                    "comment": "",
                },
            ],
        }
        assert result == expected_result

    def test_execute_all_instances_with_error(self, mock_hook):
        # Setup operator without instance_id
        operator = SsmGetCommandInvocationOperator(
            task_id="test_get_all_with_error",
            command_id=self.command_id,
        )

        # Mock list_command_invocations response
        mock_invocations = [{"InstanceId": "i-111"}]
        mock_hook.list_command_invocations.return_value = {"CommandInvocations": mock_invocations}

        # Mock get_command_invocation to raise an exception
        mock_hook.get_command_invocation.side_effect = Exception("API Error")

        result = operator.execute({})

        # Verify error handling
        expected_result = {
            "command_id": self.command_id,
            "invocations": [{"instance_id": "i-111", "error": "API Error"}],
        }
        assert result == expected_result

    def test_template_fields(self):
        validate_template_fields(self.operator)
