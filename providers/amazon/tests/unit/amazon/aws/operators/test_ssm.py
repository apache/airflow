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
from botocore.exceptions import WaiterError

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

    def test_operator_default_fails_on_nonzero_exit(self, mock_conn):
        """
        Test traditional mode where fail_on_nonzero_exit=True (default).

        Verifies that when fail_on_nonzero_exit is True (the default), the operator
        raises an exception when the waiter encounters a command failure.
        """
        self.operator.wait_for_completion = True

        # Mock waiter to raise WaiterError (simulating command failure)
        mock_waiter = mock.MagicMock()
        mock_waiter.wait.side_effect = WaiterError(
            name="command_executed",
            reason="Waiter encountered a terminal failure state",
            last_response={"Status": "Failed"},
        )

        with mock.patch.object(SsmHook, "get_waiter", return_value=mock_waiter):
            # Should raise WaiterError in traditional mode
            with pytest.raises(WaiterError):
                self.operator.execute({})

    def test_operator_enhanced_mode_tolerates_failed_status(self, mock_conn):
        """
        Test enhanced mode where fail_on_nonzero_exit=False tolerates Failed status.

        Verifies that when fail_on_nonzero_exit is False, the operator completes
        successfully even when the command returns a Failed status with non-zero exit code.
        """
        self.operator.wait_for_completion = True
        self.operator.fail_on_nonzero_exit = False

        # Mock waiter to raise WaiterError
        mock_waiter = mock.MagicMock()
        mock_waiter.wait.side_effect = WaiterError(
            name="command_executed",
            reason="Waiter encountered a terminal failure state",
            last_response={"Status": "Failed"},
        )

        # Mock get_command_invocation to return Failed status with exit code
        with (
            mock.patch.object(SsmHook, "get_waiter", return_value=mock_waiter),
            mock.patch.object(
                SsmHook, "get_command_invocation", return_value={"Status": "Failed", "ResponseCode": 1}
            ),
        ):
            # Should NOT raise in enhanced mode for Failed status
            command_id = self.operator.execute({})
            assert command_id == COMMAND_ID

    def test_operator_enhanced_mode_fails_on_timeout(self, mock_conn):
        """
        Test enhanced mode still fails on TimedOut status.

        Verifies that even when fail_on_nonzero_exit is False, the operator
        still raises an exception for AWS-level failures like TimedOut.
        """
        self.operator.wait_for_completion = True
        self.operator.fail_on_nonzero_exit = False

        # Mock waiter to raise WaiterError
        mock_waiter = mock.MagicMock()
        mock_waiter.wait.side_effect = WaiterError(
            name="command_executed",
            reason="Waiter encountered a terminal failure state",
            last_response={"Status": "TimedOut"},
        )

        # Mock get_command_invocation to return TimedOut status
        with (
            mock.patch.object(SsmHook, "get_waiter", return_value=mock_waiter),
            mock.patch.object(
                SsmHook, "get_command_invocation", return_value={"Status": "TimedOut", "ResponseCode": -1}
            ),
        ):
            # Should raise even in enhanced mode for TimedOut
            with pytest.raises(WaiterError):
                self.operator.execute({})

    def test_operator_enhanced_mode_fails_on_cancelled(self, mock_conn):
        """
        Test enhanced mode still fails on Cancelled status.

        Verifies that even when fail_on_nonzero_exit is False, the operator
        still raises an exception for AWS-level failures like Cancelled.
        """
        self.operator.wait_for_completion = True
        self.operator.fail_on_nonzero_exit = False

        # Mock waiter to raise WaiterError
        mock_waiter = mock.MagicMock()
        mock_waiter.wait.side_effect = WaiterError(
            name="command_executed",
            reason="Waiter encountered a terminal failure state",
            last_response={"Status": "Cancelled"},
        )

        # Mock get_command_invocation to return Cancelled status
        with (
            mock.patch.object(SsmHook, "get_waiter", return_value=mock_waiter),
            mock.patch.object(
                SsmHook, "get_command_invocation", return_value={"Status": "Cancelled", "ResponseCode": -1}
            ),
        ):
            # Should raise even in enhanced mode for Cancelled
            with pytest.raises(WaiterError):
                self.operator.execute({})

    @mock.patch("airflow.providers.amazon.aws.operators.ssm.SsmRunCommandTrigger")
    def test_operator_passes_parameter_to_trigger(self, mock_trigger_class, mock_conn):
        """
        Test that fail_on_nonzero_exit parameter is passed to trigger in deferrable mode.

        Verifies that when using deferrable mode, the fail_on_nonzero_exit parameter
        is correctly passed to the SsmRunCommandTrigger.
        """
        self.operator.deferrable = True
        self.operator.fail_on_nonzero_exit = False

        with mock.patch.object(self.operator, "defer") as mock_defer:
            command_id = self.operator.execute({})

            assert command_id == COMMAND_ID
            mock_conn.send_command.assert_called_once_with(
                DocumentName=DOCUMENT_NAME, InstanceIds=INSTANCE_IDS
            )

            # Verify defer was called
            mock_defer.assert_called_once()

            # Verify the trigger was instantiated with correct parameters
            mock_trigger_class.assert_called_once()
            call_kwargs = mock_trigger_class.call_args[1]

            assert call_kwargs["command_id"] == COMMAND_ID
            assert call_kwargs["fail_on_nonzero_exit"] is False


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

    def test_exit_code_routing_use_case(self, mock_hook):
        """
        Test that demonstrates the exit code routing use case.

        This test verifies that SsmGetCommandInvocationOperator correctly retrieves
        exit codes and status information that can be used for workflow routing,
        particularly when used with SsmRunCommandOperator in enhanced mode
        (fail_on_nonzero_exit=False).
        """
        # Mock response with various exit codes that might be used for routing
        mock_invocation_details = {
            "Status": "Failed",  # Command failed but we want to route based on exit code
            "ResponseCode": 42,  # Custom exit code for specific routing logic
            "StandardOutputContent": "Partial success - some items processed",
            "StandardErrorContent": "Warning: 3 items skipped",
            "ExecutionStartDateTime": "2023-01-01T12:00:00Z",
            "ExecutionEndDateTime": "2023-01-01T12:00:05Z",
            "DocumentName": "AWS-RunShellScript",
            "Comment": "Data processing script",
        }
        mock_hook.get_command_invocation.return_value = mock_invocation_details

        result = self.operator.execute({})

        # Verify that response_code is available for routing decisions
        assert result["invocations"][0]["response_code"] == 42
        assert result["invocations"][0]["status"] == "Failed"

        # Verify that output is available for additional context
        assert "Partial success" in result["invocations"][0]["standard_output"]
        assert "Warning" in result["invocations"][0]["standard_error"]

        # This demonstrates that the operator provides all necessary information
        # for downstream tasks to make routing decisions based on exit codes,
        # which is the key use case for the enhanced mode feature.

    def test_multiple_exit_codes_for_routing(self, mock_hook):
        """
        Test retrieving multiple instances with different exit codes for routing.

        This demonstrates a common pattern where a command runs on multiple instances
        and downstream tasks need to route based on the exit codes from each instance.
        """
        operator = SsmGetCommandInvocationOperator(
            task_id="test_multi_instance_routing",
            command_id=self.command_id,
        )

        # Mock list_command_invocations response
        mock_invocations = [
            {"InstanceId": "i-success"},
            {"InstanceId": "i-partial"},
            {"InstanceId": "i-failed"},
        ]
        mock_hook.list_command_invocations.return_value = {"CommandInvocations": mock_invocations}

        # Mock different exit codes for routing scenarios
        mock_hook.get_command_invocation.side_effect = [
            {
                "Status": "Success",
                "ResponseCode": 0,  # Complete success
                "StandardOutputContent": "All items processed",
                "StandardErrorContent": "",
                "ExecutionStartDateTime": "2023-01-01T12:00:00Z",
                "ExecutionEndDateTime": "2023-01-01T12:00:05Z",
                "DocumentName": "AWS-RunShellScript",
                "Comment": "",
            },
            {
                "Status": "Failed",
                "ResponseCode": 2,  # Partial success - custom exit code
                "StandardOutputContent": "Some items processed",
                "StandardErrorContent": "Warning: partial completion",
                "ExecutionStartDateTime": "2023-01-01T12:00:00Z",
                "ExecutionEndDateTime": "2023-01-01T12:00:10Z",
                "DocumentName": "AWS-RunShellScript",
                "Comment": "",
            },
            {
                "Status": "Failed",
                "ResponseCode": 1,  # Complete failure
                "StandardOutputContent": "",
                "StandardErrorContent": "Error: operation failed",
                "ExecutionStartDateTime": "2023-01-01T12:00:00Z",
                "ExecutionEndDateTime": "2023-01-01T12:00:08Z",
                "DocumentName": "AWS-RunShellScript",
                "Comment": "",
            },
        ]

        result = operator.execute({})

        # Verify all exit codes are captured for routing logic
        assert len(result["invocations"]) == 3
        assert result["invocations"][0]["response_code"] == 0
        assert result["invocations"][1]["response_code"] == 2
        assert result["invocations"][2]["response_code"] == 1

        # This demonstrates that the operator can retrieve exit codes from multiple
        # instances, enabling complex routing logic based on the results from each instance.
