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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionGetExecutionOutputOperator,
    StepFunctionStartExecutionOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

EXECUTION_ARN = (
    "arn:aws:states:us-east-1:123456789012:execution:"
    "pseudo-state-machine:020f5b16-b1a1-4149-946f-92dd32d97934"
)
AWS_CONN_ID = "aws_non_default"
REGION_NAME = "us-west-2"
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:000000000000:stateMachine:pseudo-state-machine"
NAME = "NAME"
INPUT = "{}"


@pytest.fixture
def mocked_context():
    return mock.MagicMock(name="FakeContext")


class TestStepFunctionGetExecutionOutputOperator:
    TASK_ID = "step_function_get_execution_output"

    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        with mock.patch(
            "airflow.providers.amazon.aws.links.step_function.StateMachineExecutionsDetailsLink.persist"
        ) as executions_details_link:
            self.mocked_executions_details_link = executions_details_link
            yield

    def test_init(self):
        op = StepFunctionGetExecutionOutputOperator(
            task_id=self.TASK_ID,
            execution_arn=EXECUTION_ARN,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
        )
        assert op.execution_arn == EXECUTION_ARN
        assert op.hook.aws_conn_id == AWS_CONN_ID
        assert op.hook._region_name == REGION_NAME
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = StepFunctionGetExecutionOutputOperator(task_id=self.TASK_ID, execution_arn=EXECUTION_ARN)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(StepFunctionGetExecutionOutputOperator, "hook")
    @pytest.mark.parametrize(
        ("response", "expected_output"),
        [
            pytest.param({"output": '{"foo": "bar"}'}, {"foo": "bar"}, id="output"),
            pytest.param({"error": '{"spam": "egg"}'}, {"spam": "egg"}, id="error"),
            pytest.param({"other": '{"baz": "qux"}'}, None, id="other"),
        ],
    )
    def test_execute(self, mocked_hook, mocked_context, response, expected_output):
        mocked_hook.describe_execution.return_value = response
        op = StepFunctionGetExecutionOutputOperator(
            task_id=self.TASK_ID,
            execution_arn=EXECUTION_ARN,
            aws_conn_id=None,
        )
        assert op.execute(mocked_context) == expected_output
        mocked_hook.describe_execution.assert_called_once_with(EXECUTION_ARN)
        self.mocked_executions_details_link.assert_called_once_with(
            aws_partition=mock.ANY,
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            execution_arn=EXECUTION_ARN,
        )

    def test_template_fields(self):
        operator = StepFunctionGetExecutionOutputOperator(
            task_id=self.TASK_ID,
            execution_arn=EXECUTION_ARN,
            aws_conn_id=None,
        )
        validate_template_fields(operator)


class TestStepFunctionStartExecutionOperator:
    TASK_ID = "step_function_start_execution_task"

    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        with (
            mock.patch(
                "airflow.providers.amazon.aws.links.step_function.StateMachineExecutionsDetailsLink.persist"
            ) as executions_details_link,
            mock.patch(
                "airflow.providers.amazon.aws.links.step_function.StateMachineDetailsLink.persist"
            ) as details_link,
        ):
            self.mocked_executions_details_link = executions_details_link
            self.mocked_details_link = details_link
            yield

    def test_init(self):
        op = StepFunctionStartExecutionOperator(
            task_id=self.TASK_ID,
            state_machine_arn=STATE_MACHINE_ARN,
            name=NAME,
            state_machine_input=INPUT,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.state_machine_arn == STATE_MACHINE_ARN
        assert op.state_machine_arn == STATE_MACHINE_ARN
        assert op.name == NAME
        assert op.input == INPUT
        assert op.hook.aws_conn_id == AWS_CONN_ID
        assert op.hook._region_name == REGION_NAME
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = StepFunctionStartExecutionOperator(task_id=self.TASK_ID, state_machine_arn=STATE_MACHINE_ARN)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(StepFunctionStartExecutionOperator, "hook")
    def test_execute(self, mocked_hook, mocked_context):
        hook_response = (
            "arn:aws:states:us-east-1:123456789012:execution:"
            "pseudo-state-machine:020f5b16-b1a1-4149-946f-92dd32d97934"
        )
        mocked_hook.start_execution.return_value = hook_response
        op = StepFunctionStartExecutionOperator(
            task_id=self.TASK_ID,
            state_machine_arn=STATE_MACHINE_ARN,
            name=NAME,
            state_machine_input=INPUT,
            aws_conn_id=None,
        )
        assert op.execute(mocked_context) == hook_response
        mocked_hook.start_execution.assert_called_once_with(STATE_MACHINE_ARN, NAME, INPUT, False)
        self.mocked_details_link.assert_called_once_with(
            aws_partition=mock.ANY,
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            state_machine_arn=STATE_MACHINE_ARN,
        )
        self.mocked_executions_details_link.assert_called_once_with(
            aws_partition=mock.ANY,
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            execution_arn=EXECUTION_ARN,
        )

    @mock.patch.object(StepFunctionStartExecutionOperator, "hook")
    def test_step_function_start_execution_deferrable(self, mocked_hook):
        mocked_hook.start_execution.return_value = "test-execution-arn"
        operator = StepFunctionStartExecutionOperator(
            task_id=self.TASK_ID,
            state_machine_arn=STATE_MACHINE_ARN,
            name=NAME,
            state_machine_input=INPUT,
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            operator.execute(None)
        mocked_hook.start_execution.assert_called_once_with(STATE_MACHINE_ARN, NAME, INPUT, False)

    @mock.patch.object(StepFunctionStartExecutionOperator, "hook")
    @pytest.mark.parametrize("execution_arn", [pytest.param(None, id="none"), pytest.param("", id="empty")])
    def test_step_function_no_execution_arn_returns(self, mocked_hook, execution_arn):
        mocked_hook.start_execution.return_value = execution_arn
        op = StepFunctionStartExecutionOperator(
            task_id=self.TASK_ID, state_machine_arn=STATE_MACHINE_ARN, aws_conn_id=None
        )
        with pytest.raises(AirflowException, match="Failed to start State Machine execution"):
            op.execute({})

    @mock.patch.object(StepFunctionStartExecutionOperator, "hook")
    def test_start_redrive_execution(self, mocked_hook, mocked_context):
        hook_response = (
            "arn:aws:states:us-east-1:123456789012:execution:"
            "pseudo-state-machine:020f5b16-b1a1-4149-946f-92dd32d97934"
        )
        mocked_hook.start_execution.return_value = hook_response
        op = StepFunctionStartExecutionOperator(
            task_id=self.TASK_ID,
            state_machine_arn=STATE_MACHINE_ARN,
            name=NAME,
            is_redrive_execution=True,
            state_machine_input=None,
            aws_conn_id=None,
        )
        assert op.execute(mocked_context) == hook_response
        mocked_hook.start_execution.assert_called_once_with(STATE_MACHINE_ARN, NAME, None, True)
        self.mocked_details_link.assert_called_once_with(
            aws_partition=mock.ANY,
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            state_machine_arn=STATE_MACHINE_ARN,
        )
        self.mocked_executions_details_link.assert_called_once_with(
            aws_partition=mock.ANY,
            context=mock.ANY,
            operator=mock.ANY,
            region_name=mock.ANY,
            execution_arn=EXECUTION_ARN,
        )

    def test_template_fields(self):
        operator = StepFunctionStartExecutionOperator(
            task_id=self.TASK_ID,
            state_machine_arn=STATE_MACHINE_ARN,
            name=NAME,
            is_redrive_execution=True,
            state_machine_input=None,
            aws_conn_id=None,
        )
        validate_template_fields(operator)
