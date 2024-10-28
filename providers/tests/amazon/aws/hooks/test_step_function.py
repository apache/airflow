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

from datetime import datetime
from unittest import mock

import pytest
from moto import mock_aws

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook


@mock_aws
class TestStepFunctionHook:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = StepFunctionHook(aws_conn_id="aws_default")
        assert "stepfunctions" == hook.get_conn().meta.service_model.service_name

    def test_start_execution(self):
        hook = StepFunctionHook(aws_conn_id="aws_default", region_name="us-east-1")
        state_machine = hook.get_conn().create_state_machine(
            name="pseudo-state-machine",
            definition="{}",
            roleArn="arn:aws:iam::000000000000:role/Role",
        )

        state_machine_arn = state_machine.get("stateMachineArn")

        execution_arn = hook.start_execution(
            state_machine_arn=state_machine_arn, name=None, state_machine_input={}
        )

        assert execution_arn is not None

    @mock.patch.object(StepFunctionHook, "conn")
    def test_redrive_execution(self, mock_conn):
        mock_conn.redrive_execution.return_value = {"redriveDate": datetime(2024, 1, 1)}
        StepFunctionHook().start_execution(
            state_machine_arn="arn:aws:states:us-east-1:123456789012:stateMachine:test-state-machine",
            name="random-123",
            is_redrive_execution=True,
        )

        mock_conn.redrive_execution.assert_called_once_with(
            executionArn="arn:aws:states:us-east-1:123456789012:execution:test-state-machine:random-123"
        )

    @mock.patch.object(StepFunctionHook, "conn")
    def test_redrive_execution_without_name_should_fail(self, mock_conn):
        mock_conn.redrive_execution.return_value = {"redriveDate": datetime(2024, 1, 1)}

        with pytest.raises(
            AirflowFailException,
            match="Execution name is required to start RedriveExecution",
        ):
            StepFunctionHook().start_execution(
                state_machine_arn="arn:aws:states:us-east-1:123456789012:stateMachine:test-state-machine",
                is_redrive_execution=True,
            )

    def test_describe_execution(self):
        hook = StepFunctionHook(aws_conn_id="aws_default", region_name="us-east-1")
        state_machine = hook.get_conn().create_state_machine(
            name="pseudo-state-machine",
            definition="{}",
            roleArn="arn:aws:iam::000000000000:role/Role",
        )

        state_machine_arn = state_machine.get("stateMachineArn")

        execution_arn = hook.start_execution(
            state_machine_arn=state_machine_arn, name=None, state_machine_input={}
        )
        response = hook.describe_execution(execution_arn)

        assert "input" in response
