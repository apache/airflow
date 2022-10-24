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

import unittest

from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook

try:
    from moto import mock_stepfunctions
except ImportError:
    mock_stepfunctions = None


@unittest.skipIf(mock_stepfunctions is None, "moto package not present")
class TestStepFunctionHook(unittest.TestCase):
    @mock_stepfunctions
    def test_get_conn_returns_a_boto3_connection(self):
        hook = StepFunctionHook(aws_conn_id="aws_default")
        assert "stepfunctions" == hook.get_conn().meta.service_model.service_name

    @mock_stepfunctions
    def test_start_execution(self):
        hook = StepFunctionHook(aws_conn_id="aws_default", region_name="us-east-1")
        state_machine = hook.get_conn().create_state_machine(
            name="pseudo-state-machine", definition="{}", roleArn="arn:aws:iam::000000000000:role/Role"
        )

        state_machine_arn = state_machine.get("stateMachineArn")

        execution_arn = hook.start_execution(
            state_machine_arn=state_machine_arn, name=None, state_machine_input={}
        )

        assert execution_arn is not None

    @mock_stepfunctions
    def test_describe_execution(self):
        hook = StepFunctionHook(aws_conn_id="aws_default", region_name="us-east-1")
        state_machine = hook.get_conn().create_state_machine(
            name="pseudo-state-machine", definition="{}", roleArn="arn:aws:iam::000000000000:role/Role"
        )

        state_machine_arn = state_machine.get("stateMachineArn")

        execution_arn = hook.start_execution(
            state_machine_arn=state_machine_arn, name=None, state_machine_input={}
        )
        response = hook.describe_execution(execution_arn)

        assert "input" in response
