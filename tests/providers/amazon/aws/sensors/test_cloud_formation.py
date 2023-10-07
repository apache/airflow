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

from unittest.mock import patch

import boto3
import pytest
from moto import mock_cloudformation

from airflow.providers.amazon.aws.sensors.cloud_formation import (
    CloudFormationCreateStackSensor,
    CloudFormationDeleteStackSensor,
)


@pytest.fixture
def mocked_hook_client():
    with patch("airflow.providers.amazon.aws.hooks.cloud_formation.CloudFormationHook.conn") as m:
        yield m


class TestCloudFormationCreateStackSensor:
    @mock_cloudformation
    def setup_method(self, method):
        self.client = boto3.client("cloudformation", region_name="us-east-1")

    @mock_cloudformation
    def test_poke(self):
        self.client.create_stack(StackName="foobar", TemplateBody='{"Resources": {}}')
        op = CloudFormationCreateStackSensor(task_id="task", stack_name="foobar")
        assert op.poke({})

    def test_poke_false(self, mocked_hook_client):
        mocked_hook_client.describe_stacks.return_value = {"Stacks": [{"StackStatus": "CREATE_IN_PROGRESS"}]}
        op = CloudFormationCreateStackSensor(task_id="task", stack_name="foo")
        assert not op.poke({})

    def test_poke_stack_in_unsuccessful_state(self, mocked_hook_client):
        mocked_hook_client.describe_stacks.return_value = {"Stacks": [{"StackStatus": "bar"}]}
        op = CloudFormationCreateStackSensor(task_id="task", stack_name="foo")
        with pytest.raises(ValueError, match="Stack foo in bad state: bar"):
            op.poke({})


class TestCloudFormationDeleteStackSensor:
    @mock_cloudformation
    def setup_method(self, method):
        self.client = boto3.client("cloudformation", region_name="us-east-1")

    @mock_cloudformation
    def test_poke(self):
        stack_name = "foobar"
        self.client.create_stack(StackName=stack_name, TemplateBody='{"Resources": {}}')
        self.client.delete_stack(StackName=stack_name)
        op = CloudFormationDeleteStackSensor(task_id="task", stack_name=stack_name)
        assert op.poke({})

    def test_poke_false(self, mocked_hook_client):
        mocked_hook_client.describe_stacks.return_value = {"Stacks": [{"StackStatus": "DELETE_IN_PROGRESS"}]}
        op = CloudFormationDeleteStackSensor(task_id="task", stack_name="foo")
        assert not op.poke({})

    def test_poke_stack_in_unsuccessful_state(self, mocked_hook_client):
        mocked_hook_client.describe_stacks.return_value = {"Stacks": [{"StackStatus": "bar"}]}
        op = CloudFormationDeleteStackSensor(task_id="task", stack_name="foo")
        with pytest.raises(ValueError, match="Stack foo in bad state: bar"):
            op.poke({})

    @mock_cloudformation
    def test_poke_stack_does_not_exist(self):
        op = CloudFormationDeleteStackSensor(task_id="task", stack_name="foo")
        assert op.poke({})
