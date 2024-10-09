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
from unittest.mock import MagicMock

import pytest

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.cloud_formation import (
    CloudFormationCreateStackOperator,
    CloudFormationDeleteStackOperator,
)
from airflow.utils import timezone

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

DEFAULT_DATE = timezone.datetime(2019, 1, 1)
DEFAULT_ARGS = {"owner": "airflow", "start_date": DEFAULT_DATE}


@pytest.fixture
def mocked_hook_client():
    with mock.patch("airflow.providers.amazon.aws.hooks.cloud_formation.CloudFormationHook.conn") as m:
        yield m


class TestCloudFormationCreateStackOperator:
    def test_init(self):
        op = CloudFormationCreateStackOperator(
            task_id="cf_create_stack_init",
            stack_name="fake-stack",
            cloudformation_parameters={},
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="eu-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.client_type == "cloudformation"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = CloudFormationCreateStackOperator(
            task_id="cf_create_stack_init",
            stack_name="fake-stack",
            cloudformation_parameters={},
        )
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    def test_create_stack(self, mocked_hook_client):
        stack_name = "myStack"
        timeout = 15
        template_body = "My stack body"

        operator = CloudFormationCreateStackOperator(
            task_id="test_task",
            stack_name=stack_name,
            cloudformation_parameters={"TimeoutInMinutes": timeout, "TemplateBody": template_body},
            dag=DAG("test_dag_id", schedule=None, default_args=DEFAULT_ARGS),
        )

        operator.execute(MagicMock())

        mocked_hook_client.create_stack.assert_any_call(
            StackName=stack_name, TemplateBody=template_body, TimeoutInMinutes=timeout
        )

    def test_template_fields(self):
        op = CloudFormationCreateStackOperator(
            task_id="cf_create_stack_init",
            stack_name="fake-stack",
            cloudformation_parameters={},
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="eu-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        validate_template_fields(op)


class TestCloudFormationDeleteStackOperator:
    def test_init(self):
        op = CloudFormationDeleteStackOperator(
            task_id="cf_delete_stack_init",
            stack_name="fake-stack",
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="us-east-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.client_type == "cloudformation"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "us-east-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = CloudFormationDeleteStackOperator(task_id="cf_delete_stack_init", stack_name="fake-stack")
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    def test_delete_stack(self, mocked_hook_client):
        stack_name = "myStackToBeDeleted"

        operator = CloudFormationDeleteStackOperator(
            task_id="test_task",
            stack_name=stack_name,
            dag=DAG("test_dag_id", schedule=None, default_args=DEFAULT_ARGS),
        )

        operator.execute(MagicMock())

        mocked_hook_client.delete_stack.assert_any_call(StackName=stack_name)

    def test_template_fields(self):
        op = CloudFormationDeleteStackOperator(
            task_id="cf_delete_stack_init",
            stack_name="fake-stack",
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="us-east-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )

        validate_template_fields(op)
