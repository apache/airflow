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

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.cloud_formation import (
    CloudFormationCreateStackOperator,
    CloudFormationDeleteStackOperator,
)
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2019, 1, 1)
DEFAULT_ARGS = {"owner": "airflow", "start_date": DEFAULT_DATE}


class TestCloudFormationCreateStackOperator:
    def setup_method(self):
        # Mock out the cloudformation_client (moto fails with an exception).
        self.cloudformation_client_mock = MagicMock()

        # Mock out the emr_client creator
        cloudformation_session_mock = MagicMock()
        cloudformation_session_mock.client.return_value = self.cloudformation_client_mock
        self.boto3_session_mock = MagicMock(return_value=cloudformation_session_mock)

        self.mock_context = MagicMock()

    def test_create_stack(self):
        stack_name = "myStack"
        timeout = 15
        template_body = "My stack body"

        operator = CloudFormationCreateStackOperator(
            task_id="test_task",
            stack_name=stack_name,
            cloudformation_parameters={"TimeoutInMinutes": timeout, "TemplateBody": template_body},
            dag=DAG("test_dag_id", default_args=DEFAULT_ARGS),
        )

        with mock.patch("boto3.session.Session", self.boto3_session_mock):
            operator.execute(self.mock_context)

        self.cloudformation_client_mock.create_stack.assert_any_call(
            StackName=stack_name, TemplateBody=template_body, TimeoutInMinutes=timeout
        )


class TestCloudFormationDeleteStackOperator:
    def setup_method(self):
        # Mock out the cloudformation_client (moto fails with an exception).
        self.cloudformation_client_mock = MagicMock()

        # Mock out the emr_client creator
        cloudformation_session_mock = MagicMock()
        cloudformation_session_mock.client.return_value = self.cloudformation_client_mock
        self.boto3_session_mock = MagicMock(return_value=cloudformation_session_mock)

        self.mock_context = MagicMock()

    def test_delete_stack(self):
        stack_name = "myStackToBeDeleted"

        operator = CloudFormationDeleteStackOperator(
            task_id="test_task",
            stack_name=stack_name,
            dag=DAG("test_dag_id", default_args=DEFAULT_ARGS),
        )

        with mock.patch("boto3.session.Session", self.boto3_session_mock):
            operator.execute(self.mock_context)

        self.cloudformation_client_mock.delete_stack.assert_any_call(StackName=stack_name)
