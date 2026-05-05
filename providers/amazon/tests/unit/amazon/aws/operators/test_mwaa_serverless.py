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
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.mwaa_serverless import (
    MwaaServerlessCreateWorkflowOperator,
    MwaaServerlessStartWorkflowRunOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

WORKFLOW_ARN = "arn:aws:mwaa-serverless:us-east-1:123456789012:workflow/test-workflow"
RUN_ID = "run-abc123"


class TestMwaaServerlessStartWorkflowRunOperator:
    def setup_method(self):
        self.operator = MwaaServerlessStartWorkflowRunOperator(
            task_id="start_workflow",
            workflow_arn=WORKFLOW_ARN,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.start_workflow_run.return_value = {
            "RunId": RUN_ID,
            "Status": "STARTING",
        }
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.start_workflow_run.assert_called_once_with(WorkflowArn=WORKFLOW_ARN)
        assert result == RUN_ID

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_overrides(self, mock_conn):
        op = MwaaServerlessStartWorkflowRunOperator(
            task_id="start_workflow",
            workflow_arn=WORKFLOW_ARN,
            override_parameters={"bucket": "my-bucket"},
            workflow_version="2",
        )
        mock_client = mock.MagicMock()
        mock_client.start_workflow_run.return_value = {
            "RunId": RUN_ID,
            "Status": "STARTING",
        }
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.start_workflow_run.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN,
            OverrideParameters={"bucket": "my-bucket"},
            WorkflowVersion="2",
        )
        assert result == RUN_ID

    def test_template_fields(self):
        validate_template_fields(self.operator)


WORKFLOW_NAME = "test-workflow"
WORKFLOW_ARN = "arn:aws:mwaa-serverless:us-east-1:123456789012:workflow/test-workflow"
S3_LOCATION = {"Bucket": "test-bucket", "ObjectKey": "workflow.yaml"}
ROLE_ARN = "arn:aws:iam::123456789012:role/test-role"


class TestMwaaServerlessCreateWorkflowOperator:
    def setup_method(self):
        self.operator = MwaaServerlessCreateWorkflowOperator(
            task_id="create_workflow",
            workflow_name=WORKFLOW_NAME,
            definition_s3_location=S3_LOCATION,
            role_arn=ROLE_ARN,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.create_workflow.return_value = {"WorkflowArn": WORKFLOW_ARN}
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.create_workflow.assert_called_once_with(
            Name=WORKFLOW_NAME,
            DefinitionS3Location=S3_LOCATION,
            RoleArn=ROLE_ARN,
        )
        assert result == WORKFLOW_ARN

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_skip_existing(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.create_workflow.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateWorkflow",
        )
        mock_client.get_workflow.return_value = {"WorkflowArn": WORKFLOW_ARN}
        mock_conn.return_value = mock_client

        result = self.operator.execute({})
        assert result == WORKFLOW_ARN

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_fail_on_conflict(self, mock_conn):
        op = MwaaServerlessCreateWorkflowOperator(
            task_id="create_workflow",
            workflow_name=WORKFLOW_NAME,
            definition_s3_location=S3_LOCATION,
            role_arn=ROLE_ARN,
            if_exists="fail",
        )
        mock_client = mock.MagicMock()
        mock_client.create_workflow.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateWorkflow",
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError):
            op.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)
