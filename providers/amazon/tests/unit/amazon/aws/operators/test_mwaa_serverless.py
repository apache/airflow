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
    MwaaServerlessDeleteWorkflowOperator,
    MwaaServerlessStartWorkflowRunOperator,
    MwaaServerlessStopWorkflowRunOperator,
    MwaaServerlessUpdateWorkflowOperator,
)
from airflow.providers.amazon.aws.triggers.mwaa_serverless import MwaaServerlessWorkflowRunCompletedTrigger
from airflow.providers.common.compat.sdk import TaskDeferred

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

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_client_token(self, mock_conn):
        op = MwaaServerlessStartWorkflowRunOperator(
            task_id="start_workflow",
            workflow_arn=WORKFLOW_ARN,
            client_token="my-token",
        )
        mock_client = mock.MagicMock()
        mock_client.start_workflow_run.return_value = {"RunId": RUN_ID, "Status": "STARTING"}
        mock_conn.return_value = mock_client

        op.execute({})

        mock_client.start_workflow_run.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN, ClientToken="my-token"
        )

    @mock.patch.object(AwsBaseHook, "get_waiter")
    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_wait_for_completion(self, mock_conn, mock_get_waiter):
        op = MwaaServerlessStartWorkflowRunOperator(
            task_id="start_workflow",
            workflow_arn=WORKFLOW_ARN,
            wait_for_completion=True,
            waiter_delay=5,
            waiter_max_attempts=10,
        )
        mock_client = mock.MagicMock()
        mock_client.start_workflow_run.return_value = {"RunId": RUN_ID, "Status": "STARTING"}
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_get_waiter.assert_called_once_with("workflow_run_complete")
        mock_get_waiter.return_value.wait.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN,
            RunId=RUN_ID,
            WaiterConfig={"Delay": 5, "MaxAttempts": 10},
        )
        assert result == RUN_ID

    @mock.patch.object(AwsBaseHook, "get_waiter")
    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_deferrable(self, mock_conn, mock_get_waiter):
        op = MwaaServerlessStartWorkflowRunOperator(
            task_id="start_workflow",
            workflow_arn=WORKFLOW_ARN,
            deferrable=True,
            waiter_delay=5,
            waiter_max_attempts=10,
            aws_conn_id="my_conn",
            region_name="eu-west-1",
        )
        mock_client = mock.MagicMock()
        mock_client.start_workflow_run.return_value = {"RunId": RUN_ID, "Status": "STARTING"}
        mock_conn.return_value = mock_client

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({})

        mock_get_waiter.assert_not_called()
        assert exc_info.value.method_name == "execute_complete"
        trigger = exc_info.value.trigger
        assert isinstance(trigger, MwaaServerlessWorkflowRunCompletedTrigger)
        assert trigger.waiter_args == {"WorkflowArn": WORKFLOW_ARN, "RunId": RUN_ID}
        assert trigger.waiter_delay == 5
        assert trigger.attempts == 10
        assert trigger.aws_conn_id == "my_conn"
        assert trigger.region_name == "eu-west-1"

    def test_execute_complete(self):
        assert self.operator.execute_complete({}, {"status": "success", "run_id": RUN_ID}) == RUN_ID

    def test_execute_complete_failure(self):
        with pytest.raises(RuntimeError, match="Error while waiting for MWAA Serverless workflow run"):
            self.operator.execute_complete({}, {"status": "error", "message": "failed", "run_id": RUN_ID})

    def test_template_fields(self):
        validate_template_fields(self.operator)


WORKFLOW_NAME = "test-workflow"
WORKFLOW_ARN = "arn:aws:mwaa-serverless:us-east-1:123456789012:workflow/test-workflow"
S3_LOCATION = {"Bucket": "test-bucket", "ObjectKey": "workflow.yaml"}
CODE = {"S3Location": {"Bucket": "test-bucket", "ObjectKey": "code/my_package.zip"}}
ROLE_ARN = "arn:aws:iam::123456789012:role/test-role"
CREATE_WORKFLOW_KWARGS = {
    "NetworkConfiguration": {
        "SubnetIds": ["subnet-0123456789abcdef0", "subnet-0fedcba9876543210"],
        "SecurityGroupIds": ["sg-0123456789abcdef0"],
    },
    "LoggingConfiguration": {"LogGroupName": "/aws/mwaa-serverless/test-workflow"},
    "EncryptionConfiguration": {"Type": "AWS_MANAGED_KEY"},
    "EngineVersion": 1,
    "TriggerMode": "manual_only",
    "ClientToken": "test-client-token",
}
UPDATE_WORKFLOW_KWARGS = {
    "NetworkConfiguration": {
        "SubnetIds": ["subnet-0123456789abcdef0", "subnet-0fedcba9876543210"],
        "SecurityGroupIds": ["sg-0123456789abcdef0"],
    },
    "LoggingConfiguration": {"LogGroupName": "/aws/mwaa-serverless/test-workflow"},
    "EngineVersion": 1,
    "TriggerMode": "manual_only",
}


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

    @mock.patch.object(AwsBaseHook, "account_id", new_callable=mock.PropertyMock)
    @mock.patch.object(AwsBaseHook, "conn_region_name", new_callable=mock.PropertyMock)
    @mock.patch.object(AwsBaseHook, "conn_partition", new_callable=mock.PropertyMock)
    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_skip_existing(self, mock_conn, mock_partition, mock_region, mock_account):
        mock_client = mock.MagicMock()
        mock_client.create_workflow.side_effect = ClientError(
            {
                "Error": {"Code": "ConflictException", "Message": "Already exists"},
                "ResourceId": "test-workflow-aBcDeFgHiJ",
                "ResourceType": "Workflow",
            },
            "CreateWorkflow",
        )
        mock_conn.return_value = mock_client
        mock_partition.return_value = "aws"
        mock_region.return_value = "us-east-1"
        mock_account.return_value = "123456789012"

        result = self.operator.execute({})
        assert result == "arn:aws:airflow-serverless:us-east-1:123456789012:workflow/test-workflow-aBcDeFgHiJ"

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

        with pytest.raises(ClientError, match="ConflictException"):
            op.execute({})

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_code_version_id(self, mock_conn):
        code = {"S3Location": {**CODE["S3Location"], "VersionId": "abc123"}}
        op = MwaaServerlessCreateWorkflowOperator(
            task_id="create_workflow",
            workflow_name=WORKFLOW_NAME,
            definition_s3_location=S3_LOCATION,
            code=code,
            role_arn=ROLE_ARN,
        )
        mock_client = mock.MagicMock()
        mock_client.create_workflow.return_value = {"WorkflowArn": WORKFLOW_ARN}
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.create_workflow.assert_called_once_with(
            Name=WORKFLOW_NAME, DefinitionS3Location=S3_LOCATION, Code=code, RoleArn=ROLE_ARN
        )
        assert result == WORKFLOW_ARN

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_create_workflow_kwargs(self, mock_conn):
        op = MwaaServerlessCreateWorkflowOperator(
            task_id="create_workflow",
            workflow_name=WORKFLOW_NAME,
            definition_s3_location=S3_LOCATION,
            role_arn=ROLE_ARN,
            create_workflow_kwargs=CREATE_WORKFLOW_KWARGS,
        )
        mock_client = mock.MagicMock()
        mock_client.create_workflow.return_value = {"WorkflowArn": WORKFLOW_ARN}
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.create_workflow.assert_called_once_with(
            Name=WORKFLOW_NAME,
            DefinitionS3Location=S3_LOCATION,
            RoleArn=ROLE_ARN,
            **CREATE_WORKFLOW_KWARGS,
        )
        assert result == WORKFLOW_ARN

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestMwaaServerlessUpdateWorkflowOperator:
    def setup_method(self):
        self.operator = MwaaServerlessUpdateWorkflowOperator(
            task_id="update_workflow",
            workflow_arn=WORKFLOW_ARN,
            definition_s3_location=S3_LOCATION,
            role_arn=ROLE_ARN,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.update_workflow.return_value = {
            "WorkflowArn": WORKFLOW_ARN,
            "WorkflowVersion": "abc123",
            "ModifiedAt": "2026-05-12T00:00:00Z",
        }
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.update_workflow.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN,
            DefinitionS3Location=S3_LOCATION,
            RoleArn=ROLE_ARN,
        )
        assert result == WORKFLOW_ARN

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_description(self, mock_conn):
        op = MwaaServerlessUpdateWorkflowOperator(
            task_id="update_workflow",
            workflow_arn=WORKFLOW_ARN,
            definition_s3_location=S3_LOCATION,
            role_arn=ROLE_ARN,
            description="Updated workflow",
        )
        mock_client = mock.MagicMock()
        mock_client.update_workflow.return_value = {
            "WorkflowArn": WORKFLOW_ARN,
            "WorkflowVersion": "def456",
            "ModifiedAt": "2026-05-12T00:00:00Z",
        }
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.update_workflow.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN,
            DefinitionS3Location=S3_LOCATION,
            RoleArn=ROLE_ARN,
            Description="Updated workflow",
        )
        assert result == WORKFLOW_ARN

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_not_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.update_workflow.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "UpdateWorkflow"
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError):
            self.operator.execute({})

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_code(self, mock_conn):
        op = MwaaServerlessUpdateWorkflowOperator(
            task_id="update_workflow",
            workflow_arn=WORKFLOW_ARN,
            definition_s3_location=S3_LOCATION,
            code=CODE,
            role_arn=ROLE_ARN,
        )
        mock_client = mock.MagicMock()
        mock_client.update_workflow.return_value = {
            "WorkflowArn": WORKFLOW_ARN,
            "WorkflowVersion": "abc123",
            "ModifiedAt": "2026-05-12T00:00:00Z",
        }
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.update_workflow.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN,
            DefinitionS3Location=S3_LOCATION,
            Code=CODE,
            RoleArn=ROLE_ARN,
        )
        assert result == WORKFLOW_ARN

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_update_workflow_kwargs(self, mock_conn):
        op = MwaaServerlessUpdateWorkflowOperator(
            task_id="update_workflow",
            workflow_arn=WORKFLOW_ARN,
            definition_s3_location=S3_LOCATION,
            role_arn=ROLE_ARN,
            update_workflow_kwargs=UPDATE_WORKFLOW_KWARGS,
        )
        mock_client = mock.MagicMock()
        mock_client.update_workflow.return_value = {
            "WorkflowArn": WORKFLOW_ARN,
            "WorkflowVersion": "abc123",
            "ModifiedAt": "2026-05-12T00:00:00Z",
        }
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.update_workflow.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN,
            DefinitionS3Location=S3_LOCATION,
            RoleArn=ROLE_ARN,
            **UPDATE_WORKFLOW_KWARGS,
        )
        assert result == WORKFLOW_ARN

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestMwaaServerlessDeleteWorkflowOperator:
    def setup_method(self):
        self.operator = MwaaServerlessDeleteWorkflowOperator(
            task_id="delete_workflow",
            workflow_arn=WORKFLOW_ARN,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.delete_workflow.return_value = {"WorkflowArn": WORKFLOW_ARN}
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.delete_workflow.assert_called_once_with(WorkflowArn=WORKFLOW_ARN)
        assert result is None

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_version(self, mock_conn):
        op = MwaaServerlessDeleteWorkflowOperator(
            task_id="delete_workflow",
            workflow_arn=WORKFLOW_ARN,
            workflow_version="abc123def456abc123def456abc123de",
        )
        mock_client = mock.MagicMock()
        mock_client.delete_workflow.return_value = {
            "WorkflowArn": WORKFLOW_ARN,
            "WorkflowVersion": "abc123def456abc123def456abc123de",
        }
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.delete_workflow.assert_called_once_with(
            WorkflowArn=WORKFLOW_ARN,
            WorkflowVersion="abc123def456abc123def456abc123de",
        )
        assert result is None

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_not_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.delete_workflow.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "DeleteWorkflow"
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError, match="ResourceNotFoundException"):
            self.operator.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestMwaaServerlessStopWorkflowRunOperator:
    def setup_method(self):
        self.operator = MwaaServerlessStopWorkflowRunOperator(
            task_id="stop_run",
            workflow_arn=WORKFLOW_ARN,
            run_id=RUN_ID,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.stop_workflow_run.return_value = {
            "WorkflowArn": WORKFLOW_ARN,
            "RunId": RUN_ID,
            "Status": "STOPPING",
        }
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.stop_workflow_run.assert_called_once_with(WorkflowArn=WORKFLOW_ARN, RunId=RUN_ID)
        assert result == "STOPPING"

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_not_found(self, mock_conn):
        from botocore.exceptions import ClientError

        mock_client = mock.MagicMock()
        mock_client.stop_workflow_run.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}}, "StopWorkflowRun"
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError):
            self.operator.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)
