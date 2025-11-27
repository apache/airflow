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

import json
import logging
from typing import TYPE_CHECKING
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook

if TYPE_CHECKING:
    from unittest.mock import MagicMock


class TestGlueJobHook:
    def setup_method(self):
        self.some_aws_region = "us-west-2"

    @mock_aws
    @pytest.mark.parametrize("role_path", ["/", "/custom-path/"])
    def test_get_iam_execution_role(self, role_path):
        expected_role = "my_test_role"
        boto3.client("iam").create_role(
            Path=role_path,
            RoleName=expected_role,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    },
                }
            ),
        )

        hook = GlueJobHook(
            aws_conn_id=None,
            job_name="aws_test_glue_job",
            s3_bucket="some_bucket",
            iam_role_name=expected_role,
        )
        iam_role = hook.get_iam_execution_role()
        assert iam_role is not None
        assert "Role" in iam_role
        assert "Arn" in iam_role["Role"]
        assert iam_role["Role"]["Arn"] == f"arn:aws:iam::123456789012:role{role_path}{expected_role}"

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "conn")
    def test_init_iam_role_value_error(self, mock_conn, mock_get_iam_execution_role):
        mock_get_iam_execution_role.return_value = mock.MagicMock(
            Role={"RoleName": "my_test_role_name", "RoleArn": "my_test_role"}
        )

        with pytest.raises(ValueError, match="Cannot set iam_role_arn and iam_role_name simultaneously"):
            GlueJobHook(
                job_name="aws_test_glue_job",
                desc="This is test case job from Airflow",
                s3_bucket="some-bucket",
                iam_role_name="my_test_role_name",
                iam_role_arn="my_test_role",
            )

    @mock.patch.object(AwsBaseHook, "conn")
    def test_has_job_exists(self, mock_conn):
        job_name = "aws_test_glue_job"
        mock_conn.get_job.return_value = {"Job": {"Name": job_name}}

        hook = GlueJobHook(aws_conn_id=None, job_name=job_name, s3_bucket="some_bucket")
        result = hook.has_job(job_name)
        assert result is True
        mock_conn.get_job.assert_called_once_with(JobName=hook.job_name)

    @mock.patch.object(AwsBaseHook, "conn")
    def test_has_job_job_doesnt_exists(self, mock_conn):
        class JobNotFoundException(Exception):
            pass

        mock_conn.exceptions.EntityNotFoundException = JobNotFoundException
        mock_conn.get_job.side_effect = JobNotFoundException()

        job_name = "aws_test_glue_job"
        hook = GlueJobHook(aws_conn_id=None, job_name=job_name, s3_bucket="some_bucket")
        result = hook.has_job(job_name)
        assert result is False
        mock_conn.get_job.assert_called_once_with(JobName=job_name)

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(AwsBaseHook, "conn")
    def test_role_arn_has_job_exists(self, mock_conn, mock_get_iam_execution_role):
        """
        Calls 'create_or_update_glue_job' with no existing job.
        Should create a new job.
        """

        class JobNotFoundException(Exception):
            pass

        expected_job_name = "aws_test_glue_job"
        job_description = "This is test case job from Airflow"
        role_name = "my_test_role"
        role_name_arn = "test_role"
        some_s3_bucket = "bucket"

        mock_conn.exceptions.EntityNotFoundException = JobNotFoundException
        mock_conn.get_job.side_effect = JobNotFoundException()
        mock_get_iam_execution_role.return_value = {"Role": {"RoleName": role_name, "Arn": role_name_arn}}

        hook = GlueJobHook(
            s3_bucket=some_s3_bucket,
            job_name=expected_job_name,
            desc=job_description,
            concurrent_run_limit=2,
            retry_limit=3,
            num_of_dpus=5,
            iam_role_arn=role_name_arn,
            create_job_kwargs={"Command": {}},
            region_name=self.some_aws_region,
            update_config=True,
        )

        result = hook.create_or_update_glue_job()

        mock_conn.get_job.assert_called_once_with(JobName=expected_job_name)
        mock_conn.create_job.assert_called_once_with(
            Command={},
            Description=job_description,
            ExecutionProperty={"MaxConcurrentRuns": 2},
            LogUri=f"s3://{some_s3_bucket}/logs/glue-logs/{expected_job_name}",
            MaxCapacity=5,
            MaxRetries=3,
            Name=expected_job_name,
            Role=role_name_arn,
        )
        mock_conn.update_job.assert_not_called()
        assert result == expected_job_name

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "conn")
    def test_create_or_update_glue_job_create_new_job(self, mock_conn, mock_get_iam_execution_role):
        """
        Calls 'create_or_update_glue_job' with no existing job.
        Should create a new job.
        """

        class JobNotFoundException(Exception):
            pass

        expected_job_name = "aws_test_glue_job"
        job_description = "This is test case job from Airflow"
        role_name = "my_test_role"
        role_name_arn = "test_role"
        some_s3_bucket = "bucket"

        mock_conn.exceptions.EntityNotFoundException = JobNotFoundException
        mock_conn.get_job.side_effect = JobNotFoundException()
        mock_get_iam_execution_role.return_value = {"Role": {"RoleName": role_name, "Arn": role_name_arn}}

        hook = GlueJobHook(
            s3_bucket=some_s3_bucket,
            job_name=expected_job_name,
            desc=job_description,
            concurrent_run_limit=2,
            retry_limit=3,
            num_of_dpus=5,
            iam_role_name=role_name,
            create_job_kwargs={"Command": {}},
            region_name=self.some_aws_region,
            update_config=True,
        )

        result = hook.create_or_update_glue_job()

        mock_conn.get_job.assert_called_once_with(JobName=expected_job_name)
        mock_conn.create_job.assert_called_once_with(
            Command={},
            Description=job_description,
            ExecutionProperty={"MaxConcurrentRuns": 2},
            LogUri=f"s3://{some_s3_bucket}/logs/glue-logs/{expected_job_name}",
            MaxCapacity=5,
            MaxRetries=3,
            Name=expected_job_name,
            Role=role_name_arn,
        )
        mock_conn.update_job.assert_not_called()
        assert result == expected_job_name

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "conn")
    def test_create_or_update_glue_job_create_new_job_without_s3_bucket(
        self, mock_conn, mock_get_iam_execution_role
    ):
        """
        Calls 'create_or_update_glue_job' with no existing job.
        Should create a new job.
        """

        class JobNotFoundException(Exception):
            pass

        expected_job_name = "aws_test_glue_job"
        job_description = "This is test case job from Airflow"
        role_name = "my_test_role"
        role_name_arn = "test_role"

        mock_conn.exceptions.EntityNotFoundException = JobNotFoundException
        mock_conn.get_job.side_effect = JobNotFoundException()
        mock_get_iam_execution_role.return_value = {"Role": {"RoleName": role_name, "Arn": role_name_arn}}

        hook = GlueJobHook(
            job_name=expected_job_name,
            desc=job_description,
            concurrent_run_limit=2,
            retry_limit=3,
            num_of_dpus=5,
            iam_role_name=role_name,
            create_job_kwargs={"Command": {}},
            region_name=self.some_aws_region,
            update_config=True,
        )

        result = hook.create_or_update_glue_job()

        mock_conn.get_job.assert_called_once_with(JobName=expected_job_name)
        mock_conn.create_job.assert_called_once_with(
            Command={},
            Description=job_description,
            ExecutionProperty={"MaxConcurrentRuns": 2},
            MaxCapacity=5,
            MaxRetries=3,
            Name=expected_job_name,
            Role=role_name_arn,
        )
        mock_conn.update_job.assert_not_called()
        assert result == expected_job_name

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "conn")
    def test_create_or_update_glue_job_update_existing_job(self, mock_conn, mock_get_iam_execution_role):
        """
        Calls 'create_or_update_glue_job' with a existing job.
        Should update existing job configurations.
        """
        job_name = "aws_test_glue_job"
        job_description = "This is test case job from Airflow"
        role_name = "my_test_role"
        role_name_arn = "test_role"
        some_script = "s3://glue-examples/glue-scripts/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"

        mock_conn.get_job.return_value = {
            "Job": {
                "Name": job_name,
                "Description": "Old description of job",
                "Role": role_name_arn,
            }
        }
        mock_get_iam_execution_role.return_value = {"Role": {"RoleName": role_name, "Arn": "test_role"}}

        hook = GlueJobHook(
            job_name=job_name,
            desc=job_description,
            script_location=some_script,
            iam_role_name=role_name,
            s3_bucket=some_s3_bucket,
            region_name=self.some_aws_region,
            update_config=True,
        )

        result = hook.create_or_update_glue_job()

        assert mock_conn.get_job.call_count == 2
        mock_conn.update_job.assert_called_once_with(
            JobName=job_name,
            JobUpdate={
                "Description": job_description,
                "LogUri": f"s3://{some_s3_bucket}/logs/glue-logs/{job_name}",
                "Role": role_name_arn,
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "Command": {"Name": "glueetl", "ScriptLocation": some_script},
                "MaxRetries": 0,
                "MaxCapacity": 10,
            },
        )
        assert result == job_name

    @mock_aws
    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    def test_create_or_update_glue_job_worker_type(self, mock_get_iam_execution_role):
        mock_get_iam_execution_role.return_value = {"Role": {"RoleName": "my_test_role", "Arn": "test_role"}}
        some_script = "s3:/glue-examples/glue-scripts/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"
        expected_job_name = "aws_test_glue_job_worker_type"

        glue_job = GlueJobHook(
            job_name=expected_job_name,
            desc="This is test case job from Airflow",
            script_location=some_script,
            iam_role_name="my_test_role",
            s3_bucket=some_s3_bucket,
            region_name=self.some_aws_region,
            create_job_kwargs={"WorkerType": "G.2X", "NumberOfWorkers": 60},
            update_config=True,
        )

        result = glue_job.create_or_update_glue_job()

        assert result == expected_job_name

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "conn")
    def test_init_worker_type_value_error(self, mock_conn, mock_get_iam_execution_role):
        mock_get_iam_execution_role.return_value = mock.MagicMock(Role={"RoleName": "my_test_role"})
        some_script = "s3:/glue-examples/glue-scripts/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"

        with pytest.raises(ValueError, match="Cannot specify num_of_dpus with custom WorkerType"):
            GlueJobHook(
                job_name="aws_test_glue_job",
                desc="This is test case job from Airflow",
                script_location=some_script,
                iam_role_name="my_test_role",
                s3_bucket=some_s3_bucket,
                region_name=self.some_aws_region,
                num_of_dpus=20,
                create_job_kwargs={"WorkerType": "G.2X", "NumberOfWorkers": 60},
                update_config=True,
            )

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "conn")
    def test_initialize_job(self, mock_conn, mock_get_job_state):
        some_data_path = "s3://glue-datasets/examples/medicare/SampleData.csv"
        some_script_arguments = {"--s3_input_data_path": some_data_path}
        some_run_kwargs = {"NumberOfWorkers": 5}
        some_script = "s3:/glue-examples/glue-scripts/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"

        mock_conn.start_job_run()

        mock_job_run_state = mock_get_job_state.return_value
        glue_job_hook = GlueJobHook(
            job_name="aws_test_glue_job",
            desc="This is test case job from Airflow",
            iam_role_name="my_test_role",
            script_location=some_script,
            s3_bucket=some_s3_bucket,
            region_name=self.some_aws_region,
            update_config=False,
        )
        glue_job_run = glue_job_hook.initialize_job(some_script_arguments, some_run_kwargs)
        glue_job_run_state = glue_job_hook.get_job_state(glue_job_run["JobName"], glue_job_run["JobRunId"])
        assert glue_job_run_state == mock_job_run_state, "Mocks but be equal"

    @mock.patch.object(AwsLogsHook, "get_conn")
    @mock.patch.object(GlueJobHook, "conn")
    def test_print_job_logs_returns_token(self, conn_mock: MagicMock, log_client_mock: MagicMock, caplog):
        hook = GlueJobHook(job_name="test")
        conn_mock().get_job_run.return_value = {"JobRun": {"LogGroupName": "my_log_group"}}
        log_client_mock().get_paginator().paginate.return_value = [
            # first response : 2 log lines
            {
                "events": [
                    {"logStreamName": "stream", "timestamp": 123, "message": "hello\n"},
                    {"logStreamName": "stream", "timestamp": 123, "message": "world\n"},
                ],
                "searchedLogStreams": [],
                "nextToken": "my_continuation_token",
                "ResponseMetadata": {"HTTPStatusCode": 200},
            },
            # second response, reached end of stream
            {"events": [], "searchedLogStreams": [], "ResponseMetadata": {"HTTPStatusCode": 200}},
        ]

        tokens = GlueJobHook.LogContinuationTokens()
        with caplog.at_level("INFO"):
            hook.print_job_logs("name", "run", tokens)
        assert "\thello\n\tworld\n" in caplog.text
        assert tokens.output_stream_continuation == "my_continuation_token"
        assert tokens.error_stream_continuation == "my_continuation_token"

    @mock.patch.object(AwsLogsHook, "get_conn")
    @mock.patch.object(GlueJobHook, "conn")
    def test_print_job_logs_no_stream_yet(self, conn_mock: MagicMock, client_mock: MagicMock):
        hook = GlueJobHook()
        conn_mock().get_job_run.return_value = {"JobRun": {"LogGroupName": "my_log_group"}}
        client_mock().get_paginator().paginate.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "op"
        )

        tokens = GlueJobHook.LogContinuationTokens()
        hook.print_job_logs("name", "run", tokens)  # should not error

        assert tokens.output_stream_continuation is None
        assert tokens.error_stream_continuation is None
        assert client_mock().get_paginator().paginate.call_count == 2

    @mock.patch.object(GlueJobHook, "get_job_state")
    def test_job_completion_success(self, get_state_mock: MagicMock):
        hook = GlueJobHook(job_poll_interval=0)
        get_state_mock.side_effect = [
            "RUNNING",
            "RUNNING",
            "SUCCEEDED",
        ]

        hook.job_completion("job_name", "run_id")

        assert get_state_mock.call_count == 3
        get_state_mock.assert_called_with("job_name", "run_id")

    @mock.patch.object(GlueJobHook, "get_job_state")
    def test_job_completion_failure(self, get_state_mock: MagicMock):
        hook = GlueJobHook(job_poll_interval=0)
        get_state_mock.side_effect = [
            "RUNNING",
            "RUNNING",
            "FAILED",
        ]

        with pytest.raises(AirflowException):
            hook.job_completion("job_name", "run_id")

        assert get_state_mock.call_count == 3

    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "async_get_job_state")
    async def test_async_job_completion_success(self, get_state_mock: MagicMock):
        hook = GlueJobHook(job_poll_interval=0)
        get_state_mock.side_effect = [
            "RUNNING",
            "RUNNING",
            "SUCCEEDED",
        ]

        await hook.async_job_completion("job_name", "run_id")

        assert get_state_mock.call_count == 3
        get_state_mock.assert_called_with("job_name", "run_id")

    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "async_get_job_state")
    async def test_async_job_completion_failure(self, get_state_mock: MagicMock):
        hook = GlueJobHook(job_poll_interval=0)
        get_state_mock.side_effect = [
            "RUNNING",
            "RUNNING",
            "FAILED",
        ]

        with pytest.raises(AirflowException):
            await hook.async_job_completion("job_name", "run_id")

        assert get_state_mock.call_count == 3

    @mock.patch.object(GlueJobHook, "conn")
    def test_get_job_state_success(self, mock_conn):
        hook = GlueJobHook()
        job_name = "test_job"
        run_id = "test_run_id"
        expected_state = "SUCCEEDED"

        mock_conn.get_job_run.return_value = {"JobRun": {"JobRunState": expected_state}}

        result = hook.get_job_state(job_name, run_id)

        assert result == expected_state
        mock_conn.get_job_run.assert_called_once_with(
            JobName=job_name, RunId=run_id, PredecessorsIncluded=True
        )

    @mock.patch.object(GlueJobHook, "conn")
    def test_get_job_state_retry_on_client_error(self, mock_conn):
        hook = GlueJobHook()
        job_name = "test_job"
        run_id = "test_run_id"
        expected_state = "SUCCEEDED"

        mock_conn.get_job_run.side_effect = [
            ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, "get_job_run"
            ),
            {"JobRun": {"JobRunState": expected_state}},
        ]

        result = hook.get_job_state(job_name, run_id)

        assert result == expected_state
        assert mock_conn.get_job_run.call_count == 2

    @mock.patch.object(GlueJobHook, "conn")
    def test_get_job_state_fails_after_all_retries(self, mock_conn):
        """Test get_job_state raises exception when all retries are exhausted."""
        hook = GlueJobHook()
        job_name = "test_job"
        run_id = "test_run_id"

        mock_conn.get_job_run.side_effect = ClientError(
            {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}}, "get_job_run"
        )

        with pytest.raises(ClientError) as exc_info:
            hook.get_job_state(job_name, run_id)

        assert exc_info.value.response["Error"]["Code"] == "ThrottlingException"
        assert mock_conn.get_job_run.call_count == 5

    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_async_get_job_state_success(self, mock_get_async_conn):
        hook = GlueJobHook()
        job_name = "test_job"
        run_id = "test_run_id"
        expected_state = "RUNNING"

        mock_client = mock.AsyncMock()
        mock_client.get_job_run.return_value = {"JobRun": {"JobRunState": expected_state}}
        mock_context = mock.AsyncMock()
        mock_context.__aenter__.return_value = mock_client
        mock_context.__aexit__.return_value = None
        mock_get_async_conn.return_value = mock_context

        result = await hook.async_get_job_state(job_name, run_id)

        assert result == expected_state
        mock_client.get_job_run.assert_called_once_with(JobName=job_name, RunId=run_id)

    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_async_get_job_state_retry_on_client_error(self, mock_get_async_conn):
        hook = GlueJobHook()
        job_name = "test_job"
        run_id = "test_run_id"
        expected_state = "FAILED"

        mock_client = mock.AsyncMock()
        mock_client.get_job_run.side_effect = [
            ClientError(
                {"Error": {"Code": "ServiceUnavailable", "Message": "Service temporarily unavailable"}},
                "get_job_run",
            ),
            {"JobRun": {"JobRunState": expected_state}},
        ]
        mock_context = mock.AsyncMock()
        mock_context.__aenter__.return_value = mock_client
        mock_context.__aexit__.return_value = None
        mock_get_async_conn.return_value = mock_context

        result = await hook.async_get_job_state(job_name, run_id)

        assert result == expected_state
        assert mock_client.get_job_run.call_count == 2

    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_async_get_job_state_fails_after_all_retries(self, mock_get_async_conn):
        hook = GlueJobHook()
        job_name = "test_job"
        run_id = "test_run_id"

        mock_client = mock.AsyncMock()
        mock_client.get_job_run.side_effect = ClientError(
            {"Error": {"Code": "ServiceUnavailable", "Message": "Service temporarily unavailable"}},
            "get_job_run",
        )
        mock_context = mock.AsyncMock()
        mock_context.__aenter__.return_value = mock_client
        mock_context.__aexit__.return_value = None
        mock_get_async_conn.return_value = mock_context

        with pytest.raises(ClientError) as exc_info:
            await hook.async_get_job_state(job_name, run_id)

        assert exc_info.value.response["Error"]["Code"] == "ServiceUnavailable"
        assert mock_client.get_job_run.call_count == 5


class TestGlueDataQualityHook:
    RUN_ID = "1234"
    RULE_SET_NAME = "test_rule"
    RULE_SET_CONFIG = {
        "Name": "test_rule",
        "Ruleset": 'Rules=[ColumnLength "review_id" = 15]',
        "TargetTable": {"DatabaseName": "test_db", "TableName": "test_table"},
        "Description": "test rule",
    }

    def setup_method(self):
        self.glue = GlueDataQualityHook()

    def test_glue_data_quality_hook(self):
        glue_data_quality_hook = GlueDataQualityHook()
        assert glue_data_quality_hook.conn is not None
        assert glue_data_quality_hook.aws_conn_id == "aws_default"

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_data_quality_ruleset_exists(self, mock_conn):
        mock_conn.get_data_quality_ruleset.return_value = {"Name": self.RULE_SET_NAME}

        result = self.glue.has_data_quality_ruleset(name=self.RULE_SET_NAME)

        assert result is True
        mock_conn.get_data_quality_ruleset.assert_called_once_with(Name=self.RULE_SET_NAME)

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_quality_ruleset_doesnt_exists(self, mock_conn):
        error_message = f"Cannot find Data Quality Ruleset in account 1234567 with name {self.RULE_SET_NAME}"

        err_response = {"Error": {"Code": "EntityNotFoundException", "Message": error_message}}

        exception = boto3.client("glue").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.EntityNotFoundException = returned_exception
        mock_conn.get_data_quality_ruleset.side_effect = exception

        result = self.glue.has_data_quality_ruleset(name=self.RULE_SET_NAME)

        assert result is False
        mock_conn.get_data_quality_ruleset.assert_called_once_with(Name=self.RULE_SET_NAME)

    @mock.patch.object(AwsBaseHook, "conn")
    def test_validate_evaluation_results(self, mock_conn, caplog):
        response_evaluation_run = {"RunId": self.RUN_ID, "ResultIds": ["resultId1"]}

        response_batch_result = {
            "RunId": self.RUN_ID,
            "ResultIds": ["resultId1"],
            "Results": [
                {
                    "ResultId": "resultId1",
                    "RulesetName": "rulesetOne",
                    "RuleResults": [
                        {
                            "Name": "Rule_1",
                            "Description": "RowCount between 150000 and 600000",
                            "EvaluatedMetrics": {"Dataset.*.RowCount": 300000.0},
                            "Result": "PASS",
                        }
                    ],
                }
            ],
        }
        mock_conn.get_data_quality_ruleset_evaluation_run.return_value = response_evaluation_run

        mock_conn.batch_get_data_quality_result.return_value = response_batch_result

        with caplog.at_level(logging.INFO, logger=self.glue.log.name):
            caplog.clear()
            self.glue.validate_evaluation_run_results(evaluation_run_id=self.RUN_ID, show_results=False)

        mock_conn.get_data_quality_ruleset_evaluation_run.assert_called_once_with(RunId=self.RUN_ID)
        mock_conn.batch_get_data_quality_result.assert_called_once_with(
            ResultIds=response_evaluation_run["ResultIds"]
        )

        assert caplog.messages == [
            "AWS Glue data quality ruleset evaluation run, total number of rules failed: 0"
        ]

    @mock.patch.object(AwsBaseHook, "conn")
    def test_validate_evaluation_results_show_results_True(self, mock_conn, caplog):
        response_evaluation_run = {"RunId": self.RUN_ID, "ResultIds": ["resultId1"]}

        response_batch_result = {
            "RunId": self.RUN_ID,
            "ResultIds": ["resultId1"],
            "Results": [
                {
                    "ResultId": "resultId1",
                    "RulesetName": "rulesetOne",
                    "RuleResults": [
                        {
                            "Name": "Rule_1",
                            "Description": "RowCount between 150000 and 600000",
                            "EvaluatedMetrics": {"Dataset.*.RowCount": 300000.0},
                            "Result": "PASS",
                        }
                    ],
                }
            ],
        }
        mock_conn.get_data_quality_ruleset_evaluation_run.return_value = response_evaluation_run

        mock_conn.batch_get_data_quality_result.return_value = response_batch_result

        with caplog.at_level(logging.INFO, logger=self.glue.log.name):
            caplog.clear()
            self.glue.validate_evaluation_run_results(evaluation_run_id=self.RUN_ID, show_results=True)

        mock_conn.get_data_quality_ruleset_evaluation_run.assert_called_once_with(RunId=self.RUN_ID)
        mock_conn.batch_get_data_quality_result.assert_called_once_with(
            ResultIds=response_evaluation_run["ResultIds"]
        )
        # The messages have extra spaces to create spacing in the output, the number of consecutive spaces
        # may vary. Remove any sequence of spaces greater than 1 before asserting.
        messages = [" ".join(msg.split()) for msg in caplog.messages]
        assert messages == [
            "AWS Glue data quality ruleset evaluation result for RulesetName: rulesetOne RulesetEvaluationRunId: None Score: None",
            "Name Description EvaluatedMetrics Result 0 Rule_1 RowCount between 150000 and 600000 {'Dataset.*.RowCount': 300000.0} PASS",
            "AWS Glue data quality ruleset evaluation run, total number of rules failed: 0",
        ]

    @mock.patch.object(AwsBaseHook, "conn")
    def test_validate_evaluation_results_show_results_True_no_pandas(self, mock_conn, caplog):
        response_evaluation_run = {"RunId": self.RUN_ID, "ResultIds": ["resultId1"]}

        response_batch_result = {
            "RunId": self.RUN_ID,
            "ResultIds": ["resultId1"],
            "Results": [
                {
                    "ResultId": "resultId1",
                    "RulesetName": "rulesetOne",
                    "RuleResults": [
                        {
                            "Name": "Rule_1",
                            "Description": "RowCount between 150000 and 600000",
                            "EvaluatedMetrics": {"Dataset.*.RowCount": 300000.0},
                            "Result": "PASS",
                        }
                    ],
                }
            ],
        }
        mock_conn.get_data_quality_ruleset_evaluation_run.return_value = response_evaluation_run

        mock_conn.batch_get_data_quality_result.return_value = response_batch_result

        # Emulate/mock the import of pandas failing with ModlueNotFoundError
        with mock.patch.dict("sys.modules", {"pandas": None}):
            with caplog.at_level(logging.INFO, logger=self.glue.log.name):
                caplog.clear()
                self.glue.validate_evaluation_run_results(evaluation_run_id=self.RUN_ID, show_results=True)

        mock_conn.get_data_quality_ruleset_evaluation_run.assert_called_once_with(RunId=self.RUN_ID)
        mock_conn.batch_get_data_quality_result.assert_called_once_with(
            ResultIds=response_evaluation_run["ResultIds"]
        )
        assert caplog.messages == [
            "Pandas is not installed. Please install pandas to see the detailed Data Quality results.",
            "AWS Glue data quality ruleset evaluation run, total number of rules failed: 0",
        ]

    @mock.patch.object(AwsBaseHook, "conn")
    def test_validate_evaluation_results_should_fail_when_any_rules_failed(self, mock_conn, caplog):
        response_batch_result = {
            "RunId": self.RUN_ID,
            "ResultIds": ["resultId1"],
            "Results": [
                {
                    "ResultId": "resultId1",
                    "RulesetName": "rulesetOne",
                    "RuleResults": [
                        {
                            "Name": "Rule_1",
                            "Description": "RowCount between 150000 and 600000",
                            "EvaluatedMetrics": {"Dataset.*.RowCount": 300000.0},
                            "Result": "PASS",
                        },
                        {
                            "Name": "Rule_2",
                            "Description": "ColumnLength 'marketplace' between 1 and 2",
                            "EvaluationMessage": "Value: 9.0 does not meet the constraint requirement!",
                            "Result": "FAIL",
                            "EvaluatedMetrics": {
                                "Column.marketplace.MaximumLength": 9.0,
                                "Column.marketplace.MinimumLength": 2.0,
                            },
                        },
                    ],
                }
            ],
        }

        response_evaluation_run = {"RunId": self.RUN_ID, "ResultIds": ["resultId1"]}

        mock_conn.get_data_quality_ruleset_evaluation_run.return_value = response_evaluation_run

        mock_conn.batch_get_data_quality_result.return_value = response_batch_result

        with caplog.at_level(logging.INFO, logger=self.glue.log.name):
            caplog.clear()

            with pytest.raises(
                AirflowException,
                match="AWS Glue data quality ruleset evaluation run failed for one or more rules",
            ):
                self.glue.validate_evaluation_run_results(evaluation_run_id=self.RUN_ID, show_results=False)

            mock_conn.get_data_quality_ruleset_evaluation_run.assert_called_once_with(RunId=self.RUN_ID)
            mock_conn.batch_get_data_quality_result.assert_called_once_with(
                ResultIds=response_evaluation_run["ResultIds"]
            )

        assert caplog.messages == [
            "AWS Glue data quality ruleset evaluation run, total number of rules failed: 1"
        ]

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_log_recommendation_results(self, glue_data_quality_hook_mock_conn, caplog):
        rules = """ Rules = [
                                RowCount between 2 and 8,
                                IsComplete "name"
                                ]
                    """
        glue_data_quality_hook_mock_conn.get_data_quality_rule_recommendation_run.return_value = {
            "RunId": self.RUN_ID,
            "DataSource": {"GlueTable": {"DatabaseName": "TestDB", "TableName": "TestTable"}},
            "RecommendedRuleset": rules,
        }

        with caplog.at_level(logging.INFO, logger=self.glue.log.name):
            self.glue.log_recommendation_results(run_id=self.RUN_ID)

        glue_data_quality_hook_mock_conn.get_data_quality_rule_recommendation_run.assert_called_once_with(
            RunId=self.RUN_ID
        )
        assert rules in caplog.messages
