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
from unittest import mock

import boto3
import pytest
from moto import mock_glue, mock_iam

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook


class TestGlueJobHook:
    def setup_method(self):
        self.some_aws_region = "us-west-2"

    @mock_iam
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

    @mock.patch.object(AwsBaseHook, "get_conn")
    def test_has_job_exists(self, mock_get_conn):
        job_name = "aws_test_glue_job"
        mock_get_conn.return_value.get_job.return_value = {"Job": {"Name": job_name}}

        hook = GlueJobHook(aws_conn_id=None, job_name=job_name, s3_bucket="some_bucket")
        result = hook.has_job(job_name)
        assert result is True
        mock_get_conn.return_value.get_job.assert_called_once_with(JobName=hook.job_name)

    @mock.patch.object(AwsBaseHook, "get_conn")
    def test_has_job_job_doesnt_exists(self, mock_get_conn):
        class JobNotFoundException(Exception):
            pass

        mock_get_conn.return_value.exceptions.EntityNotFoundException = JobNotFoundException
        mock_get_conn.return_value.get_job.side_effect = JobNotFoundException()

        job_name = "aws_test_glue_job"
        hook = GlueJobHook(aws_conn_id=None, job_name=job_name, s3_bucket="some_bucket")
        result = hook.has_job(job_name)
        assert result is False
        mock_get_conn.return_value.get_job.assert_called_once_with(JobName=job_name)

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_create_or_update_glue_job_create_new_job(self, mock_get_conn, mock_get_iam_execution_role):
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

        mock_get_conn.return_value.exceptions.EntityNotFoundException = JobNotFoundException
        mock_get_conn.return_value.get_job.side_effect = JobNotFoundException()
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
        )

        result = hook.create_or_update_glue_job()

        mock_get_conn.return_value.get_job.assert_called_once_with(JobName=expected_job_name)
        mock_get_conn.return_value.create_job.assert_called_once_with(
            Command={},
            Description=job_description,
            ExecutionProperty={"MaxConcurrentRuns": 2},
            LogUri=f"s3://{some_s3_bucket}/logs/glue-logs/{expected_job_name}",
            MaxCapacity=5,
            MaxRetries=3,
            Name=expected_job_name,
            Role=role_name_arn,
        )
        mock_get_conn.return_value.update_job.assert_not_called()
        assert result == expected_job_name

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_create_or_update_glue_job_update_existing_job(self, mock_get_conn, mock_get_iam_execution_role):
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

        mock_get_conn.return_value.get_job.return_value = {
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
        )

        result = hook.create_or_update_glue_job()

        assert mock_get_conn.return_value.get_job.call_count == 2
        mock_get_conn.return_value.update_job.assert_called_once_with(
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

    @mock_glue
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
        )

        result = glue_job.create_or_update_glue_job()

        assert result == expected_job_name

    @mock.patch.object(GlueJobHook, "get_iam_execution_role")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_init_worker_type_value_error(self, mock_get_conn, mock_get_iam_execution_role):
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
            )

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "create_or_update_glue_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_initialize_job(self, mock_get_conn, mock_create_or_update_glue_job, mock_get_job_state):
        some_data_path = "s3://glue-datasets/examples/medicare/SampleData.csv"
        some_script_arguments = {"--s3_input_data_path": some_data_path}
        some_run_kwargs = {"NumberOfWorkers": 5}
        some_script = "s3:/glue-examples/glue-scripts/sample_aws_glue_job.py"
        some_s3_bucket = "my-includes"

        mock_create_or_update_glue_job.Name = mock.Mock(Name="aws_test_glue_job")
        mock_get_conn.return_value.start_job_run()

        mock_job_run_state = mock_get_job_state.return_value
        glue_job_hook = GlueJobHook(
            job_name="aws_test_glue_job",
            desc="This is test case job from Airflow",
            iam_role_name="my_test_role",
            script_location=some_script,
            s3_bucket=some_s3_bucket,
            region_name=self.some_aws_region,
        )
        glue_job_run = glue_job_hook.initialize_job(some_script_arguments, some_run_kwargs)
        glue_job_run_state = glue_job_hook.get_job_state(glue_job_run["JobName"], glue_job_run["JobRunId"])
        assert glue_job_run_state == mock_job_run_state, "Mocks but be equal"
