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

from airflow.configuration import conf
from airflow.models import TaskInstance
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

TASK_ID = "test_glue_operator"
DAG_ID = "test_dag_id"
JOB_NAME = "test_job_name"


class TestGlueJobOperator:
    @pytest.fixture(autouse=True)
    def setup_method(self):
        conf.load_test_config()

    def test_render_template(self, create_task_instance_of_operator):
        ti: TaskInstance = create_task_instance_of_operator(
            GlueJobOperator,
            dag_id=DAG_ID,
            task_id=TASK_ID,
            script_location="{{ dag.dag_id }}",
            script_args="{{ dag.dag_id }}",
            create_job_kwargs="{{ dag.dag_id }}",
            iam_role_name="{{ dag.dag_id }}",
            s3_bucket="{{ dag.dag_id }}",
            job_name="{{ dag.dag_id }}",
        )
        rendered_template: GlueJobOperator = ti.render_templates()

        assert DAG_ID == rendered_template.script_location
        assert DAG_ID == rendered_template.script_args
        assert DAG_ID == rendered_template.create_job_kwargs
        assert DAG_ID == rendered_template.iam_role_name
        assert DAG_ID == rendered_template.s3_bucket
        assert DAG_ID == rendered_template.job_name

    @pytest.mark.parametrize(
        "script_location",
        [
            "s3://glue-examples/glue-scripts/sample_aws_glue_job.py",
            "/glue-examples/glue-scripts/sample_aws_glue_job.py",
        ],
    )
    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_without_failure(
        self,
        mock_load_file,
        mock_get_conn,
        mock_initialize_job,
        mock_get_job_state,
        mock_print_job_logs,
        script_location,
    ):
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location=script_location,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            s3_bucket="some_bucket",
            iam_role_name="my_test_role",
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": "11111"}
        mock_get_job_state.return_value = "SUCCEEDED"

        glue.execute({})

        mock_initialize_job.assert_called_once_with({}, {})
        mock_print_job_logs.assert_not_called()
        assert glue.job_name == JOB_NAME

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_with_verbose_logging(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_print_job_logs
    ):
        job_run_id = "11111"
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3_uri",
            s3_bucket="bucket_name",
            iam_role_name="role_arn",
            verbose=True,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": job_run_id}
        mock_get_job_state.return_value = "SUCCEEDED"

        glue.execute({})

        mock_initialize_job.assert_called_once_with({}, {})
        mock_print_job_logs.assert_called_once_with(
            job_name=JOB_NAME,
            run_id=job_run_id,
            job_failed=False,
            next_token=None,
        )
        assert glue.job_name == JOB_NAME

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_without_waiting_for_completion(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_job_completion, mock_print_job_logs
    ):
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://glue-examples/glue-scripts/sample_aws_glue_job.py",
            aws_conn_id="aws_default",
            region_name="us-west-2",
            s3_bucket="some_bucket",
            iam_role_name="my_test_role",
            wait_for_completion=False,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": "11111"}

        job_run_id = glue.execute({})

        mock_initialize_job.assert_called_once_with({}, {})
        mock_job_completion.assert_not_called()
        mock_print_job_logs.assert_not_called()
        assert glue.job_name == JOB_NAME
        assert job_run_id == "11111"
