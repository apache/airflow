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

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.links.glue import GlueJobRunDetailsLink
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

if TYPE_CHECKING:
    from airflow.models import TaskInstance

TASK_ID = "test_glue_operator"
DAG_ID = "test_dag_id"
JOB_NAME = "test_job_name/with_slash"
JOB_RUN_ID = "11111"


class TestGlueJobOperator:
    @pytest.mark.db_test
    def test_render_template(self, create_task_instance_of_operator):
        ti: TaskInstance = create_task_instance_of_operator(
            GlueJobOperator,
            dag_id=DAG_ID,
            task_id=TASK_ID,
            script_location="{{ dag.dag_id }}",
            script_args="{{ dag.dag_id }}",
            create_job_kwargs="{{ dag.dag_id }}",
            iam_role_name="{{ dag.dag_id }}",
            iam_role_arn="{{ dag.dag_id }}",
            s3_bucket="{{ dag.dag_id }}",
            job_name="{{ dag.dag_id }}",
        )
        rendered_template: GlueJobOperator = ti.render_templates()

        assert DAG_ID == rendered_template.script_location
        assert DAG_ID == rendered_template.script_args
        assert DAG_ID == rendered_template.create_job_kwargs
        assert DAG_ID == rendered_template.iam_role_name
        assert DAG_ID == rendered_template.iam_role_arn
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
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_get_job_state.return_value = "SUCCEEDED"

        glue.execute(mock.MagicMock())

        mock_initialize_job.assert_called_once_with({}, {})
        mock_print_job_logs.assert_not_called()
        assert glue.job_name == JOB_NAME

    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_role_arn_execute_deferrable(self, _, mock_initialize_job):
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            aws_conn_id="aws_default",
            region_name="us-west-2",
            s3_bucket="some_bucket",
            iam_role_arn="test_role",
            deferrable=True,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        with pytest.raises(TaskDeferred) as defer:
            glue.execute(mock.MagicMock())

        assert defer.value.trigger.job_name == JOB_NAME
        assert defer.value.trigger.run_id == JOB_RUN_ID

    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_execute_deferrable(self, _, mock_initialize_job):
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            aws_conn_id="aws_default",
            region_name="us-west-2",
            s3_bucket="some_bucket",
            iam_role_name="my_test_role",
            deferrable=True,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        with pytest.raises(TaskDeferred) as defer:
            glue.execute(mock.MagicMock())

        assert defer.value.trigger.job_name == JOB_NAME
        assert defer.value.trigger.run_id == JOB_RUN_ID

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_with_verbose_logging(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_print_job_logs
    ):
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3_uri",
            s3_bucket="bucket_name",
            iam_role_name="role_arn",
            verbose=True,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_get_job_state.return_value = "SUCCEEDED"

        glue.execute(mock.MagicMock())

        mock_initialize_job.assert_called_once_with({}, {})
        mock_print_job_logs.assert_called_once_with(
            job_name=JOB_NAME, run_id=JOB_RUN_ID, continuation_tokens=mock.ANY
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
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        job_run_id = glue.execute(mock.MagicMock())

        mock_initialize_job.assert_called_once_with({}, {})
        mock_job_completion.assert_not_called()
        mock_print_job_logs.assert_not_called()
        assert glue.job_name == JOB_NAME
        assert job_run_id == JOB_RUN_ID

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_log_correct_url(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_print_job_logs
    ):
        region = "us-west-2"
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://glue-examples/glue-scripts/sample_aws_glue_job.py",
            aws_conn_id="aws_default",
            region_name=region,
            s3_bucket="some_bucket",
            iam_role_name="my_test_role",
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_get_job_state.return_value = "SUCCEEDED"

        aws_domain = GlueJobRunDetailsLink.get_aws_domain("aws")
        glue_job_run_url = (
            f"https://console.{aws_domain}/gluestudio/home?region="
            f"{region}#/job/test_job_name%2Fwith_slash/run/{JOB_RUN_ID}"
        )

        with mock.patch.object(glue.log, "info") as mock_log_info:
            job_run_id = glue.execute(mock.MagicMock())
            assert job_run_id == JOB_RUN_ID

        mock_log_info.assert_any_call("You can monitor this Glue Job run at: %s", glue_job_run_url)

    @mock.patch.object(GlueJobHook, "conn")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_killed_without_stop_job_run_on_kill(
        self,
        _,
        mock_glue_hook,
    ):
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            aws_conn_id="aws_default",
            region_name="us-west-2",
            s3_bucket="some_bucket",
            iam_role_name="my_test_role",
        )
        glue.on_kill()
        mock_glue_hook.batch_stop_job_run.assert_not_called()

    @mock.patch.object(GlueJobHook, "conn")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_killed_with_stop_job_run_on_kill(
        self,
        _,
        mock_glue_hook,
    ):
        glue = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            aws_conn_id="aws_default",
            region_name="us-west-2",
            s3_bucket="some_bucket",
            iam_role_name="my_test_role",
            stop_job_run_on_kill=True,
        )
        glue._job_run_id = JOB_RUN_ID
        glue.on_kill()
        mock_glue_hook.batch_stop_job_run.assert_called_once_with(
            JobName=JOB_NAME,
            JobRunIds=[JOB_RUN_ID],
        )
