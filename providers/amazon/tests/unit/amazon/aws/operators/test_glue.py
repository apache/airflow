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

import re
import warnings
from collections.abc import Generator
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from unittest import mock

import boto3
import pytest
from boto3 import client
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.exceptions import GlueJobRunStoppedError
from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.links.glue import GlueJobRunDetailsLink
from airflow.providers.amazon.aws.operators.glue import (
    _DURABLE_UNSET,
    GlueDataQualityOperator,
    GlueDataQualityRuleRecommendationRunOperator,
    GlueDataQualityRuleSetEvaluationRunOperator,
    GlueJobOperator,
    _warn_and_disable_durable_pre_3_3,
)
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS
from unit.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection

TASK_ID = "test_glue_operator"
DAG_ID = "test_dag_id"
JOB_NAME = "test_job_name/with_slash"
JOB_RUN_ID = "11111"

_DEPRECATION_MESSAGE_PREFIX = (
    "`resume_glue_job_on_retry` is deprecated and will be removed once this provider's "
    "minimum supported Airflow version reaches 3.3. "
)
DEPRECATION_MESSAGE_PRE_3_3 = _DEPRECATION_MESSAGE_PREFIX + "On Airflow 3.3+, use `durable` instead."
DEPRECATION_MESSAGE_3_3_PLUS = _DEPRECATION_MESSAGE_PREFIX + "Use `durable` instead."
EXPECTED_DEPRECATION_MESSAGE = (
    DEPRECATION_MESSAGE_3_3_PLUS if AIRFLOW_V_3_3_PLUS else DEPRECATION_MESSAGE_PRE_3_3
)


class TestGlueJobOperator:
    @pytest.mark.db_test
    def test_render_template(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        rendered_template: GlueJobOperator = ti.render_templates()

        assert rendered_template.script_location == DAG_ID
        assert rendered_template.script_args == DAG_ID
        assert rendered_template.create_job_kwargs == DAG_ID
        assert rendered_template.iam_role_name == DAG_ID
        assert rendered_template.iam_role_arn == DAG_ID
        assert rendered_template.s3_bucket == DAG_ID
        assert rendered_template.job_name == DAG_ID

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
            durable=False,
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
            durable=False,
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
            durable=False,
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
        assert defer.value.trigger.region_name == "us-west-2"
        assert not defer.value.trigger.verbose
        assert defer.value.trigger.waiter_delay == 60
        assert defer.value.trigger.attempts == 75
        assert defer.value.trigger.aws_conn_id == "aws_default"

    @mock.patch.object(GlueJobHook, "conn", new_callable=mock.PropertyMock)
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_deferrable_first_attempt_injects_task_uuid_but_skips_scan(
        self, mock_get_conn, mock_initialize_job, mock_conn
    ):
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=f"^{re.escape(EXPECTED_DEPRECATION_MESSAGE)}$"
        ):
            glue = GlueJobOperator(
                task_id=TASK_ID,
                job_name=JOB_NAME,
                script_location="s3://folder/file",
                deferrable=True,
                resume_glue_job_on_retry=True,
            )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_ti = mock.MagicMock()
        mock_ti.try_number = 1

        with pytest.raises(TaskDeferred):
            glue.execute({"ti": mock_ti})

        call_args = mock_initialize_job.call_args[0][0]
        assert GlueJobOperator.TASK_UUID_ARG in call_args
        mock_conn.return_value.get_job_runs.assert_not_called()

    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_deferrable_retry_reattaches_via_task_uuid_scan(self, mock_get_conn, mock_initialize_job):
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=f"^{re.escape(EXPECTED_DEPRECATION_MESSAGE)}$"
        ):
            glue = GlueJobOperator(
                task_id=TASK_ID,
                job_name=JOB_NAME,
                script_location="s3://folder/file",
                deferrable=True,
                resume_glue_job_on_retry=True,
            )
        mock_ti = mock.MagicMock()
        mock_ti.dag_id = "test_dag_id"
        mock_ti.task_id = TASK_ID
        mock_ti.run_id = "manual__2024-01-01T00:00:00+00:00"
        mock_ti.map_index = -1
        mock_ti.try_number = 2
        mock_ti.xcom_pull.return_value = None
        task_uuid = f"{mock_ti.dag_id}:{mock_ti.task_id}:{mock_ti.run_id}:{mock_ti.map_index}"

        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.return_value = {
            "JobRuns": [
                {
                    "Id": JOB_RUN_ID,
                    "Arguments": {GlueJobOperator.TASK_UUID_ARG: task_uuid},
                    "JobRunState": "RUNNING",
                }
            ]
        }

        with pytest.raises(TaskDeferred) as defer:
            glue.execute({"ti": mock_ti})

        assert defer.value.trigger.run_id == JOB_RUN_ID

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="task_state_store only exists as an execute() context key on Airflow 3.3+",
    )
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_deferrable_retry_reattaches_via_fallback_when_task_store_errors(
        self, mock_get_conn, mock_initialize_job
    ):
        glue = GlueJobOperator(
            durable=True,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            deferrable=True,
        )
        mock_ti = mock.MagicMock()
        mock_ti.dag_id = "test_dag_id"
        mock_ti.task_id = TASK_ID
        mock_ti.run_id = "manual__2024-01-01T00:00:00+00:00"
        mock_ti.map_index = -1
        mock_ti.try_number = 2
        mock_ti.xcom_pull.return_value = None
        task_uuid = f"{mock_ti.dag_id}:{mock_ti.task_id}:{mock_ti.run_id}:{mock_ti.map_index}"

        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.return_value = {
            "JobRuns": [
                {
                    "Id": JOB_RUN_ID,
                    "Arguments": {GlueJobOperator.TASK_UUID_ARG: task_uuid},
                    "JobRunState": "RUNNING",
                }
            ]
        }
        erroring_store = mock.MagicMock()
        erroring_store.get.side_effect = RuntimeError("store unavailable")

        with pytest.raises(TaskDeferred) as defer:
            glue.execute({"ti": mock_ti, "task_state_store": erroring_store})

        assert defer.value.trigger.run_id == JOB_RUN_ID
        mock_initialize_job.assert_not_called()
        mock_initialize_job.assert_not_called()

    @mock.patch.object(GlueJobHook, "print_job_logs")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_with_verbose_logging(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_print_job_logs
    ):
        glue = GlueJobOperator(
            durable=False,
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
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_without_verbose_logging(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_print_job_logs
    ):
        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3_uri",
            s3_bucket="bucket_name",
            iam_role_name="role_arn",
            verbose=False,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_get_job_state.return_value = "SUCCEEDED"

        glue.execute(mock.MagicMock())

        mock_initialize_job.assert_called_once_with({}, {})
        mock_print_job_logs.assert_not_called()
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
            durable=False,
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
            durable=False,
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

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_replace_script_file(
        self, mock_load_file, mock_conn, mock_get_connection, mock_initialize_job, mock_get_job_state
    ):
        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="folder/file",
            s3_bucket="bucket_name",
            iam_role_name="role_arn",
            replace_script_file=True,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_get_job_state.return_value = "SUCCEEDED"
        glue.execute(mock.MagicMock())
        mock_load_file.assert_called_once_with(
            "folder/file", "artifacts/glue-scripts/file", bucket_name="bucket_name", replace=True
        )

        assert glue.s3_script_location == "s3://bucket_name/artifacts/glue-scripts/file"

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "conn")
    @mock.patch.object(S3Hook, "load_file")
    @mock.patch.object(GlueJobOperator, "upload_etl_script_to_s3")
    def test_upload_script_to_s3_no_upload(
        self,
        mock_upload,
        mock_load_file,
        mock_conn,
        mock_get_connection,
        mock_initialize_job,
        mock_get_job_state,
    ):
        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://my_bucket/folder/file",
            s3_bucket="bucket_name",
            iam_role_name="role_arn",
            replace_script_file=True,
        )
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_get_job_state.return_value = "SUCCEEDED"
        glue.execute(mock.MagicMock())

        assert glue.s3_script_location == "s3://my_bucket/folder/file"
        mock_load_file.assert_not_called()
        mock_upload.assert_not_called()

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "conn")
    @mock.patch.object(S3Hook, "load_file")
    @mock.patch.object(GlueJobOperator, "upload_etl_script_to_s3")
    def test_no_script_file(
        self,
        mock_upload,
        mock_load_file,
        mock_conn,
        mock_get_connection,
        mock_initialize_job,
        mock_get_job_state,
    ):
        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            iam_role_name="role_arn",
            replace_script_file=True,
        )

        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}
        mock_get_job_state.return_value = "SUCCEEDED"
        glue.execute(mock.MagicMock())

        assert glue.s3_script_location is None
        mock_upload.assert_not_called()

    def test_template_fields(self):
        operator = GlueJobOperator(
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="folder/file",
            s3_bucket="bucket_name",
            iam_role_name="role_arn",
            replace_script_file=True,
        )
        validate_template_fields(operator)

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = GlueJobOperator(
            task_id=TASK_ID,
            aws_conn_id=OVERWRITTEN_CONN,
            iam_role_name="role_arn",
            replace_script_file=True,
        )
        assert op.hook.aws_conn_id == OVERWRITTEN_CONN

    def test_default_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = GlueJobOperator(
            task_id=TASK_ID,
            iam_role_name="role_arn",
            replace_script_file=True,
        )
        assert op.hook.aws_conn_id == DEFAULT_CONN

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @pytest.mark.parametrize("state", ["RUNNING", "STARTING", "WAITING", "STOPPING"])
    def test_find_previous_job_run_reuses_from_xcom(self, mock_initialize_job, mock_get_conn, state):
        with mock.patch("airflow.providers.amazon.aws.operators.glue.AIRFLOW_V_3_3_PLUS", False):
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=f"^{re.escape(DEPRECATION_MESSAGE_PRE_3_3)}$"
            ):
                glue = GlueJobOperator(
                    task_id=TASK_ID,
                    job_name=JOB_NAME,
                    script_location="s3://folder/file",
                    aws_conn_id="aws_default",
                    region_name="us-west-2",
                    s3_bucket="some_bucket",
                    iam_role_name="my_test_role",
                    wait_for_completion=False,
                    resume_glue_job_on_retry=True,
                )

            mock_ti = mock.MagicMock()
            mock_ti.try_number = 2  # the lookup only runs on a retry
            previous_job_run_id = "previous_run_12345"
            mock_ti.xcom_pull.return_value = previous_job_run_id
            mock_context = {"ti": mock_ti}

            mock_glue_client = mock.MagicMock()
            glue.hook.conn = mock_glue_client
            mock_glue_client.get_job_run.return_value = {"JobRun": {"JobRunState": state}}

            job_run_id = glue.execute(mock_context)

        assert job_run_id == previous_job_run_id
        assert glue._job_run_id == previous_job_run_id
        mock_initialize_job.assert_not_called()
        mock_glue_client.get_job_runs.assert_not_called()

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    def test_find_previous_job_run_does_not_fall_back_to_scan_on_xcom_state_mismatch(
        self, mock_initialize_job, mock_get_conn
    ):
        """A stale XCom state doesn't fall back to the task-UUID scan -- it's elif, not a chain."""
        glue = GlueJobOperator(
            durable=True,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            aws_conn_id="aws_default",
            region_name="us-west-2",
            s3_bucket="some_bucket",
            iam_role_name="my_test_role",
            wait_for_completion=False,
        )

        mock_ti = mock.MagicMock()
        mock_ti.try_number = 2
        mock_ti.xcom_pull.return_value = "previous_run_12345"
        mock_context = {"ti": mock_ti}

        mock_glue_client = mock.MagicMock()
        glue.hook.conn = mock_glue_client
        mock_glue_client.get_job_run.return_value = {"JobRun": {"JobRunState": "SUCCEEDED"}}

        new_job_run_id = "new_run_67890"
        mock_initialize_job.return_value = {"JobRunId": new_job_run_id}

        job_run_id = glue.execute(mock_context)

        assert job_run_id == new_job_run_id
        mock_initialize_job.assert_called_once()
        mock_glue_client.get_job_runs.assert_not_called()
        mock_ti.xcom_push.assert_any_call(key="glue_job_run_id", value=new_job_run_id)

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @pytest.mark.parametrize("state", ["RUNNING", "STARTING", "WAITING", "STOPPING"])
    def test_find_job_run_by_task_uuid_reconnects(self, mock_initialize_job, mock_get_conn, state):
        with mock.patch("airflow.providers.amazon.aws.operators.glue.AIRFLOW_V_3_3_PLUS", False):
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=f"^{re.escape(DEPRECATION_MESSAGE_PRE_3_3)}$"
            ):
                glue = GlueJobOperator(
                    task_id=TASK_ID,
                    job_name=JOB_NAME,
                    script_location="s3://folder/file",
                    aws_conn_id="aws_default",
                    region_name="us-west-2",
                    s3_bucket="some_bucket",
                    iam_role_name="my_test_role",
                    wait_for_completion=False,
                    resume_glue_job_on_retry=True,
                )

            mock_ti = mock.MagicMock()
            mock_ti.dag_id = "test_dag_id"
            mock_ti.task_id = TASK_ID
            mock_ti.run_id = "manual__2024-01-01T00:00:00+00:00"
            mock_ti.map_index = -1
            mock_ti.try_number = 2
            mock_ti.xcom_pull.return_value = None
            mock_context = {"ti": mock_ti}

            task_uuid = f"{mock_ti.dag_id}:{mock_ti.task_id}:{mock_ti.run_id}:{mock_ti.map_index}"

            mock_glue_client = mock.MagicMock()
            glue.hook.conn = mock_glue_client
            mock_glue_client.get_job_runs.return_value = {
                "JobRuns": [
                    {
                        "Id": "existing_run_123",
                        "Arguments": {GlueJobOperator.TASK_UUID_ARG: task_uuid},
                        "JobRunState": state,
                    }
                ]
            }

            job_run_id = glue.execute(mock_context)

        assert job_run_id == "existing_run_123"
        assert glue._job_run_id == "existing_run_123"
        mock_initialize_job.assert_not_called()
        mock_ti.xcom_push.assert_any_call(key="glue_job_run_id", value="existing_run_123")

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_task_uuid_scan_paginates_until_match(self, mock_get_conn):
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.side_effect = [
            {"JobRuns": [], "NextToken": "next-page"},
            {
                "JobRuns": [
                    {
                        "Id": JOB_RUN_ID,
                        "Arguments": {GlueJobOperator.TASK_UUID_ARG: "test-task-uuid"},
                        "JobRunState": "RUNNING",
                    }
                ]
            },
        ]

        assert glue._find_job_run_id_by_task_uuid("test-task-uuid") == (JOB_RUN_ID, "RUNNING")
        assert glue.hook.conn.get_job_runs.call_args_list == [
            mock.call(JobName=JOB_NAME, MaxResults=GlueJobOperator.TASK_UUID_SCAN_PAGE_SIZE),
            mock.call(
                JobName=JOB_NAME,
                MaxResults=GlueJobOperator.TASK_UUID_SCAN_PAGE_SIZE,
                NextToken="next-page",
            ),
        ]

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_task_uuid_scan_stops_after_page_limit(self, mock_get_conn, caplog):
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.TASK_UUID_SCAN_MAX_PAGES = 2
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.return_value = {"JobRuns": [], "NextToken": "next-page"}

        with caplog.at_level("ERROR"):
            assert glue._find_job_run_id_by_task_uuid("missing-task-uuid") is None

        assert glue.hook.conn.get_job_runs.call_count == 2
        assert "pages without a match" in caplog

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_task_uuid_scan_stops_when_started_on_predates_cutoff(self, mock_get_conn):
        cutoff = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.return_value = {
            "JobRuns": [
                {
                    "Id": "older-run",
                    "JobRunState": "SUCCEEDED",
                    "StartedOn": datetime(2024, 5, 31, 12, 0, tzinfo=timezone.utc),
                    "Arguments": {},
                }
            ],
            "NextToken": "next-page",
        }

        assert glue._find_job_run_id_by_task_uuid("test-task-uuid", cutoff=cutoff) is None
        glue.hook.conn.get_job_runs.assert_called_once_with(
            JobName=JOB_NAME, MaxResults=GlueJobOperator.TASK_UUID_SCAN_PAGE_SIZE
        )

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_find_previous_job_run_uses_dag_run_start_date_as_cutoff(self, mock_get_conn):
        cutoff = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.return_value = {
            "JobRuns": [
                {
                    "Id": "older-run",
                    "JobRunState": "SUCCEEDED",
                    "StartedOn": datetime(2024, 5, 31, 12, 0, tzinfo=timezone.utc),
                    "Arguments": {},
                }
            ],
            "NextToken": "next-page",
        }
        mock_ti = mock.MagicMock()
        mock_ti.task_id = TASK_ID
        mock_ti.xcom_pull.return_value = None
        dag_run = mock.MagicMock()
        dag_run.start_date = cutoff

        assert glue._find_previous_job_run({"ti": mock_ti, "dag_run": dag_run}, "test-task-uuid") is None
        glue.hook.conn.get_job_runs.assert_called_once_with(
            JobName=JOB_NAME, MaxResults=GlueJobOperator.TASK_UUID_SCAN_PAGE_SIZE
        )

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_task_uuid_scan_continues_when_started_on_is_not_before_cutoff(self, mock_get_conn):
        cutoff = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.side_effect = [
            {
                "JobRuns": [
                    {
                        "Id": "newer-unrelated",
                        "JobRunState": "SUCCEEDED",
                        "StartedOn": cutoff,
                        "Arguments": {},
                    }
                ],
                "NextToken": "next-page",
            },
            {
                "JobRuns": [
                    {
                        "Id": JOB_RUN_ID,
                        "JobRunState": "RUNNING",
                        "StartedOn": cutoff,
                        "Arguments": {GlueJobOperator.TASK_UUID_ARG: "test-task-uuid"},
                    }
                ]
            },
        ]

        assert glue._find_job_run_id_by_task_uuid("test-task-uuid", cutoff=cutoff) == (JOB_RUN_ID, "RUNNING")
        assert glue.hook.conn.get_job_runs.call_count == 2

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_find_previous_job_run_scan_client_error_returns_none(self, mock_get_conn, caplog):
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "not authorized"}},
            "GetJobRuns",
        )
        mock_ti = mock.MagicMock()
        mock_ti.task_id = TASK_ID
        mock_ti.xcom_pull.return_value = None

        with caplog.at_level("ERROR"):
            assert glue._find_previous_job_run({"ti": mock_ti}, "test-task-uuid") is None

        assert "Failed to find previous Glue job run by task UUID" in caplog

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_find_previous_job_run_scan_non_client_error_propagates(self, mock_get_conn):
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.side_effect = RuntimeError("scan bug")
        mock_ti = mock.MagicMock()
        mock_ti.task_id = TASK_ID
        mock_ti.xcom_pull.return_value = None

        with pytest.raises(RuntimeError, match="scan bug"):
            glue._find_previous_job_run({"ti": mock_ti}, "test-task-uuid")

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_find_previous_job_run_xcom_client_error_returns_none(self, mock_get_conn, caplog):
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_run.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "missing"}},
            "GetJobRun",
        )
        mock_ti = mock.MagicMock()
        mock_ti.task_id = TASK_ID
        mock_ti.xcom_pull.return_value = "previous_run_12345"

        with caplog.at_level("ERROR"):
            assert glue._find_previous_job_run({"ti": mock_ti}, "unused-uuid") is None

        glue.hook.conn.get_job_runs.assert_not_called()
        assert "Failed to get previous Glue job run state" in caplog

    @mock.patch.object(GlueJobHook, "get_conn")
    def test_find_previous_job_run_xcom_non_client_error_propagates(self, mock_get_conn):
        glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_run.side_effect = RuntimeError("lookup bug")
        mock_ti = mock.MagicMock()
        mock_ti.task_id = TASK_ID
        mock_ti.xcom_pull.return_value = "previous_run_12345"

        with pytest.raises(RuntimeError, match="lookup bug"):
            glue._find_previous_job_run({"ti": mock_ti}, "unused-uuid")


class TestGlueJobOperatorOpenLineageInjection:
    """Tests for OpenLineage parent job info and transport info injection in GlueJobOperator."""

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch(
        "airflow.providers.amazon.aws.operators.glue.inject_parent_job_information_into_glue_arguments"
    )
    def test_inject_parent_job_info_called_when_enabled(
        self, mock_inject_parent, mock_initialize_job, mock_get_conn
    ):
        mock_inject_parent.side_effect = lambda args, ctx: {
            **args,
            "--conf": "spark.openlineage.parentJobNamespace=ns",
        }
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            iam_role_name="my_test_role",
            wait_for_completion=False,
            openlineage_inject_parent_job_info=True,
        )
        context = mock.MagicMock()
        glue.execute(context)

        mock_inject_parent.assert_called_once()
        call_args = mock_initialize_job.call_args[0][0]
        assert "--conf" in call_args
        assert "spark.openlineage.parentJobNamespace=ns" in call_args["--conf"]

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch(
        "airflow.providers.amazon.aws.operators.glue.inject_parent_job_information_into_glue_arguments"
    )
    def test_inject_parent_job_info_not_called_when_disabled(
        self, mock_inject_parent, mock_initialize_job, mock_get_conn
    ):
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            iam_role_name="my_test_role",
            wait_for_completion=False,
            openlineage_inject_parent_job_info=False,
        )
        glue.execute(mock.MagicMock())

        mock_inject_parent.assert_not_called()

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch(
        "airflow.providers.amazon.aws.operators.glue.inject_transport_information_into_glue_arguments"
    )
    def test_inject_transport_info_called_when_enabled(
        self, mock_inject_transport, mock_initialize_job, mock_get_conn
    ):
        mock_inject_transport.side_effect = lambda args, ctx: {
            **args,
            "--conf": "spark.openlineage.transport.type=http",
        }
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            iam_role_name="my_test_role",
            wait_for_completion=False,
            openlineage_inject_transport_info=True,
        )
        context = mock.MagicMock()
        glue.execute(context)

        mock_inject_transport.assert_called_once()
        call_args = mock_initialize_job.call_args[0][0]
        assert "--conf" in call_args
        assert "spark.openlineage.transport.type=http" in call_args["--conf"]

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch(
        "airflow.providers.amazon.aws.operators.glue.inject_parent_job_information_into_glue_arguments"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.operators.glue.inject_transport_information_into_glue_arguments"
    )
    def test_inject_both_parent_and_transport_info(
        self, mock_inject_transport, mock_inject_parent, mock_initialize_job, mock_get_conn
    ):
        mock_inject_parent.side_effect = lambda args, ctx: {
            **args,
            "--conf": "spark.openlineage.parentJobNamespace=ns",
        }
        mock_inject_transport.side_effect = lambda args, ctx: {
            **args,
            "--conf": args.get("--conf", "") + " --conf spark.openlineage.transport.type=http",
        }
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            iam_role_name="my_test_role",
            wait_for_completion=False,
            openlineage_inject_parent_job_info=True,
            openlineage_inject_transport_info=True,
        )
        glue.execute(mock.MagicMock())

        mock_inject_parent.assert_called_once()
        mock_inject_transport.assert_called_once()
        call_args = mock_initialize_job.call_args[0][0]
        assert "spark.openlineage.parentJobNamespace=ns" in call_args["--conf"]
        assert "spark.openlineage.transport.type=http" in call_args["--conf"]

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch(
        "airflow.providers.amazon.aws.operators.glue.inject_parent_job_information_into_glue_arguments"
    )
    def test_inject_parent_job_info_preserves_existing_script_args(
        self, mock_inject_parent, mock_initialize_job, mock_get_conn
    ):
        mock_inject_parent.side_effect = lambda args, ctx: {
            **args,
            "--conf": "spark.openlineage.parentJobNamespace=ns",
        }
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        glue = GlueJobOperator(
            durable=False,
            task_id=TASK_ID,
            job_name=JOB_NAME,
            script_location="s3://folder/file",
            iam_role_name="my_test_role",
            wait_for_completion=False,
            openlineage_inject_parent_job_info=True,
            script_args={"--my-arg": "my-value"},
        )
        glue.execute(mock.MagicMock())

        call_args = mock_initialize_job.call_args[0][0]
        assert call_args["--my-arg"] == "my-value"
        assert "--conf" in call_args

    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch(
        "airflow.providers.amazon.aws.operators.glue.inject_parent_job_information_into_glue_arguments"
    )
    def test_inject_parent_job_info_with_durable_scan(
        self, mock_inject_parent, mock_initialize_job, mock_get_conn
    ):
        """OL injection is applied before task UUID is added; both end up in the args passed to initialize_job."""
        mock_inject_parent.side_effect = lambda args, ctx: {
            **args,
            "--conf": "spark.openlineage.parentJobNamespace=ns",
        }
        mock_initialize_job.return_value = {"JobRunState": "RUNNING", "JobRunId": JOB_RUN_ID}

        with mock.patch("airflow.providers.amazon.aws.operators.glue.AIRFLOW_V_3_3_PLUS", False):
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=f"^{re.escape(DEPRECATION_MESSAGE_PRE_3_3)}$"
            ):
                glue = GlueJobOperator(
                    task_id=TASK_ID,
                    job_name=JOB_NAME,
                    script_location="s3://folder/file",
                    iam_role_name="my_test_role",
                    wait_for_completion=False,
                    openlineage_inject_parent_job_info=True,
                    resume_glue_job_on_retry=True,
                )

            mock_ti = mock.MagicMock()
            mock_ti.try_number = 2
            mock_ti.xcom_pull.return_value = None
            context = {"ti": mock_ti}
            mock_glue_client = mock.MagicMock()
            glue.hook.conn = mock_glue_client
            mock_glue_client.get_job_runs.return_value = {"JobRuns": []}
            glue.execute(context)

        mock_inject_parent.assert_called_once()
        # The injected OL arg and the task UUID arg should both be present
        call_args = mock_initialize_job.call_args[0][0]
        assert "--conf" in call_args
        assert GlueJobOperator.TASK_UUID_ARG in call_args


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _warn_and_disable_durable_pre_3_3(_DURABLE_UNSET)
        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value):
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result = _warn_and_disable_durable_pre_3_3(value)
        assert result is False


class TestGlueJobOperatorDeprecation:
    @pytest.mark.parametrize("resume_value", [True, False])
    def test_warns_and_maps_to_durable_old_flag(self, resume_value):
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=f"^{re.escape(EXPECTED_DEPRECATION_MESSAGE)}$"
        ):
            glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME, resume_glue_job_on_retry=resume_value)
        assert glue.durable is resume_value

    def test_warns_on_every_supported_airflow_version(self):
        with mock.patch("airflow.providers.amazon.aws.operators.glue.AIRFLOW_V_3_3_PLUS", False):
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=f"^{re.escape(DEPRECATION_MESSAGE_PRE_3_3)}$"
            ):
                GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME, resume_glue_job_on_retry=True)

    def test_default_args_durable_reaches_operator(self):
        with DAG(
            dag_id="test_glue_durable_default_args",
            schedule=None,
            start_date=datetime(2024, 1, 1),
            default_args={"durable": False},
        ):
            glue = GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME)
        assert glue.durable is False

    def test_legacy_flag_wins_over_conflicting_durable_below_3_3(self):
        with mock.patch("airflow.providers.amazon.aws.operators.glue.AIRFLOW_V_3_3_PLUS", False):
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=f"^{re.escape(DEPRECATION_MESSAGE_PRE_3_3)}$"
            ):
                glue = GlueJobOperator(
                    task_id=TASK_ID,
                    job_name=JOB_NAME,
                    durable=False,
                    resume_glue_job_on_retry=True,
                )
        # assert that glube.durable is True even though durable is set to False, because resume_glue_job_on_retry takes precedence in this case.
        assert glue.durable is True


class FakeTaskStateStore:
    """In-memory task state store for tests."""

    def __init__(self, stored: dict[str, str] | None = None):
        self._store: dict[str, str] = dict(stored or {})

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def set(self, key: str, value: str) -> None:
        self._store[key] = value


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS,
    reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
)
class TestGlueJobOperatorDurableExecution:
    def _build(self, **kwargs):
        return GlueJobOperator(task_id=TASK_ID, job_name=JOB_NAME, **kwargs)

    def _stub_empty_scan(self, glue):
        # submit_job scans for a task UUID tagged run whenever durable is set, regardless of why it
        # was called. Stub it to return no matches so tests don't depend on that fallback mechanism.
        glue.hook.conn = mock.MagicMock()
        glue.hook.conn.get_job_runs.return_value = {"JobRuns": []}

    def _context(self, store=None, try_number=2):
        ti = mock.MagicMock()
        ti.try_number = try_number
        ti.xcom_pull.return_value = None
        ctx = {"ti": ti}
        if store is not None:
            ctx["task_state_store"] = store
        return ctx

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_fresh_submit_persists_before_polling(
        self, mock_get_conn, mock_initialize_job, mock_job_completion
    ):
        glue = self._build(durable=True)
        self._stub_empty_scan(glue)
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        store = FakeTaskStateStore()
        persisted_before_poll = []
        mock_job_completion.side_effect = lambda *a, **k: (
            persisted_before_poll.append(store.get("glue_job_run_id")) or {"JobRunState": "SUCCEEDED"}
        )

        job_run_id = glue.execute(self._context(store))

        assert job_run_id == "jr_new"
        assert store.get("glue_job_run_id") == "jr_new"
        assert persisted_before_poll == ["jr_new"]
        mock_initialize_job.assert_called_once()

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_reconnect_when_stored_run_is_running(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion
    ):
        glue = self._build(durable=True)
        mock_get_job_state.return_value = "RUNNING"
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})
        context = self._context(store)

        job_run_id = glue.execute(context)

        assert job_run_id == "jr_old"
        mock_initialize_job.assert_not_called()
        mock_job_completion.assert_called_once_with(JOB_NAME, "jr_old", False, 0)
        context["ti"].xcom_push.assert_any_call(key="glue_job_run_id", value="jr_old")

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_legacy_flag_still_warns_and_reconnects_like_durable(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion
    ):
        """resume_glue_job_on_retry warns on 3.3+, but durable execution behaves identically to durable=True."""
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=f"^{re.escape(DEPRECATION_MESSAGE_3_3_PLUS)}$"
        ):
            glue = self._build(resume_glue_job_on_retry=True)
        mock_get_job_state.return_value = "RUNNING"
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        job_run_id = glue.execute(self._context(store))

        assert job_run_id == "jr_old"
        mock_initialize_job.assert_not_called()

    @pytest.mark.parametrize("status", ["STARTING", "RUNNING", "WAITING", "STOPPING"])
    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_reconnect_from_every_active_state(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion, status
    ):
        glue = self._build(durable=True)
        mock_get_job_state.return_value = status
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        glue.execute(self._context(store))

        mock_initialize_job.assert_not_called()

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_reconnect_to_stopping_run_that_settles_stopped_raises(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion
    ):
        glue = self._build(durable=True)
        mock_get_job_state.return_value = "STOPPING"
        mock_job_completion.return_value = {"JobRunState": "STOPPED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        with pytest.raises(GlueJobRunStoppedError, match="jr_old"):
            glue.execute(self._context(store))

        mock_initialize_job.assert_not_called()

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_already_succeeded_returns_without_resubmit(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion
    ):
        glue = self._build(durable=True)
        mock_get_job_state.return_value = "SUCCEEDED"
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})
        context = self._context(store)

        job_run_id = glue.execute(context)

        assert job_run_id == "jr_old"
        mock_initialize_job.assert_not_called()
        mock_job_completion.assert_not_called()
        context["ti"].xcom_push.assert_any_call(key="glue_job_run_id", value="jr_old")

    @pytest.mark.parametrize("status", ["FAILED", "TIMEOUT", "STOPPED"])
    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_terminal_failure_resubmits_fresh(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion, status
    ):
        glue = self._build(durable=True)
        self._stub_empty_scan(glue)
        mock_get_job_state.return_value = status
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        job_run_id = glue.execute(self._context(store))

        assert job_run_id == "jr_new"
        assert store.get("glue_job_run_id") == "jr_new"
        mock_initialize_job.assert_called_once()

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_terminal_resubmit_skips_the_scan_when_store_already_had_an_id(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion
    ):
        glue = self._build(durable=True)
        glue.hook.conn = mock.MagicMock()
        mock_get_job_state.return_value = "FAILED"
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        job_run_id = glue.execute(self._context(store))

        assert job_run_id == "jr_new"
        glue.hook.conn.get_job_runs.assert_not_called()
        mock_initialize_job.assert_called_once()

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_terminal_resubmit_clears_stale_id_if_initialize_job_fails(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state
    ):
        glue = self._build(durable=True, stop_job_run_on_kill=True)
        glue.hook.conn = mock.MagicMock()
        mock_get_job_state.return_value = "FAILED"
        mock_initialize_job.side_effect = ClientError(
            {"Error": {"Code": "Throttling", "Message": "slow down"}}, "StartJobRun"
        )
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        with pytest.raises(ClientError):
            glue.execute(self._context(store))

        assert glue._job_run_id is None
        glue.on_kill()
        glue.hook.conn.batch_stop_job_run.assert_not_called()

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_not_found_resubmits_fresh(
        self, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_job_completion
    ):
        glue = self._build(durable=True)
        self._stub_empty_scan(glue)
        mock_get_job_state.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "gone"}}, "GetJobRun"
        )
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        job_run_id = glue.execute(self._context(store))

        assert job_run_id == "jr_new"
        mock_initialize_job.assert_called_once()

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_durable_false_never_touches_store(self, mock_get_conn, mock_initialize_job, mock_job_completion):
        glue = self._build(durable=False)
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore({"glue_job_run_id": "jr_old"})

        job_run_id = glue.execute(self._context(store))

        assert job_run_id == "jr_new"
        assert store.get("glue_job_run_id") == "jr_old", "store must be left untouched"
        mock_initialize_job.assert_called_once()

    @mock.patch.object(GlueJobHook, "conn", new_callable=mock.PropertyMock)
    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_first_attempt_skips_the_retry_lookup_entirely(
        self, mock_get_conn, mock_initialize_job, mock_job_completion, mock_conn
    ):
        glue = self._build(durable=True)
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore()

        job_run_id = glue.execute(self._context(store, try_number=1))

        assert job_run_id == "jr_new"
        mock_initialize_job.assert_called_once()
        # No tag needed on 3.3+ synchronous runs: task_state_store is the sole reconnect mechanism.
        assert GlueJobOperator.TASK_UUID_ARG not in mock_initialize_job.call_args[0][0]
        mock_conn.return_value.get_job_run.assert_not_called()
        mock_conn.return_value.get_job_runs.assert_not_called()

    @mock.patch.object(GlueJobHook, "conn", new_callable=mock.PropertyMock)
    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_retry_on_3_3_plus_sync_never_scans_or_tags(
        self, mock_get_conn, mock_initialize_job, mock_job_completion, mock_conn
    ):
        glue = self._build(durable=True)
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        mock_job_completion.return_value = {"JobRunState": "SUCCEEDED"}
        store = FakeTaskStateStore()

        job_run_id = glue.execute(self._context(store, try_number=3))

        assert job_run_id == "jr_new"
        assert GlueJobOperator.TASK_UUID_ARG not in mock_initialize_job.call_args[0][0]
        mock_conn.return_value.get_job_run.assert_not_called()
        mock_conn.return_value.get_job_runs.assert_not_called()

    @mock.patch.object(GlueJobHook, "job_completion")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    def test_wait_for_completion_false_still_persists_immediately(
        self, mock_get_conn, mock_initialize_job, mock_job_completion
    ):
        glue = self._build(durable=True, wait_for_completion=False)
        self._stub_empty_scan(glue)
        mock_initialize_job.return_value = {"JobRunId": "jr_new"}
        store = FakeTaskStateStore()

        job_run_id = glue.execute(self._context(store))

        assert job_run_id == "jr_new"
        assert store.get("glue_job_run_id") == "jr_new"
        mock_job_completion.assert_not_called()

    @pytest.mark.parametrize(
        ("status", "expected_active"),
        [
            ("STARTING", True),
            ("RUNNING", True),
            ("WAITING", True),
            ("STOPPING", True),
            ("SUCCEEDED", False),
            ("STOPPED", False),
            ("FAILED", False),
            ("TIMEOUT", False),
            ("ERROR", False),
            ("EXPIRED", False),
            ("NOT_FOUND", False),
            ("SOME_FUTURE_STATE", True),
        ],
    )
    def test_is_job_active(self, status, expected_active):
        glue = self._build()
        assert glue.is_job_active(status) is expected_active

    @pytest.mark.parametrize(
        ("status", "expected_succeeded"),
        [
            ("SUCCEEDED", True),
            ("STOPPED", False),
            ("RUNNING", False),
            ("FAILED", False),
        ],
    )
    def test_is_job_succeeded(self, status, expected_succeeded):
        glue = self._build()
        assert glue.is_job_succeeded(status) is expected_succeeded


class TestGlueDataQualityOperator:
    RULE_SET_NAME = "TestRuleSet"
    RULE_SET = 'Rules=[ColumnLength "review_id" = 15]'
    TARGET_TABLE = {"TableName": "TestTable", "DatabaseName": "TestDB"}

    @pytest.fixture
    def glue_data_quality_hook(self) -> Generator[GlueDataQualityHook, None, None]:
        with mock_aws():
            hook = GlueDataQualityHook(aws_conn_id="aws_default")
            yield hook

    def test_init(self):
        self.operator = GlueDataQualityOperator(
            task_id="create_data_quality_ruleset", name=self.RULE_SET_NAME, ruleset=self.RULE_SET
        )
        self.operator.defer = mock.MagicMock()

        assert self.operator.name == self.RULE_SET_NAME
        assert self.operator.ruleset == self.RULE_SET

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_execute_create_rule(self, glue_data_quality_mock_conn):
        self.operator = GlueDataQualityOperator(
            task_id="create_data_quality_ruleset",
            name=self.RULE_SET_NAME,
            ruleset=self.RULE_SET,
            description="create ruleset",
        )
        self.operator.defer = mock.MagicMock()

        self.operator.execute({})
        glue_data_quality_mock_conn.create_data_quality_ruleset.assert_called_once_with(
            Description="create ruleset",
            Name=self.RULE_SET_NAME,
            Ruleset=self.RULE_SET,
        )

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_execute_strips_rendered_ruleset(self, glue_data_quality_mock_conn):
        # ruleset is a template field; execute strips the rendered value (rendering can add whitespace).
        with DAG("glue_dq_strip", schedule=None, start_date=datetime(2020, 1, 1)) as dag:
            self.operator = GlueDataQualityOperator(
                task_id="create_data_quality_ruleset",
                name=self.RULE_SET_NAME,
                ruleset="{{ params.rules }}",
                dag=dag,
            )
        self.operator.defer = mock.MagicMock()
        self.operator.render_template_fields({"params": {"rules": f"  {self.RULE_SET}  "}})
        assert self.operator.ruleset == f"  {self.RULE_SET}  "

        self.operator.execute({})

        glue_data_quality_mock_conn.create_data_quality_ruleset.assert_called_once_with(
            Description="AWS Glue Data Quality Rule Set With Airflow",
            Name=self.RULE_SET_NAME,
            Ruleset=self.RULE_SET,
        )

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_execute_create_rule_should_fail_if_rule_already_exists(self, glue_data_quality_mock_conn):
        self.operator = GlueDataQualityOperator(
            task_id="create_data_quality_ruleset",
            name=self.RULE_SET_NAME,
            ruleset=self.RULE_SET,
            description="create ruleset",
        )
        self.operator.defer = mock.MagicMock()
        error_message = f"Another ruleset with the same name already exists: {self.RULE_SET_NAME}"

        err_response = {"Error": {"Code": "AlreadyExistsException", "Message": error_message}}

        exception = client("glue").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        glue_data_quality_mock_conn.exceptions.AlreadyExistsException = returned_exception
        glue_data_quality_mock_conn.create_data_quality_ruleset.side_effect = exception

        with pytest.raises(AirflowException, match=error_message):
            self.operator.execute({})

        glue_data_quality_mock_conn.create_data_quality_ruleset.assert_called_once_with(
            Description="create ruleset",
            Name=self.RULE_SET_NAME,
            Ruleset=self.RULE_SET,
        )

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_execute_update_rule(self, glue_data_quality_mock_conn):
        self.operator = GlueDataQualityOperator(
            task_id="update_data_quality_ruleset",
            name=self.RULE_SET_NAME,
            ruleset=self.RULE_SET,
            description="update ruleset",
            update_rule_set=True,
        )
        self.operator.defer = mock.MagicMock()

        self.operator.execute({})
        glue_data_quality_mock_conn.update_data_quality_ruleset.assert_called_once_with(
            Description="update ruleset", Name=self.RULE_SET_NAME, Ruleset=self.RULE_SET
        )

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_execute_update_rule_should_fail_if_rule_not_exists(self, glue_data_quality_mock_conn):
        self.operator = GlueDataQualityOperator(
            task_id="update_data_quality_ruleset",
            name=self.RULE_SET_NAME,
            ruleset=self.RULE_SET,
            description="update ruleset",
            update_rule_set=True,
        )
        self.operator.defer = mock.MagicMock()
        error_message = f"Cannot find Data Quality Ruleset in account 1234567 with name {self.RULE_SET_NAME}"

        err_response = {"Error": {"Code": "EntityNotFoundException", "Message": error_message}}

        exception = client("glue").exceptions.ClientError(err_response, "test")
        returned_exception = type(exception)

        glue_data_quality_mock_conn.exceptions.EntityNotFoundException = returned_exception
        glue_data_quality_mock_conn.update_data_quality_ruleset.side_effect = exception

        with pytest.raises(AirflowException, match=error_message):
            self.operator.execute({})

        glue_data_quality_mock_conn.update_data_quality_ruleset.assert_called_once_with(
            Description="update ruleset", Name=self.RULE_SET_NAME, Ruleset=self.RULE_SET
        )

    def test_validate_inputs(self):
        self.operator = GlueDataQualityOperator(
            task_id="create_data_quality_ruleset",
            name=self.RULE_SET_NAME,
            ruleset=self.RULE_SET,
        )

        assert self.operator.validate_inputs() is None

    def test_validate_inputs_error(self):
        self.operator = GlueDataQualityOperator(
            task_id="create_data_quality_ruleset",
            name=self.RULE_SET_NAME,
            ruleset='[ColumnLength "review_id" = 15]',
        )

        with pytest.raises(AttributeError, match="RuleSet must starts with Rules = \\[ and ends with \\]"):
            self.operator.validate_inputs()

    def test_template_fields(self):
        operator = GlueDataQualityOperator(
            task_id="create_data_quality_ruleset", name=self.RULE_SET_NAME, ruleset=self.RULE_SET
        )
        validate_template_fields(operator)

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = GlueDataQualityOperator(
            task_id="test_overwritten_conn_passed_to_hook",
            name=self.RULE_SET_NAME,
            ruleset=self.RULE_SET,
            aws_conn_id=OVERWRITTEN_CONN,
        )
        assert op.hook.aws_conn_id == OVERWRITTEN_CONN

    def test_default_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = GlueDataQualityOperator(
            task_id="test_default_conn_passed_to_hook", name=self.RULE_SET_NAME, ruleset=self.RULE_SET
        )
        assert op.hook.aws_conn_id == DEFAULT_CONN


class TestGlueDataQualityRuleSetEvaluationRunOperator:
    RUN_ID = "1234567890"
    DATA_SOURCE = {"GlueTable": {"DatabaseName": "TestDB", "TableName": "TestTable"}}
    ROLE = "role_arn"
    RULE_SET_NAMES = ["TestRuleSet"]

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(GlueDataQualityHook, "conn") as _conn:
            _conn.start_data_quality_ruleset_evaluation_run.return_value = {"RunId": self.RUN_ID}
            yield _conn

    @pytest.fixture
    def glue_data_quality_hook(self) -> Generator[GlueDataQualityHook, None, None]:
        with mock_aws():
            hook = GlueDataQualityHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = GlueDataQualityRuleSetEvaluationRunOperator(
            task_id="stat_evaluation_run",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            rule_set_names=self.RULE_SET_NAMES,
            show_results=False,
        )
        self.operator.defer = mock.MagicMock()

    def test_init(self):
        assert self.operator.datasource == self.DATA_SOURCE
        assert self.operator.role == self.ROLE
        assert self.operator.rule_set_names == self.RULE_SET_NAMES

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_start_data_quality_ruleset_evaluation_run(self, glue_data_quality_mock_conn):
        glue_data_quality_mock_conn.get_data_quality_ruleset.return_value = {"Name": "TestRuleSet"}

        self.op = GlueDataQualityRuleSetEvaluationRunOperator(
            task_id="stat_evaluation_run",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            number_of_workers=10,
            timeout=1000,
            rule_set_names=self.RULE_SET_NAMES,
            rule_set_evaluation_run_kwargs={"AdditionalRunOptions": {"CloudWatchMetricsEnabled": True}},
        )

        self.op.wait_for_completion = False
        self.op.execute({})

        glue_data_quality_mock_conn.start_data_quality_ruleset_evaluation_run.assert_called_once_with(
            DataSource=self.DATA_SOURCE,
            Role=self.ROLE,
            NumberOfWorkers=10,
            Timeout=1000,
            RulesetNames=self.RULE_SET_NAMES,
            AdditionalRunOptions={"CloudWatchMetricsEnabled": True},
        )

    def test_validate_inputs(self, mock_conn):
        mock_conn.get_data_quality_ruleset.return_value = {"Name": "TestRuleSet"}
        assert self.operator.validate_inputs() is None

    def test_validate_inputs_error(self, mock_conn):
        class RuleSetNotFoundException(Exception):
            pass

        mock_conn.exceptions.EntityNotFoundException = RuleSetNotFoundException
        mock_conn.get_data_quality_ruleset.side_effect = RuleSetNotFoundException()

        self.operator = GlueDataQualityRuleSetEvaluationRunOperator(
            task_id="stat_evaluation_run",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            rule_set_names=["dummy"],
        )

        with pytest.raises(AirflowException, match="Following RulesetNames are not found \\['dummy'\\]"):
            self.operator.validate_inputs()

    @pytest.mark.parametrize(
        ("wait_for_completion", "deferrable"),
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(GlueDataQualityHook, "get_waiter")
    def test_start_data_quality_ruleset_evaluation_run_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, glue_data_quality_hook
    ):
        mock_conn.get_data_quality_ruleset.return_value = {"Name": "TestRuleSet"}
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.RUN_ID
        assert glue_data_quality_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    def test_template_fields(self):
        validate_template_fields(self.operator)

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = GlueDataQualityRuleSetEvaluationRunOperator(
            task_id="test_overwritten_conn_passed_to_hook",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            rule_set_names=self.RULE_SET_NAMES,
            show_results=False,
            aws_conn_id=OVERWRITTEN_CONN,
        )
        assert op.hook.aws_conn_id == OVERWRITTEN_CONN

    def test_default_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = GlueDataQualityRuleSetEvaluationRunOperator(
            task_id="test_default_conn_passed_to_hook",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            rule_set_names=self.RULE_SET_NAMES,
            show_results=False,
        )
        assert op.hook.aws_conn_id == DEFAULT_CONN


class TestGlueDataQualityRuleRecommendationRunOperator:
    RUN_ID = "1234567890"
    DATA_SOURCE = {"GlueTable": {"DatabaseName": "TestDB", "TableName": "TestTable"}}
    ROLE = "role_arn"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(GlueDataQualityHook, "conn") as _conn:
            _conn.start_data_quality_rule_recommendation_run.return_value = {"RunId": self.RUN_ID}
            yield _conn

    @pytest.fixture
    def glue_data_quality_hook(self) -> Generator[GlueDataQualityHook, None, None]:
        with mock_aws():
            hook = GlueDataQualityHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = GlueDataQualityRuleRecommendationRunOperator(
            task_id="start_recommendation_run",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            show_results=False,
            recommendation_run_kwargs={"CreatedRulesetName": "test-ruleset"},
        )
        self.operator.defer = mock.MagicMock()

    def test_init(self):
        assert self.operator.datasource == self.DATA_SOURCE
        assert self.operator.role == self.ROLE
        assert self.operator.show_results is False
        assert self.operator.recommendation_run_kwargs == {"CreatedRulesetName": "test-ruleset"}

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_start_data_quality_rule_recommendation_run(self, glue_data_quality_mock_conn):
        self.op = GlueDataQualityRuleRecommendationRunOperator(
            task_id="start_recommendation_run",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            number_of_workers=10,
            timeout=1000,
            recommendation_run_kwargs={"CreatedRulesetName": "test-ruleset"},
        )

        self.op.wait_for_completion = False
        self.op.execute({})

        glue_data_quality_mock_conn.start_data_quality_rule_recommendation_run.assert_called_once_with(
            DataSource=self.DATA_SOURCE,
            Role=self.ROLE,
            NumberOfWorkers=10,
            Timeout=1000,
            CreatedRulesetName="test-ruleset",
        )

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_start_data_quality_rule_recommendation_run_failed(self, glue_data_quality_mock_conn):
        created_ruleset_name = "test-ruleset"
        error_message = f"Ruleset {created_ruleset_name} already exists"

        err_response = {"Error": {"Code": "InvalidInputException", "Message": error_message}}

        exception = boto3.client("glue").exceptions.ClientError(
            err_response, "StartDataQualityRuleRecommendationRun"
        )
        returned_exception = type(exception)

        glue_data_quality_mock_conn.exceptions.InvalidInputException = returned_exception
        glue_data_quality_mock_conn.start_data_quality_rule_recommendation_run.side_effect = exception

        operator = GlueDataQualityRuleRecommendationRunOperator(
            task_id="stat_recommendation_run",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            recommendation_run_kwargs={"CreatedRulesetName": created_ruleset_name},
        )
        operator.wait_for_completion = False

        with pytest.raises(
            AirflowException,
            match=f"AWS Glue data quality recommendation run failed: Ruleset {created_ruleset_name} already exists",
        ):
            operator.execute({})

    @pytest.mark.parametrize(
        ("wait_for_completion", "deferrable"),
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(GlueDataQualityHook, "get_waiter")
    def test_start_data_quality_rule_recommendation_run_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, glue_data_quality_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.RUN_ID
        assert glue_data_quality_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    def test_template_fields(self):
        validate_template_fields(self.operator)

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = GlueDataQualityRuleRecommendationRunOperator(
            task_id="test_overwritten_conn_passed_to_hook",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            number_of_workers=10,
            timeout=1000,
            recommendation_run_kwargs={"CreatedRulesetName": "test-ruleset"},
            aws_conn_id=OVERWRITTEN_CONN,
        )
        assert op.hook.aws_conn_id == OVERWRITTEN_CONN

    def test_default_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = GlueDataQualityRuleRecommendationRunOperator(
            task_id="test_default_conn_passed_to_hook",
            datasource=self.DATA_SOURCE,
            role=self.ROLE,
            number_of_workers=10,
            timeout=1000,
            recommendation_run_kwargs={"CreatedRulesetName": "test-ruleset"},
        )
        assert op.hook.aws_conn_id == DEFAULT_CONN
