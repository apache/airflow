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

from collections.abc import Generator
from typing import TYPE_CHECKING
from unittest import mock

import boto3
import pytest
from boto3 import client
from moto import mock_aws

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.links.glue import GlueJobRunDetailsLink
from airflow.providers.amazon.aws.operators.glue import (
    GlueDataQualityOperator,
    GlueDataQualityRuleRecommendationRunOperator,
    GlueDataQualityRuleSetEvaluationRunOperator,
    GlueJobOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from airflow.models import TaskInstance
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection

TASK_ID = "test_glue_operator"
DAG_ID = "test_dag_id"
JOB_NAME = "test_job_name/with_slash"
JOB_RUN_ID = "11111"


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
        assert defer.value.trigger.region_name == "us-west-2"
        assert not defer.value.trigger.verbose
        assert defer.value.trigger.waiter_delay == 60
        assert defer.value.trigger.attempts == 75
        assert defer.value.trigger.aws_conn_id == "aws_default"

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
    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_execute_without_verbose_logging(
        self, mock_load_file, mock_get_conn, mock_initialize_job, mock_get_job_state, mock_print_job_logs
    ):
        glue = GlueJobOperator(
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

    @mock.patch.object(GlueJobHook, "get_job_state")
    @mock.patch.object(GlueJobHook, "initialize_job")
    @mock.patch.object(GlueJobHook, "get_conn")
    @mock.patch.object(GlueJobHook, "conn")
    @mock.patch.object(S3Hook, "load_file")
    def test_replace_script_file(
        self, mock_load_file, mock_conn, mock_get_connection, mock_initialize_job, mock_get_job_state
    ):
        glue = GlueJobOperator(
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
