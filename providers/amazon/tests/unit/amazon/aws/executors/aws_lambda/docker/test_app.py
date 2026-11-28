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

import pytest

from airflow.providers.amazon.aws.executors.aws_lambda.docker import app


class TestGetQueueUrl:
    def test_prefers_airflow_queue_url_env_var(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL",
            "https://sqs.us-east-1.amazonaws.com/123456789012/airflow-queue",
        )
        monkeypatch.setenv(
            "QUEUE_URL",
            "https://sqs.us-east-1.amazonaws.com/123456789012/fallback-queue",
        )
        assert app.get_queue_url() == "https://sqs.us-east-1.amazonaws.com/123456789012/airflow-queue"

    def test_falls_back_to_queue_url_env_var(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL", raising=False)
        monkeypatch.setenv(
            "QUEUE_URL",
            "https://sqs.us-east-1.amazonaws.com/123456789012/fallback-queue",
        )
        assert app.get_queue_url() == "https://sqs.us-east-1.amazonaws.com/123456789012/fallback-queue"

    def test_raises_runtime_error_when_no_queue_url_set(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL", raising=False)
        monkeypatch.delenv("QUEUE_URL", raising=False)
        with pytest.raises(RuntimeError, match="No Queue URL detected"):
            app.get_queue_url()


class TestGetSqsClient:
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.boto3.client")
    def test_creates_boto3_sqs_client(self, mock_boto3_client):
        mock_client = mock.MagicMock()
        mock_boto3_client.return_value = mock_client

        client = app.get_sqs_client()

        mock_boto3_client.assert_called_once_with("sqs")
        assert client == mock_client


class TestFetchDagsFromS3:
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.mkdtemp")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.boto3.resource")
    def test_downloads_dags_and_skips_directories(
        self, mock_boto3_resource, mock_mkdtemp, monkeypatch, tmp_path
    ):
        mock_mkdtemp.return_value = str(tmp_path)
        monkeypatch.setenv("AIRFLOW__CORE__DAGS_FOLDER", "/initial/dags/folder")

        mock_s3 = mock.MagicMock()
        mock_bucket = mock.MagicMock()
        mock_boto3_resource.return_value = mock_s3
        mock_s3.Bucket.return_value = mock_bucket

        obj_file = mock.MagicMock()
        obj_file.key = "dags/my_dag.py"

        obj_dir = mock.MagicMock()
        obj_dir.key = "dags/subdir/"

        mock_bucket.objects.filter.return_value = [obj_file, obj_dir]

        app.fetch_dags_from_s3("s3://test-bucket/dags/")

        mock_mkdtemp.assert_called_once_with(prefix="airflow_dags_")
        mock_s3.Bucket.assert_called_once_with("test-bucket")
        mock_bucket.objects.filter.assert_called_once_with(Prefix="dags/")
        mock_bucket.download_file.assert_called_once_with("dags/my_dag.py", str(tmp_path / "my_dag.py"))
        assert app.os.environ["AIRFLOW__CORE__DAGS_FOLDER"] == str(tmp_path)


class TestRunAndReport:
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.get_sqs_client")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.get_queue_url")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.subprocess.run")
    def test_successful_execution_sends_return_code_zero(
        self, mock_subprocess_run, mock_get_queue_url, mock_get_sqs_client
    ):
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = b"Task completed successfully"
        mock_subprocess_run.return_value = mock_proc

        mock_get_queue_url.return_value = "https://sqs.us-east-1.amazonaws.com/123/queue"
        mock_sqs = mock.MagicMock()
        mock_get_sqs_client.return_value = mock_sqs

        app.run_and_report(["airflow", "tasks", "run"], "test_task_key")

        mock_subprocess_run.assert_called_once_with(
            ["airflow", "tasks", "run"],
            check=False,
            shell=False,
            stdout=app.subprocess.PIPE,
            stderr=app.subprocess.STDOUT,
        )
        mock_sqs.send_message.assert_called_once()
        sent_body = json.loads(mock_sqs.send_message.call_args[1]["MessageBody"])
        assert sent_body == {"task_key": "test_task_key", "return_code": 0}

    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.get_sqs_client")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.get_queue_url")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.subprocess.run")
    def test_subprocess_exception_reports_return_code_one(
        self, mock_subprocess_run, mock_get_queue_url, mock_get_sqs_client
    ):
        mock_subprocess_run.side_effect = OSError("Command not found")
        mock_get_queue_url.return_value = "https://sqs.us-east-1.amazonaws.com/123/queue"
        mock_sqs = mock.MagicMock()
        mock_get_sqs_client.return_value = mock_sqs

        app.run_and_report("echo test", "task_fail_key")

        mock_subprocess_run.assert_called_once_with(
            "echo test",
            check=False,
            shell=True,
            stdout=app.subprocess.PIPE,
            stderr=app.subprocess.STDOUT,
        )
        mock_sqs.send_message.assert_called_once()
        sent_body = json.loads(mock_sqs.send_message.call_args[1]["MessageBody"])
        assert sent_body == {"task_key": "task_fail_key", "return_code": 1}

    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.get_sqs_client")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.get_queue_url")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.subprocess.run")
    def test_sqs_send_failure_handled_gracefully(
        self, mock_subprocess_run, mock_get_queue_url, mock_get_sqs_client
    ):
        mock_proc = mock.MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = b"Done"
        mock_subprocess_run.return_value = mock_proc

        mock_get_queue_url.return_value = "https://sqs.us-east-1.amazonaws.com/123/queue"
        mock_sqs = mock.MagicMock()
        mock_sqs.send_message.side_effect = RuntimeError("SQS unavailable")
        mock_get_sqs_client.return_value = mock_sqs

        # Should log exception and not raise
        app.run_and_report(["airflow", "version"], "task_sqs_fail")
        mock_sqs.send_message.assert_called_once()


class TestLambdaHandler:
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.run_and_report")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.fetch_dags_from_s3")
    def test_lambda_handler_calls_fetch_dags_when_s3_uri_configured(
        self, mock_fetch_dags, mock_run_and_report, monkeypatch
    ):
        monkeypatch.setattr(app, "S3_URI", "s3://my-dags-bucket/airflow/")
        event = {
            app.COMMAND_KEY: ["airflow", "tasks", "run"],
            app.TASK_KEY_KEY: "sample_task",
            app.EXECUTOR_CONFIG_KEY: {},
        }

        app.lambda_handler(event, {})

        mock_fetch_dags.assert_called_once_with("s3://my-dags-bucket/airflow/")
        mock_run_and_report.assert_called_once_with(["airflow", "tasks", "run"], "sample_task")

    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.run_and_report")
    @mock.patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.fetch_dags_from_s3")
    def test_lambda_handler_skips_fetch_dags_when_no_s3_uri(
        self, mock_fetch_dags, mock_run_and_report, monkeypatch
    ):
        monkeypatch.setattr(app, "S3_URI", None)
        event = {
            app.COMMAND_KEY: "echo hello",
            app.TASK_KEY_KEY: "sample_task_no_s3",
        }

        app.lambda_handler(event, {})

        mock_fetch_dags.assert_not_called()
        mock_run_and_report.assert_called_once_with("echo hello", "sample_task_no_s3")
