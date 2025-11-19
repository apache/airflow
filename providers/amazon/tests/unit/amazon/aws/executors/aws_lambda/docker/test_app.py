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
import os
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.amazon.aws.executors.aws_lambda.docker.app import (
    COMMAND_KEY,
    EXECUTOR_CONFIG_KEY,
    RETURN_CODE_KEY,
    TASK_KEY_KEY,
    fetch_dags_from_s3,
    get_queue_url,
    get_sqs_client,
    lambda_handler,
    run_and_report,
)


class TestApp:
    """Test cases for the AWS Lambda Docker app."""

    @pytest.fixture(autouse=True)
    def setup_environment(self):
        """Setup test environment for each test."""
        with patch.dict(
            os.environ,
            {
                "AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
            },
            clear=True,
        ):
            yield

    @pytest.fixture
    def mock_context(self):
        """Create a mock Lambda context."""
        context = MagicMock()
        context.function_name = "test-function"
        context.function_version = "1"
        context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test-function"
        return context

    @pytest.fixture
    def mock_sqs_client(self):
        """Create a mock SQS client."""
        with patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.get_sqs_client") as mock:
            mock_client = MagicMock()
            mock.return_value = mock_client
            yield mock

    @pytest.fixture
    def mock_s3_resource(self):
        """Create a mock S3 resource."""
        with patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.boto3.resource") as mock:
            mock_resource = MagicMock()
            mock.return_value = mock_resource
            yield mock

    @pytest.fixture
    def mock_subprocess_run(self):
        """Create a mock subprocess run."""
        with patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.subprocess.run") as mock:
            # Create a mock result with correct attributes
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = b"Airflow version output"
            mock.return_value = mock_result
            yield mock

    @pytest.fixture
    def mock_mkdtemp(self):
        """Create a mock mkdtemp."""
        with patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.mkdtemp") as mock:
            mock.return_value = "/tmp/airflow_dags_test"
            yield mock

    def test_lambda_handler_success(self, mock_context, mock_sqs_client, mock_subprocess_run):
        """Test successful execution of lambda_handler."""
        # Setup
        event = {
            COMMAND_KEY: ["airflow", "version"],
            TASK_KEY_KEY: "test-task-key",
            EXECUTOR_CONFIG_KEY: {"test": "config"},
        }

        mock_sqs = mock_sqs_client.return_value

        # Execute
        lambda_handler(event, mock_context)

        # Assert
        mock_subprocess_run.assert_called_once()
        mock_sqs_client.assert_called_once()
        mock_sqs.send_message.assert_called_once()

        # Check the message sent to SQS
        call_args = mock_sqs.send_message.call_args
        assert call_args[1]["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        message_body = json.loads(call_args[1]["MessageBody"])
        assert message_body[TASK_KEY_KEY] == "test-task-key"
        assert message_body[RETURN_CODE_KEY] == 0

    def test_lambda_handler_with_s3_sync(
        self, mock_context, mock_sqs_client, mock_subprocess_run, mock_s3_resource, mock_mkdtemp
    ):
        """Test lambda_handler with S3 sync."""
        # Setup - We need to patch the S3_URI at the module level since it's imported at the top
        with patch(
            "airflow.providers.amazon.aws.executors.aws_lambda.docker.app.S3_URI", "s3://test-bucket/dags/"
        ):
            # Mock S3 operations
            mock_bucket = MagicMock()
            mock_s3 = mock_s3_resource.return_value
            mock_s3.Bucket.return_value = mock_bucket

            mock_obj = MagicMock()
            mock_obj.key = "dags/test_dag.py"
            mock_bucket.objects.filter.return_value = [mock_obj]

            event = {
                COMMAND_KEY: ["airflow", "version"],
                TASK_KEY_KEY: "test-task-key",
                EXECUTOR_CONFIG_KEY: {},
            }

            mock_sqs = mock_sqs_client.return_value

            # Execute
            lambda_handler(event, mock_context)

            # Assert - Check that S3 operations were called (indicating fetch_dags_from_s3 was executed)
            mock_s3_resource.assert_called_once_with("s3")
            mock_s3.Bucket.assert_called_once_with("test-bucket")
            mock_bucket.objects.filter.assert_called_once_with(Prefix="dags/")
            mock_bucket.download_file.assert_called_once_with(
                "dags/test_dag.py", "/tmp/airflow_dags_test/test_dag.py"
            )

            mock_subprocess_run.assert_called_once()
            mock_sqs_client.assert_called_once()
            mock_sqs.send_message.assert_called_once()

    def test_lambda_handler_without_s3_sync(self, mock_context, mock_sqs_client, mock_subprocess_run):
        """Test lambda_handler without S3 sync when S3_URI is not set."""
        # Setup - Ensure S3_URI is None (the default)
        # No need to patch since S3_URI defaults to None in the module
        event = {
            COMMAND_KEY: ["airflow", "version"],
            TASK_KEY_KEY: "test-task-key",
            EXECUTOR_CONFIG_KEY: {},
        }

        mock_sqs = mock_sqs_client.return_value

        # Execute
        lambda_handler(event, mock_context)

        # Assert - S3 operations should not be called
        mock_subprocess_run.assert_called_once()
        mock_sqs_client.assert_called_once()
        mock_sqs.send_message.assert_called_once()

    def test_lambda_handler_subprocess_exception(self, mock_context, mock_sqs_client):
        """Test lambda_handler when subprocess raises an exception."""
        # Setup
        event = {COMMAND_KEY: ["airflow", "version"], TASK_KEY_KEY: "test-task-key", EXECUTOR_CONFIG_KEY: {}}

        with patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.subprocess.run") as mock_run:
            # Create a mock result for the failed execution
            mock_result = MagicMock()
            mock_result.returncode = 1
            mock_run.return_value = mock_result
            mock_run.side_effect = Exception("Subprocess failed")

            mock_sqs = mock_sqs_client.return_value

            # Execute
            lambda_handler(event, mock_context)

            # Assert
            mock_run.assert_called_once()
            mock_sqs_client.assert_called_once()
            mock_sqs.send_message.assert_called_once()

            # Check that return code is 1 (failure)
            call_args = mock_sqs.send_message.call_args
            message_body = json.loads(call_args[1]["MessageBody"])
            assert message_body[RETURN_CODE_KEY] == 1

    def test_run_and_report_success(self, mock_sqs_client, mock_subprocess_run):
        """Test successful execution of run_and_report."""
        # Setup
        command = ["airflow", "version"]
        task_key = "test-task-key"

        mock_sqs = mock_sqs_client.return_value

        # Execute
        run_and_report(command, task_key)

        # Assert - Match the actual call parameters from the source code
        mock_subprocess_run.assert_called_once_with(
            command,
            check=False,
            shell=False,  # command is a list, so shell=False
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        mock_sqs_client.assert_called_once()
        mock_sqs.send_message.assert_called_once()

        # Check the message sent to SQS
        call_args = mock_sqs.send_message.call_args
        assert call_args[1]["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        message_body = json.loads(call_args[1]["MessageBody"])
        assert message_body[TASK_KEY_KEY] == task_key
        assert message_body[RETURN_CODE_KEY] == 0

    def test_run_and_report_string_command(self, mock_sqs_client):
        """Test run_and_report with string command (shell=True)."""
        # Setup
        command = "airflow version"  # String command
        task_key = "test-task-key"

        with patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.subprocess.run") as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = b"Airflow version output"
            mock_run.return_value = mock_result

            # Execute
            run_and_report(command, task_key)

            # Assert - shell should be True for string commands
            mock_run.assert_called_once_with(
                command,
                check=False,
                shell=True,  # command is a string, so shell=True
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

    def test_run_and_report_sqs_exception(self, mock_sqs_client, mock_subprocess_run, caplog):
        """Test run_and_report when SQS send_message raises an exception."""
        # Setup
        command = ["airflow", "version"]
        task_key = "test-task-key"

        mock_sqs = mock_sqs_client.return_value
        mock_sqs.send_message.side_effect = Exception("SQS failed")

        # Execute
        with caplog.at_level("ERROR"):
            run_and_report(command, task_key)

        # Assert
        mock_subprocess_run.assert_called_once()
        mock_sqs_client.assert_called_once()
        mock_sqs.send_message.assert_called_once()
        # Check that error was logged
        assert "Failed to send message to SQS" in caplog.text

    def test_get_sqs_client(self):
        """Test get_sqs_client function."""
        # Setup
        with patch("airflow.providers.amazon.aws.executors.aws_lambda.docker.app.boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client

            # Execute
            result = get_sqs_client()

            # Assert
            mock_boto.assert_called_once_with("sqs")
            assert result == mock_client

    def test_get_queue_url_from_env_var(self):
        """Test get_queue_url with AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL environment variable."""
        # Setup
        test_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        with patch.dict(os.environ, {"AIRFLOW__AWS_LAMBDA_EXECUTOR__QUEUE_URL": test_url}):
            # Execute
            result = get_queue_url()

            # Assert
            assert result == test_url

    def test_get_queue_url_from_legacy_env_var(self):
        """Test get_queue_url with legacy QUEUE_URL environment variable."""
        # Setup
        test_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        with patch.dict(os.environ, {"QUEUE_URL": test_url}):
            # Execute
            result = get_queue_url()

            # Assert
            assert result == test_url

    def test_get_queue_url_missing_env_var(self):
        """Test get_queue_url when no environment variable is set."""
        # Setup
        with patch.dict(os.environ, {}, clear=True):
            # Execute & Assert
            with pytest.raises(RuntimeError, match="No Queue URL detected"):
                get_queue_url()

    def test_fetch_dags_from_s3_success(self, mock_s3_resource, mock_mkdtemp):
        """Test successful execution of fetch_dags_from_s3."""
        # Setup
        s3_uri = "s3://test-bucket/dags/"

        mock_bucket = MagicMock()
        mock_s3 = mock_s3_resource.return_value
        mock_s3.Bucket.return_value = mock_bucket

        # Mock S3 objects
        mock_obj1 = MagicMock()
        mock_obj1.key = "dags/test_dag1.py"

        mock_obj2 = MagicMock()
        mock_obj2.key = "dags/test_dag2.py"

        mock_bucket.objects.filter.return_value = [mock_obj1, mock_obj2]

        # Execute
        fetch_dags_from_s3(s3_uri)

        # Assert
        mock_s3_resource.assert_called_once_with("s3")
        mock_s3.Bucket.assert_called_once_with("test-bucket")
        mock_bucket.objects.filter.assert_called_once_with(Prefix="dags/")
        mock_bucket.download_file.assert_any_call("dags/test_dag1.py", "/tmp/airflow_dags_test/test_dag1.py")
        mock_bucket.download_file.assert_any_call("dags/test_dag2.py", "/tmp/airflow_dags_test/test_dag2.py")

        # Check that AIRFLOW__CORE__DAGS_FOLDER was set
        assert "AIRFLOW__CORE__DAGS_FOLDER" in os.environ
        assert os.environ["AIRFLOW__CORE__DAGS_FOLDER"] == "/tmp/airflow_dags_test"

    def test_fetch_dags_from_s3_with_subdirectory(self, mock_s3_resource, mock_mkdtemp):
        """Test fetch_dags_from_s3 with subdirectories in S3."""
        # Setup
        s3_uri = "s3://test-bucket/path/to/dags/"

        mock_bucket = MagicMock()
        mock_s3 = mock_s3_resource.return_value
        mock_s3.Bucket.return_value = mock_bucket

        # Mock S3 objects including a directory
        mock_obj1 = MagicMock()
        mock_obj1.key = "path/to/dags/test_dag1.py"

        mock_obj2 = MagicMock()
        mock_obj2.key = "path/to/dags/subdir/"  # This should be skipped

        mock_obj3 = MagicMock()
        mock_obj3.key = "path/to/dags/test_dag2.py"

        mock_bucket.objects.filter.return_value = [mock_obj1, mock_obj2, mock_obj3]

        # Execute
        fetch_dags_from_s3(s3_uri)

        # Assert - Only files should be downloaded, directories skipped
        mock_s3_resource.assert_called_once_with("s3")
        mock_s3.Bucket.assert_called_once_with("test-bucket")
        mock_bucket.objects.filter.assert_called_once_with(Prefix="path/to/dags/")
        # Only files should be downloaded, directory should be skipped
        mock_bucket.download_file.assert_any_call(
            "path/to/dags/test_dag1.py", "/tmp/airflow_dags_test/test_dag1.py"
        )
        mock_bucket.download_file.assert_any_call(
            "path/to/dags/test_dag2.py", "/tmp/airflow_dags_test/test_dag2.py"
        )
        assert mock_bucket.download_file.call_count == 2

    def test_fetch_dags_from_s3_exception_handling(self, caplog):
        """Test fetch_dags_from_s3 when S3 operations raise an exception."""
        # Setup
        s3_uri = "s3://test-bucket/dags/"

        # Patch boto3.resource to raise an exception
        with patch(
            "airflow.providers.amazon.aws.executors.aws_lambda.docker.app.boto3.resource"
        ) as mock_resource:
            mock_resource.side_effect = Exception("S3 connection failed")

            # Execute - Since the actual function doesn't handle exceptions, we expect it to raise
            with pytest.raises(Exception, match="S3 connection failed"):
                fetch_dags_from_s3(s3_uri)

            # Assert
            mock_resource.assert_called_once_with("s3")

    def test_fetch_dags_from_s3_bucket_operation_exception(self, mock_s3_resource, mock_mkdtemp, caplog):
        """Test fetch_dags_from_s3 when bucket operations raise an exception."""
        # Setup
        s3_uri = "s3://test-bucket/dags/"

        mock_bucket = MagicMock()
        mock_s3 = mock_s3_resource.return_value
        mock_s3.Bucket.return_value = mock_bucket

        # Make objects.filter return objects but download_file raises exception
        mock_obj = MagicMock()
        mock_obj.key = "dags/test_dag.py"
        mock_bucket.objects.filter.return_value = [mock_obj]
        mock_bucket.download_file.side_effect = Exception("Download failed")

        # Execute - Since the actual function doesn't handle exceptions, we expect it to raise
        with pytest.raises(Exception, match="Download failed"):
            fetch_dags_from_s3(s3_uri)

        # Assert
        mock_s3_resource.assert_called_once_with("s3")
        mock_s3.Bucket.assert_called_once_with("test-bucket")
        mock_bucket.objects.filter.assert_called_once_with(Prefix="dags/")
        mock_bucket.download_file.assert_called_once_with(
            "dags/test_dag.py", "/tmp/airflow_dags_test/test_dag.py"
        )
