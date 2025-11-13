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

import datetime as dt
from unittest import mock

import pytest

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    CONFIG_GROUP_NAME,
    INVALID_CREDENTIALS_EXCEPTIONS,
    AllLambdaConfigKeys,
    InvokeLambdaKwargsConfigKeys,
    LambdaQueuedTask,
)


class TestInvokeLambdaKwargsConfigKeys:
    """Test the InvokeLambdaKwargsConfigKeys class."""

    def test_invoke_lambda_kwargs_config_keys_values(self):
        """Test that the config keys have the expected values."""
        assert InvokeLambdaKwargsConfigKeys.FUNCTION_NAME == "function_name"
        assert InvokeLambdaKwargsConfigKeys.QUALIFIER == "function_qualifier"

    def test_invoke_lambda_kwargs_config_keys_iteration(self):
        """Test that the config keys can be iterated over."""
        expected_keys = {"function_name", "function_qualifier"}
        actual_keys = set(InvokeLambdaKwargsConfigKeys())
        assert actual_keys == expected_keys


class TestAllLambdaConfigKeys:
    """Test the AllLambdaConfigKeys class."""

    def test_all_lambda_config_keys_inheritance(self):
        """Test that AllLambdaConfigKeys inherits from InvokeLambdaKwargsConfigKeys."""
        assert issubclass(AllLambdaConfigKeys, InvokeLambdaKwargsConfigKeys)

    def test_all_lambda_config_keys_values(self):
        """Test that the config keys have the expected values."""
        # Inherited from InvokeLambdaKwargsConfigKeys
        assert AllLambdaConfigKeys.FUNCTION_NAME == "function_name"
        assert AllLambdaConfigKeys.QUALIFIER == "function_qualifier"
        
        # Defined in AllLambdaConfigKeys
        assert AllLambdaConfigKeys.AWS_CONN_ID == "conn_id"
        assert AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP == "check_health_on_startup"
        assert AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS == "max_run_task_attempts"
        assert AllLambdaConfigKeys.REGION_NAME == "region_name"
        assert AllLambdaConfigKeys.QUEUE_URL == "queue_url"
        assert AllLambdaConfigKeys.DLQ_URL == "dead_letter_queue_url"
        assert AllLambdaConfigKeys.END_WAIT_TIMEOUT == "end_wait_timeout"

    def test_all_lambda_config_keys_iteration(self):
        """Test that the config keys can be iterated over."""
        expected_keys = {
            "function_name",
            "function_qualifier",
            "conn_id",
            "check_health_on_startup",
            "max_run_task_attempts",
            "region_name",
            "queue_url",
            "dead_letter_queue_url",
            "end_wait_timeout",
        }
        actual_keys = set(AllLambdaConfigKeys())
        assert actual_keys == expected_keys


class TestLambdaQueuedTask:
    """Test the LambdaQueuedTask dataclass."""

    def test_lambda_queued_task_creation(self):
        """Test that a LambdaQueuedTask can be created with the expected attributes."""
        # Create a mock TaskInstanceKey
        mock_key = mock.Mock(spec=TaskInstanceKey)
        
        # Define test values
        command = ["airflow", "tasks", "run", "dag_id", "task_id", "run_id"]
        queue = "test-queue"
        executor_config = {"test": "config"}
        attempt_number = 1
        next_attempt_time = dt.datetime.now()
        
        # Create the LambdaQueuedTask
        task = LambdaQueuedTask(
            key=mock_key,
            command=command,
            queue=queue,
            executor_config=executor_config,
            attempt_number=attempt_number,
            next_attempt_time=next_attempt_time,
        )
        
        # Verify all attributes are set correctly
        assert task.key == mock_key
        assert task.command == command
        assert task.queue == queue
        assert task.executor_config == executor_config
        assert task.attempt_number == attempt_number
        assert task.next_attempt_time == next_attempt_time


def test_config_group_name():
    """Test that CONFIG_GROUP_NAME has the expected value."""
    assert CONFIG_GROUP_NAME == "aws_lambda_executor"


def test_invalid_credentials_exceptions():
    """Test that INVALID_CREDENTIALS_EXCEPTIONS has the expected values."""
    expected_exceptions = [
        "ExpiredTokenException",
        "InvalidClientTokenId",
        "UnrecognizedClientException",
    ]
    assert INVALID_CREDENTIALS_EXCEPTIONS == expected_exceptions


if __name__ == "__main__":
    pytest.main([__file__])
