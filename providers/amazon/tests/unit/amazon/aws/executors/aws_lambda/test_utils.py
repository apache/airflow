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

import pytest

from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    CONFIG_GROUP_NAME,
    INVALID_CREDENTIALS_EXCEPTIONS,
    AllLambdaConfigKeys,
    InvokeLambdaKwargsConfigKeys,
    LambdaQueuedTask,
)


class TestLambdaQueuedTask:
    def test_stores_queue_metadata(self):
        next_attempt_time = dt.datetime(2026, 9, 15, tzinfo=dt.timezone.utc)

        queued_task = LambdaQueuedTask(
            key="key",
            command=["airflow", "tasks", "run"],
            queue="default",
            executor_config={"function_name": "function"},
            attempt_number=2,
            next_attempt_time=next_attempt_time,
        )

        assert queued_task.key == "key"
        assert queued_task.command == ["airflow", "tasks", "run"]
        assert queued_task.queue == "default"
        assert queued_task.executor_config == {"function_name": "function"}
        assert queued_task.attempt_number == 2
        assert queued_task.next_attempt_time == next_attempt_time


class TestLambdaConfigKeys:
    def test_config_group_name(self):
        assert CONFIG_GROUP_NAME == "aws_lambda_executor"

    def test_invalid_credentials_exceptions(self):
        assert INVALID_CREDENTIALS_EXCEPTIONS == [
            "ExpiredTokenException",
            "InvalidClientTokenId",
            "UnrecognizedClientException",
        ]

    @pytest.mark.parametrize(
        ("config_key", "expected"),
        [
            (InvokeLambdaKwargsConfigKeys.FUNCTION_NAME, "function_name"),
            (InvokeLambdaKwargsConfigKeys.QUALIFIER, "function_qualifier"),
        ],
    )
    def test_invoke_lambda_kwargs_config_keys_values(self, config_key, expected):
        assert config_key == expected

    @pytest.mark.parametrize(
        ("config_key", "expected"),
        [
            (AllLambdaConfigKeys.FUNCTION_NAME, "function_name"),
            (AllLambdaConfigKeys.QUALIFIER, "function_qualifier"),
            (AllLambdaConfigKeys.AWS_CONN_ID, "conn_id"),
            (AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP, "check_health_on_startup"),
            (AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS, "max_invoke_attempts"),
            (AllLambdaConfigKeys.REGION_NAME, "region_name"),
            (AllLambdaConfigKeys.QUEUE_URL, "queue_url"),
            (AllLambdaConfigKeys.DLQ_URL, "dead_letter_queue_url"),
            (AllLambdaConfigKeys.END_WAIT_TIMEOUT, "end_wait_timeout"),
        ],
    )
    def test_all_lambda_config_keys_values(self, config_key, expected):
        assert config_key == expected

    def test_invoke_lambda_kwargs_config_keys_iterable(self):
        assert set(InvokeLambdaKwargsConfigKeys()) == {"function_name", "function_qualifier"}
