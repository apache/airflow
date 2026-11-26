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

import datetime
from dataclasses import is_dataclass

from airflow.providers.amazon.aws.executors.aws_lambda.utils import (
    CONFIG_GROUP_NAME,
    INVALID_CREDENTIALS_EXCEPTIONS,
    AllLambdaConfigKeys,
    InvokeLambdaKwargsConfigKeys,
    LambdaQueuedTask,
)
from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys


class TestAwsLambdaUtilsConstants:
    def test_config_group_name(self):
        assert CONFIG_GROUP_NAME == "aws_lambda_executor"

    def test_invalid_credentials_exceptions(self):
        assert INVALID_CREDENTIALS_EXCEPTIONS == [
            "ExpiredTokenException",
            "InvalidClientTokenId",
            "UnrecognizedClientException",
        ]


class TestConfigKeys:
    def test_invoke_lambda_kwargs_config_keys(self):
        assert issubclass(InvokeLambdaKwargsConfigKeys, BaseConfigKeys)
        assert InvokeLambdaKwargsConfigKeys.FUNCTION_NAME == "function_name"
        assert InvokeLambdaKwargsConfigKeys.QUALIFIER == "function_qualifier"

    def test_all_lambda_config_keys_inheritance(self):
        assert issubclass(AllLambdaConfigKeys, InvokeLambdaKwargsConfigKeys)
        assert issubclass(AllLambdaConfigKeys, BaseConfigKeys)

    def test_all_lambda_config_keys_values(self):
        assert AllLambdaConfigKeys.AWS_CONN_ID == "conn_id"
        assert AllLambdaConfigKeys.CHECK_HEALTH_ON_STARTUP == "check_health_on_startup"
        assert AllLambdaConfigKeys.MAX_INVOKE_ATTEMPTS == "max_invoke_attempts"
        assert AllLambdaConfigKeys.REGION_NAME == "region_name"
        assert AllLambdaConfigKeys.QUEUE_URL == "queue_url"
        assert AllLambdaConfigKeys.DLQ_URL == "dead_letter_queue_url"
        assert AllLambdaConfigKeys.END_WAIT_TIMEOUT == "end_wait_timeout"
        assert AllLambdaConfigKeys.FUNCTION_NAME == "function_name"
        assert AllLambdaConfigKeys.QUALIFIER == "function_qualifier"


class TestLambdaQueuedTask:
    def test_is_dataclass(self):
        assert is_dataclass(LambdaQueuedTask)

    def test_instantiation_and_fields(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        task = LambdaQueuedTask(
            key="test_task_key",
            command=["airflow", "tasks", "run"],
            queue="default",
            executor_config={"memory": 512},
            attempt_number=1,
            next_attempt_time=now,
        )

        assert task.key == "test_task_key"
        assert task.command == ["airflow", "tasks", "run"]
        assert task.queue == "default"
        assert task.executor_config == {"memory": 512}
        assert task.attempt_number == 1
        assert task.next_attempt_time == now

    def test_equality(self):
        now = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        task1 = LambdaQueuedTask(
            key="k1",
            command=["run"],
            queue="q",
            executor_config={},
            attempt_number=1,
            next_attempt_time=now,
        )
        task2 = LambdaQueuedTask(
            key="k1",
            command=["run"],
            queue="q",
            executor_config={},
            attempt_number=1,
            next_attempt_time=now,
        )
        task3 = LambdaQueuedTask(
            key="k2",
            command=["run"],
            queue="q",
            executor_config={},
            attempt_number=1,
            next_attempt_time=now,
        )

        assert task1 == task2
        assert task1 != task3
