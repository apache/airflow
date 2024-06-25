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
from unittest.mock import AsyncMock, Mock

import pytest

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.triggers.lambda_function import LambdaCreateFunctionCompleteTrigger, \
    LambdaInvokeFunctionCompleteTrigger
from airflow.triggers.base import TriggerEvent

TEST_AWS_CONN_ID = "test-aws-conn-id"
TEST_REGION_NAME = "eu-west-2"
TEST_VERIFY = True
TEST_BOTOCORE_CONFIG = {"region_name": "eu-west-2"}


class TestLambdaCreateFunctionCompleteTrigger:
    def test_serialization(self):
        function_name = "test_function_name"
        function_arn = "test_function_arn"
        waiter_delay = 60
        waiter_max_attempts = 30
        aws_conn_id = "aws_default"

        trigger = LambdaCreateFunctionCompleteTrigger(
            function_name=function_name,
            function_arn=function_arn,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.amazon.aws.triggers.lambda_function.LambdaCreateFunctionCompleteTrigger"
        )
        assert kwargs == {
            "function_name": "test_function_name",
            "function_arn": "test_function_arn",
            "waiter_delay": 60,
            "waiter_max_attempts": 30,
            "aws_conn_id": "aws_default",
        }


class TestLambdaInvokeFunctionCompleteTrigger:

    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self.invoke_lambda_trigger = LambdaInvokeFunctionCompleteTrigger(
            function_name="test",
            payload=b'{"key": "value"}',
            invocation_type="RequestResponse",
            log_type="Tail",
            qualifier="test",
            client_context=None,
            aws_conn_id=TEST_AWS_CONN_ID,
            region_name=TEST_REGION_NAME,
            verify=TEST_VERIFY,
            botocore_config=TEST_BOTOCORE_CONFIG,
        )

    def test_serialization(self):
        classpath, kwargs = self.invoke_lambda_trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.lambda_function.LambdaInvokeFunctionCompleteTrigger"
        assert kwargs == {
            "function_name": "test",
            "payload": b'{"key": "value"}',
            "invocation_type": "RequestResponse",
            "log_type": "Tail",
            "qualifier": "test",
            "client_context": None,
            "aws_conn_id": TEST_AWS_CONN_ID,
            "region_name": TEST_REGION_NAME,
            "verify": TEST_VERIFY,
            "botocore_config": TEST_BOTOCORE_CONFIG,
        }

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.invoke_lambda_async"
    )
    @mock.patch.object(LambdaHook, "async_conn")
    async def test_run(self, mock_async_conn, mock_invoke_lambda_async):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        payload = b'{"key": "value"}'
        returned_payload = AsyncMock()
        returned_payload.read.return_value = payload

        fake_response = {
            "ResponseMetadata": "",
            "StatusCode": 200,
            "Payload": returned_payload,
        }

        mock_invoke_lambda_async.return_value = fake_response
        generator = self.invoke_lambda_trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "success"
        assert event.payload["payload"] == payload.decode()
