#
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
from unittest.mock import Mock, patch

import pytest

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaCreateFunctionOperator,
    LambdaInvokeFunctionOperator,
)

FUNCTION_NAME = "function_name"
ROLE_ARN = "role_arn"
IMAGE_URI = "image_uri"


class TestLambdaCreateFunctionOperator:
    @mock.patch.object(LambdaHook, "create_lambda")
    @mock.patch.object(LambdaHook, "conn")
    def test_create_lambda_without_wait_for_completion(self, mock_hook_conn, mock_hook_create_lambda):
        operator = LambdaCreateFunctionOperator(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            role=ROLE_ARN,
            code={
                "ImageUri": IMAGE_URI,
            },
        )
        operator.execute(None)

        mock_hook_create_lambda.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(LambdaHook, "create_lambda")
    @mock.patch.object(LambdaHook, "conn")
    def test_create_lambda_with_wait_for_completion(self, mock_hook_conn, mock_hook_create_lambda):
        operator = LambdaCreateFunctionOperator(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            role=ROLE_ARN,
            code={
                "ImageUri": IMAGE_URI,
            },
            wait_for_completion=True,
        )
        operator.execute(None)

        mock_hook_create_lambda.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("function_active_v2")


class TestLambdaInvokeFunctionOperator:
    def test_init(self):
        lambda_operator = LambdaInvokeFunctionOperator(
            task_id="test",
            function_name="test",
            payload=json.dumps({"TestInput": "Testdata"}),
            log_type="None",
            aws_conn_id="aws_conn_test",
        )
        assert lambda_operator.task_id == "test"
        assert lambda_operator.function_name == "test"
        assert lambda_operator.payload == json.dumps({"TestInput": "Testdata"})
        assert lambda_operator.log_type == "None"
        assert lambda_operator.aws_conn_id == "aws_conn_test"

    @patch.object(LambdaInvokeFunctionOperator, "hook", new_callable=mock.PropertyMock)
    def test_invoke_lambda(self, hook_mock):
        operator = LambdaInvokeFunctionOperator(
            task_id="task_test",
            function_name="a",
            invocation_type="b",
            log_type="c",
            client_context="d",
            payload="e",
            qualifier="f",
        )
        returned_payload = Mock()
        returned_payload.read().decode.return_value = "data was read"
        hook_mock().invoke_lambda.return_value = {
            "ResponseMetadata": "",
            "StatusCode": 200,
            "Payload": returned_payload,
        }

        value = operator.execute(None)

        assert value == "data was read"
        hook_mock().invoke_lambda.assert_called_once_with(
            function_name="a",
            invocation_type="b",
            log_type="c",
            client_context="d",
            payload="e",
            qualifier="f",
        )

    @patch.object(LambdaInvokeFunctionOperator, "hook", new_callable=mock.PropertyMock)
    def test_invoke_lambda_bad_http_code(self, hook_mock):
        operator = LambdaInvokeFunctionOperator(
            task_id="task_test",
            function_name="a",
        )
        hook_mock().invoke_lambda.return_value = {"ResponseMetadata": "", "StatusCode": 404}

        with pytest.raises(ValueError):
            operator.execute(None)

    @patch.object(LambdaInvokeFunctionOperator, "hook", new_callable=mock.PropertyMock)
    def test_invoke_lambda_function_error(self, hook_mock):
        operator = LambdaInvokeFunctionOperator(
            task_id="task_test",
            function_name="a",
        )
        hook_mock().invoke_lambda.return_value = {
            "ResponseMetadata": "",
            "StatusCode": 404,
            "FunctionError": "yes",
            "Payload": Mock(),
        }

        with pytest.raises(ValueError):
            operator.execute(None)
