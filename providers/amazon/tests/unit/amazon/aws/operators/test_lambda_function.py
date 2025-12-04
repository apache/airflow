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

import base64
from unittest import mock
from unittest.mock import Mock, patch

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaCreateFunctionOperator,
    LambdaInvokeFunctionOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

FUNCTION_NAME = "function_name"
PAYLOADS = [
    pytest.param('{"hello": "airflow"}', id="string-payload"),
    pytest.param(b'{"hello": "airflow"}', id="bytes-payload"),
]
ROLE_ARN = "role_arn"
IMAGE_URI = "image_uri"
LOG_RESPONSE = base64.b64encode(b"FOO\n\nBAR\n\n").decode()
BAD_LOG_RESPONSE = LOG_RESPONSE[:-3]
NO_LOG_RESPONSE_SENTINEL = type("NoLogResponseSentinel", (), {})()
LAMBDA_FUNC_NO_EXECUTION = "Lambda function did not execute"


class TestLambdaCreateFunctionOperator:
    def test_init(self):
        op = LambdaCreateFunctionOperator(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            role=ROLE_ARN,
            code={
                "ImageUri": IMAGE_URI,
            },
            aws_conn_id="aws_conn_test",
            region_name="foo-bar-1",
            verify="/spam/egg.pem",
            botocore_config={"baz": "qux"},
        )

        assert op.function_name == FUNCTION_NAME
        assert op.role == ROLE_ARN
        assert op.code == {"ImageUri": IMAGE_URI}

        assert op.aws_conn_id == "aws_conn_test"
        assert op.region_name == "foo-bar-1"
        assert op.verify == "/spam/egg.pem"
        assert op.botocore_config == {"baz": "qux"}

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
    @pytest.mark.parametrize(
        "op_kwargs",
        [
            pytest.param({}, id="no-additional-parameters"),
            pytest.param(
                {"region_name": "eu-west-1", "verify": True, "botocore_config": {}},
                id="additional-parameters",
            ),
        ],
    )
    def test_create_lambda_with_wait_for_completion(self, mock_hook_conn, mock_hook_create_lambda, op_kwargs):
        operator = LambdaCreateFunctionOperator(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            role=ROLE_ARN,
            code={
                "ImageUri": IMAGE_URI,
            },
            wait_for_completion=True,
            aws_conn_id="aws_conn_test",
            **op_kwargs,
        )
        operator.execute(None)

        mock_hook_create_lambda.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("function_active_v2")

    @mock.patch.object(LambdaHook, "create_lambda")
    def test_create_lambda_deferrable(self, _):
        operator = LambdaCreateFunctionOperator(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            role=ROLE_ARN,
            code={
                "ImageUri": IMAGE_URI,
            },
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            operator.execute(None)

    @mock.patch.object(LambdaHook, "create_lambda")
    @mock.patch.object(LambdaHook, "conn")
    @pytest.mark.parametrize(
        "config",
        [
            pytest.param(
                {
                    "architectures": ["arm64"],
                    "logging_config": {"LogFormat": "Text", "LogGroup": "/custom/log-group/"},
                    "snap_start": {"ApplyOn": "PublishedVersions"},
                    "ephemeral_storage": {"Size": 1024},
                },
                id="with-config-argument",
            ),
        ],
    )
    def test_create_lambda_using_config_argument(self, mock_hook_conn, mock_hook_create_lambda, config):
        operator = LambdaCreateFunctionOperator(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            role=ROLE_ARN,
            code={
                "ImageUri": IMAGE_URI,
            },
            config=config,
        )
        operator.execute(None)

        mock_hook_create_lambda.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()
        assert operator.config.get("logging_config") == config.get("logging_config")
        assert operator.config.get("architectures") == config.get("architectures")
        assert operator.config.get("snap_start") == config.get("snap_start")
        assert operator.config.get("ephemeral_storage") == config.get("ephemeral_storage")

    def test_template_fields(self):
        operator = LambdaCreateFunctionOperator(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            role=ROLE_ARN,
            code={
                "ImageUri": IMAGE_URI,
            },
        )
        validate_template_fields(operator)


class TestLambdaInvokeFunctionOperator:
    @pytest.mark.parametrize("payload", PAYLOADS)
    def test_init(self, payload):
        lambda_operator = LambdaInvokeFunctionOperator(
            task_id="test",
            function_name="test",
            payload=payload,
            log_type="None",
            aws_conn_id="aws_conn_test",
            region_name="foo-bar-1",
            verify="/spam/egg.pem",
            botocore_config={"baz": "qux"},
        )
        assert lambda_operator.task_id == "test"
        assert lambda_operator.function_name == "test"
        assert lambda_operator.payload == payload
        assert lambda_operator.log_type == "None"
        assert lambda_operator.aws_conn_id == "aws_conn_test"
        assert lambda_operator.region_name == "foo-bar-1"
        assert lambda_operator.verify == "/spam/egg.pem"
        assert lambda_operator.botocore_config == {"baz": "qux"}

    @mock.patch.object(LambdaHook, "invoke_lambda")
    @mock.patch.object(LambdaHook, "conn")
    @pytest.mark.parametrize(
        "keep_empty_log_lines", [pytest.param(True, id="keep"), pytest.param(False, id="truncate")]
    )
    @pytest.mark.parametrize(
        ("log_result", "expected_execution_logs"),
        [
            pytest.param(LOG_RESPONSE, True, id="log-result"),
            pytest.param(BAD_LOG_RESPONSE, False, id="corrupted-log-result"),
            pytest.param(None, False, id="none-log-result"),
            pytest.param(NO_LOG_RESPONSE_SENTINEL, False, id="no-response"),
        ],
    )
    @pytest.mark.parametrize("payload", PAYLOADS)
    def test_invoke_lambda(
        self,
        mock_conn,
        mock_invoke,
        payload,
        keep_empty_log_lines,
        log_result,
        expected_execution_logs,
        caplog,
    ):
        operator = LambdaInvokeFunctionOperator(
            task_id="task_test",
            function_name="a",
            invocation_type="b",
            log_type="c",
            keep_empty_log_lines=keep_empty_log_lines,
            client_context="d",
            payload=payload,
            qualifier="f",
        )
        returned_payload = Mock()
        returned_payload.read().decode.return_value = "data was read"
        fake_response = {
            "ResponseMetadata": "",
            "StatusCode": 200,
            "Payload": returned_payload,
        }
        if log_result is not NO_LOG_RESPONSE_SENTINEL:
            fake_response["LogResult"] = log_result
        mock_invoke.return_value = fake_response

        caplog.set_level("INFO", "airflow.task.operators")
        value = operator.execute(None)

        assert value == "data was read"
        mock_invoke.assert_called_once_with(
            function_name="a",
            invocation_type="b",
            log_type="c",
            client_context="d",
            payload=payload,
            qualifier="f",
        )

        # Validate log messages in task logs
        if expected_execution_logs:
            assert "The last 4 KB of the Lambda execution log" in caplog.text
            assert "FOO" in caplog.messages
            assert "BAR" in caplog.messages
            if keep_empty_log_lines:
                assert "" in caplog.messages
            else:
                assert "" not in caplog.messages
        else:
            assert "The last 4 KB of the Lambda execution log" not in caplog.text

    @patch.object(LambdaInvokeFunctionOperator, "hook", new_callable=mock.PropertyMock)
    def test_invoke_lambda_bad_http_code(self, hook_mock):
        operator = LambdaInvokeFunctionOperator(
            task_id="task_test",
            function_name="a",
        )
        hook_mock().invoke_lambda.return_value = {"ResponseMetadata": "", "StatusCode": 404}

        with pytest.raises(ValueError, match=LAMBDA_FUNC_NO_EXECUTION):
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

        with pytest.raises(ValueError, match=LAMBDA_FUNC_NO_EXECUTION):
            operator.execute(None)

    def test_template_fields(self):
        operator = LambdaInvokeFunctionOperator(
            task_id="task_test",
            function_name="a",
        )
        validate_template_fields(operator)
