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
import logging
from unittest import mock, mock as async_mock
from unittest.mock import MagicMock, AsyncMock

import pytest

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

FUNCTION_NAME = "test_function"
PAYLOAD = '{"hello": "airflow"}'
BYTES_PAYLOAD = b'{"hello": "airflow"}'
RUNTIME = "python3.9"
ROLE = "role"
HANDLER = "handler"
CODE: dict = {}
LOG_RESPONSE = base64.b64encode(b"FOO\n\nBAR\n\n").decode()
BAD_LOG_RESPONSE = LOG_RESPONSE[:-3]


class LambdaHookForTests(LambdaHook):
    conn = MagicMock(name="conn")


@pytest.fixture
def hook():
    return LambdaHookForTests()


class TestLambdaHook:
    def test_get_conn_returns_a_boto3_connection(self, hook):
        assert hook.conn is not None

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.conn", new_callable=mock.PropertyMock
    )
    @pytest.mark.parametrize(
        "payload, invoke_payload",
        [(PAYLOAD, BYTES_PAYLOAD), (BYTES_PAYLOAD, BYTES_PAYLOAD)],
    )
    def test_invoke_lambda(self, mock_conn, payload, invoke_payload):
        hook = LambdaHook()
        hook.invoke_lambda(function_name=FUNCTION_NAME, payload=payload)

        mock_conn().invoke.assert_called_once_with(
            FunctionName=FUNCTION_NAME,
            Payload=invoke_payload,
        )

    @pytest.mark.parametrize(
        "hook_params, boto3_params",
        [
            pytest.param(
                {
                    "function_name": FUNCTION_NAME,
                    "runtime": RUNTIME,
                    "role": ROLE,
                    "handler": HANDLER,
                    "code": CODE,
                    "package_type": "Zip",
                },
                {
                    "FunctionName": FUNCTION_NAME,
                    "Runtime": RUNTIME,
                    "Role": ROLE,
                    "Handler": HANDLER,
                    "Code": CODE,
                    "PackageType": "Zip",
                },
                id="'Zip' as 'package_type'",
            ),
            pytest.param(
                {
                    "function_name": FUNCTION_NAME,
                    "role": ROLE,
                    "code": CODE,
                    "package_type": "Image",
                },
                {
                    "FunctionName": FUNCTION_NAME,
                    "Role": ROLE,
                    "Code": CODE,
                    "PackageType": "Image",
                },
                id="'Image' as 'package_type'",
            ),
        ],
    )
    def test_create_lambda(self, hook_params, boto3_params, hook):
        hook.conn.create_function.reset_mock()
        hook.conn.create_function.return_value = {}
        hook.create_lambda(**hook_params)

        hook.conn.create_function.assert_called_once_with(**boto3_params)

    @pytest.mark.parametrize(
        "params",
        [
            pytest.param(
                {
                    "handler": HANDLER,
                },
                id="'runtime' not provided",
            ),
            pytest.param(
                {
                    "runtime": RUNTIME,
                },
                id="'handler' not provided",
            ),
        ],
    )
    def test_create_lambda_with_zip_package_type_and_missing_args(self, params, hook):
        hook.conn.create_function.return_value = {}

        with pytest.raises(TypeError):
            hook.create_lambda(
                function_name=FUNCTION_NAME,
                role=ROLE,
                code=CODE,
                package_type="Zip",
                **params,
            )

    def test_encode_log_result(self):
        assert LambdaHook.encode_log_result(LOG_RESPONSE) == ["FOO", "", "BAR", ""]
        assert LambdaHook.encode_log_result(LOG_RESPONSE, keep_empty_lines=False) == ["FOO", "BAR"]
        assert LambdaHook.encode_log_result("") == []

    @pytest.mark.parametrize(
        "log_result",
        [
            pytest.param(BAD_LOG_RESPONSE, id="corrupted"),
            pytest.param(None, id="none"),
        ],
    )
    def test_encode_corrupted_log_result(self, log_result):
        assert LambdaHook.encode_log_result(log_result) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload, invoke_payload",
        [(PAYLOAD, BYTES_PAYLOAD), (BYTES_PAYLOAD, BYTES_PAYLOAD)],
    )
    @mock.patch("aiobotocore.client.AioBaseClient._make_api_call")
    async def test_invoke_lambda_async(self, mock_make_api_call, payload, invoke_payload):
        hook = LambdaHook()
        await hook.invoke_lambda_async(function_name=FUNCTION_NAME, payload=payload)

        mock_make_api_call.assert_called_once_with("Invoke",
                                                   {"FunctionName": FUNCTION_NAME, "Payload": invoke_payload})

    def test_validate_response_success(self):
        sample_response = {
            "ResponseMetadata": {
                "RequestId": "2a3cbd1f-afba-4cd8-98d7-2d0156632cee",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "date": "Mon, 24 Jun 2024 22:32:01 GMT",
                    "content-type": "application/json",
                    "content-length": "53",
                    "connection": "keep-alive",
                    "x-amzn-requestid": "2a3cbd1f-afba-4cd8-98d7-2d0156632cee",
                    "x-amzn-remapped-content-length": "0",
                    "x-amz-executed-version": "$LATEST",
                    "x-amz-log-result": "U1RBUlQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUgVmVyc2lvbjogJExBVEVTVApFTkQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUKUkVQT1JUIFJlcXVlc3RJZDogMmEzY2JkMWYtYWZiYS00Y2Q4LTk4ZDctMmQwMTU2NjMyY2VlCUR1cmF0aW9uOiAxLjg3IG1zCUJpbGxlZCBEdXJhdGlvbjogMiBtcwlNZW1vcnkgU2l6ZTogMTI4IE1CCU1heCBNZW1vcnkgVXNlZDogMzAgTUIJSW5pdCBEdXJhdGlvbjogMTA3LjE0IG1zCQo=",
                    "x-amzn-trace-id": "root=1-6679f3e1-42fb62f97deb767d7712386f;parent=30345a2039563aa5;sampled=0;lineage=570f530d:0"
                },
                "RetryAttempts": 0
            },
            "StatusCode": 200,
            "LogResult": "U1RBUlQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUgVmVyc2lvbjogJExBVEVTVApFTkQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUKUkVQT1JUIFJlcXVlc3RJZDogMmEzY2JkMWYtYWZiYS00Y2Q4LTk4ZDctMmQwMTU2NjMyY2VlCUR1cmF0aW9uOiAxLjg3IG1zCUJpbGxlZCBEdXJhdGlvbjogMiBtcwlNZW1vcnkgU2l6ZTogMTI4IE1CCU1heCBNZW1vcnkgVXNlZDogMzAgTUIJSW5pdCBEdXJhdGlvbjogMTA3LjE0IG1zCQo=",
            "ExecutedVersion": "$LATEST",
        }
        hook = LambdaHook()
        payload = b'{"key": "value"}'
        result = hook.validate_response(response=sample_response, payload=payload)

        assert payload.decode() == result

    def test_validate_response_failed(self):
        sample_response = {
            "ResponseMetadata": {
                "RequestId": "2a3cbd1f-afba-4cd8-98d7-2d0156632cee",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "date": "Mon, 24 Jun 2024 22:32:01 GMT",
                    "content-type": "application/json",
                    "content-length": "53",
                    "connection": "keep-alive",
                    "x-amzn-requestid": "2a3cbd1f-afba-4cd8-98d7-2d0156632cee",
                    "x-amzn-remapped-content-length": "0",
                    "x-amz-executed-version": "$LATEST",
                    "x-amz-log-result": "U1RBUlQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUgVmVyc2lvbjogJExBVEVTVApFTkQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUKUkVQT1JUIFJlcXVlc3RJZDogMmEzY2JkMWYtYWZiYS00Y2Q4LTk4ZDctMmQwMTU2NjMyY2VlCUR1cmF0aW9uOiAxLjg3IG1zCUJpbGxlZCBEdXJhdGlvbjogMiBtcwlNZW1vcnkgU2l6ZTogMTI4IE1CCU1heCBNZW1vcnkgVXNlZDogMzAgTUIJSW5pdCBEdXJhdGlvbjogMTA3LjE0IG1zCQo=",
                    "x-amzn-trace-id": "root=1-6679f3e1-42fb62f97deb767d7712386f;parent=30345a2039563aa5;sampled=0;lineage=570f530d:0"
                },
                "RetryAttempts": 0
            },
            "StatusCode": 200,
            'FunctionError': 'Unhandled',
            "LogResult": "U1RBUlQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUgVmVyc2lvbjogJExBVEVTVApFTkQgUmVxdWVzdElkOiAyYTNjYmQxZi1hZmJhLTRjZDgtOThkNy0yZDAxNTY2MzJjZWUKUkVQT1JUIFJlcXVlc3RJZDogMmEzY2JkMWYtYWZiYS00Y2Q4LTk4ZDctMmQwMTU2NjMyY2VlCUR1cmF0aW9uOiAxLjg3IG1zCUJpbGxlZCBEdXJhdGlvbjogMiBtcwlNZW1vcnkgU2l6ZTogMTI4IE1CCU1heCBNZW1vcnkgVXNlZDogMzAgTUIJSW5pdCBEdXJhdGlvbjogMTA3LjE0IG1zCQo=",
            "ExecutedVersion": "$LATEST",
        }
        hook = LambdaHook()
        payload = b'{"errorMessage":"2024-06-24T22:36:15.685Z 62596cdb-b2ee-419d-a99f-f4eb76eb2138 Task timed out after 3.01 seconds"}'

        with pytest.raises(ValueError, match="Lambda function execution resulted in error"):
            hook.validate_response(response=sample_response, payload=payload)
