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

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

FUNCTION_NAME = "test_function"
PAYLOAD = '{"hello": "airflow"}'
RUNTIME = "python3.9"
ROLE = "role"
HANDLER = "handler"
CODE = {}


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
    def test_invoke_lambda(self, mock_conn):
        hook = LambdaHook()
        hook.invoke_lambda(function_name=FUNCTION_NAME, payload=PAYLOAD)

        mock_conn().invoke.assert_called_once_with(
            FunctionName=FUNCTION_NAME,
            Payload=PAYLOAD,
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
