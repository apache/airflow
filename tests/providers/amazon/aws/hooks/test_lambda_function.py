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

import pytest

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

FUNCTION_NAME = "test_function"
PAYLOAD = '{"hello": "airflow"}'
RUNTIME = "python3.9"
ROLE = "role"
HANDLER = "handler"
CODE = {}


class TestLambdaHook:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = LambdaHook(aws_conn_id="aws_default")
        assert hook.conn is not None

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.conn", new_callable=mock.PropertyMock
    )
    def test_invoke_lambda(self, mock_conn):
        mock_conn().invoke.return_value = {}

        hook = LambdaHook(aws_conn_id="aws_default")
        hook.invoke_lambda(function_name=FUNCTION_NAME, payload=PAYLOAD)

        mock_conn().invoke.assert_called_once_with(
            FunctionName=FUNCTION_NAME,
            Payload=PAYLOAD,
        )

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.conn", new_callable=mock.PropertyMock
    )
    def test_create_lambda_with_zip_package_type(self, mock_conn):
        mock_conn().create_function.return_value = {}

        hook = LambdaHook(aws_conn_id="aws_default")
        hook.create_lambda(
            function_name=FUNCTION_NAME,
            runtime=RUNTIME,
            role=ROLE,
            handler=HANDLER,
            code=CODE,
            package_type="Zip",
        )

        mock_conn().create_function.assert_called_once_with(
            FunctionName=FUNCTION_NAME,
            Runtime=RUNTIME,
            Role=ROLE,
            Handler=HANDLER,
            Code=CODE,
            PackageType="Zip",
        )

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.conn", new_callable=mock.PropertyMock
    )
    def test_create_lambda_with_zip_package_type_and_no_runtime(self, mock_conn):
        mock_conn().create_function.return_value = {}

        hook = LambdaHook(aws_conn_id="aws_default")
        with pytest.raises(TypeError):
            hook.create_lambda(
                function_name=FUNCTION_NAME,
                role=ROLE,
                handler=HANDLER,
                code=CODE,
                package_type="Zip",
            )

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.conn", new_callable=mock.PropertyMock
    )
    def test_create_lambda_with_zip_package_type_and_no_handler(self, mock_conn):
        mock_conn().create_function.return_value = {}

        hook = LambdaHook(aws_conn_id="aws_default")
        with pytest.raises(TypeError):
            hook.create_lambda(
                function_name=FUNCTION_NAME,
                runtime=RUNTIME,
                role=ROLE,
                code=CODE,
                package_type="Zip",
            )

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.conn", new_callable=mock.PropertyMock
    )
    def test_create_lambda_with_image_package_type(self, mock_conn):
        mock_conn().create_function.return_value = {}

        hook = LambdaHook(aws_conn_id="aws_default")
        hook.create_lambda(
            function_name=FUNCTION_NAME,
            role=ROLE,
            code=CODE,
            package_type="Image",
        )

        mock_conn().create_function.assert_called_once_with(
            FunctionName=FUNCTION_NAME,
            Role=ROLE,
            Code=CODE,
            PackageType="Image",
        )
