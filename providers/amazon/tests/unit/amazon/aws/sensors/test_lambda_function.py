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
from airflow.providers.amazon.aws.sensors.lambda_function import LambdaFunctionStateSensor
from airflow.providers.common.compat.sdk import AirflowException

FUNCTION_NAME = "function_name"


class TestLambdaFunctionStateSensor:
    def test_init(self):
        op = LambdaFunctionStateSensor(
            task_id="task_test",
            function_name=FUNCTION_NAME,
            aws_conn_id="aws_conn_test",
            region_name="foo-bar-1",
            verify="/spam/egg.pem",
            botocore_config={"baz": "qux"},
        )

        assert op.function_name == FUNCTION_NAME

        assert op.aws_conn_id == "aws_conn_test"
        assert op.region_name == "foo-bar-1"
        assert op.verify == "/spam/egg.pem"
        assert op.botocore_config == {"baz": "qux"}

    @pytest.mark.parametrize(
        ("get_function_output", "expect_failure", "expected"),
        [
            pytest.param(
                {"Configuration": {"State": "Active"}},
                False,
                True,
                id="Active state",
            ),
            pytest.param(
                {"Configuration": {"State": "Pending"}},
                False,
                False,
                id="Pending state",
            ),
            pytest.param(
                {"Configuration": {"State": "Failed"}},
                True,
                None,
                id="Failed state",
            ),
        ],
    )
    def test_poke(self, get_function_output, expect_failure, expected):
        with mock.patch.object(LambdaHook, "conn") as mock_conn:
            mock_conn.get_function.return_value = get_function_output
            sensor = LambdaFunctionStateSensor(
                task_id="test_sensor",
                function_name=FUNCTION_NAME,
            )

            if expect_failure:
                with pytest.raises(AirflowException):
                    sensor.poke({})
            else:
                result = sensor.poke({})
                assert result == expected

            mock_conn.get_function.assert_called_once_with(
                FunctionName=FUNCTION_NAME,
            )

    def test_fail_poke(self):
        sensor = LambdaFunctionStateSensor(
            task_id="test_sensor",
            function_name=FUNCTION_NAME,
        )
        message = "Lambda function state sensor failed because the Lambda is in a failed state"
        with mock.patch("airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook.conn") as conn:
            conn.get_function.return_value = {"Configuration": {"State": "Failed"}}
            with pytest.raises(AirflowException, match=message):
                sensor.poke(context={})
