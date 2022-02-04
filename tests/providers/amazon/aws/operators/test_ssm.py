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

import unittest

from moto import mock_ssm

from airflow.providers.amazon.aws.hooks.ssm_parameter_store import SSMParameterStoreHook
from airflow.providers.amazon.aws.operators.ssm import (
    SSMCreateParameterOperator,
    SSMGetParameterByPathOperator,
    SSMGetParameterOperator,
)


class TestSSMGetParameterOperator(unittest.TestCase):
    def test_init(self):
        ssm_operator = SSMGetParameterOperator(
            task_id="task_test",
            parameter_name="test",
            with_decryption=True,
            aws_conn_id="aws_conn_test",
        )
        assert ssm_operator.task_id == "task_test"
        assert ssm_operator.parameter_name == "test"
        assert ssm_operator.aws_conn_id == "aws_conn_test"

    @mock_ssm
    def test_get_parameter(self):
        # create instance
        ssm_hook = SSMParameterStoreHook()
        ssm_hook.put_parameter(parameter_name='test', value='testing', param_type='SecureString')
        # start instance
        ssm_get_parameter = SSMGetParameterOperator(
            task_id="task_test",
            parameter_name="test",
            with_decryption=True,
        )
        value = ssm_get_parameter.execute(None)
        # assert instance state is running
        assert value == "testing"


class TestSSMGetParameterByPathOperator(unittest.TestCase):
    def test_init(self):
        ssm_operator = SSMGetParameterByPathOperator(
            task_id="task_test",
            path="/test",
            aws_conn_id="aws_conn_test",
        )
        assert ssm_operator.task_id == "task_test"
        assert ssm_operator.path == "/test"
        assert ssm_operator.aws_conn_id == "aws_conn_test"

    @mock_ssm
    def test_get_parameter_by_path(self):
        # create instance
        ssm_hook = SSMParameterStoreHook()
        ssm_hook.put_parameter(parameter_name='/test/foo1', value='testing path', param_type='String')
        # start instance
        ssm_get_parameter_by_path = SSMGetParameterByPathOperator(task_id="task_test", path="/test")
        response = ssm_get_parameter_by_path.execute(None)

        assert len(response["Parameters"]) == 1
        assert response['Parameters'][0]['Value'] == "testing path"


class TestSSMCreateParameterOperator(unittest.TestCase):
    def test_init(self):
        ssm_operator = SSMCreateParameterOperator(
            task_id="task_test",
            parameter_name="test",
            value="testing",
            param_type="String",
            aws_conn_id="aws_conn_test",
        )
        assert ssm_operator.task_id == "task_test"
        assert ssm_operator.parameter_name == "test"
        assert ssm_operator.value == "testing"
        assert ssm_operator.param_type == "String"
        assert ssm_operator.aws_conn_id == "aws_conn_test"

    @mock_ssm
    def test_get_parameter(self):
        # create instance
        ssm_hook = SSMParameterStoreHook()

        ssm_operator = SSMCreateParameterOperator(
            task_id="task_test",
            parameter_name="test",
            value="testing",
            param_type="String",
            parameter_kwargs={'Description': 'Test Parameter'},
            aws_conn_id="aws_conn_test",
        )
        ssm_operator.execute(None)
        assert ssm_hook.get_parameter("test", True) == "testing"
