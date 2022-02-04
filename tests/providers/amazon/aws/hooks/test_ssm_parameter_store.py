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

import pytest
from botocore.exceptions import ClientError
from moto import mock_ssm

from airflow.providers.amazon.aws.hooks.ssm_parameter_store import SSMParameterStoreHook


class TestSSMParameterStoreHook(unittest.TestCase):
    def setUp(self):
        self.ssm = SSMParameterStoreHook()

    def test_init(self):
        assert self.ssm.aws_conn_id == 'aws_default'

    @mock_ssm
    def test_create_parameter_store(self):
        hook = self.ssm
        hook.put_parameter(parameter_name='testing', value='test', param_type='String')
        param = hook.get_parameter(parameter_name='testing', with_decryption=False)
        assert param is not None

    @mock_ssm
    def test_create_parameter_with_empty_value(self):
        hook = self.ssm
        with pytest.raises(ClientError) as e:
            hook.put_parameter(parameter_name='testing', value='', param_type='String')
        ex = e.value
        assert ex.operation_name == "PutParameter"
        assert ex.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    @mock_ssm
    def test_create_parameter_with_wrong_data_type(self):
        hook = self.ssm
        with pytest.raises(ClientError) as e:
            hook.put_parameter(parameter_name='testing', value='test', param_type="String", DataType="not_text")
        ex = e.value
        assert ex.operation_name == "PutParameter"
        assert ex.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    @mock_ssm
    def test_get_parameter(self):
        hook = self.ssm
        hook.put_parameter(parameter_name='testing', value='test', param_type='String')
        param = hook.get_parameter(parameter_name='testing', with_decryption=False)
        assert param is not None
        assert param == "test"

    @mock_ssm
    def test_get_non_existing_parameter(self):
        hook = self.ssm
        with pytest.raises(ClientError) as e:
            hook.get_parameter(parameter_name='non_existing', with_decryption=True)
        ex = e.value
        assert ex.operation_name == "GetParameter"
        assert ex.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert ex.response["Error"]["Message"] == "Parameter non_existing not found."

    @mock_ssm
    def test_get_parameter_by_path(self):
        hook = self.ssm
        hook.put_parameter(parameter_name='/foo/name1', value='name1', param_type='String')
        hook.put_parameter(parameter_name='/foo/name2', value='name2', param_type='String')
        hook.put_parameter(parameter_name='/foo/name3', value='name3', param_type='String')
        hook.put_parameter(parameter_name="foo", Description="A test parameter", value="bar", param_type="String")
        hook.put_parameter(parameter_name="baz", Description="A test parameter", value="qux", param_type="String")
        response = hook.get_parameters_by_path(path='/')
        assert len(response["Parameters"]) == 2
        assert {p["Value"] for p in response["Parameters"]} == ({"bar", "qux"})

        response = hook.get_parameters_by_path(path="/foo")
        assert len(response["Parameters"]) == 3
        assert {p["Value"] for p in response["Parameters"]} == ({"name1", "name2", "name3"})

        hook.put_parameter(
            parameter_name="/baz/name1",
            Description="A test parameter (list)",
            value="value1,value2,value3",
            param_type="StringList",
        )

        filters = [{"Key": "Type", "Option": "Equals", "Values": ["StringList"]}]
        response = hook.get_parameters_by_path(path="/baz", ParameterFilters=filters)

        assert len(response["Parameters"]) == 1
        assert {p["Name"] for p in response["Parameters"]} == ({"/baz/name1"})

    @mock_ssm
    def test_get_parameter_by_path_wrong_filter(self):
        hook = self.ssm
        hook.put_parameter(parameter_name='/foo/name1', value='name1', param_type='String')
        hook.put_parameter(parameter_name='/foo/name2', value='name2', param_type='String')
        hook.put_parameter(parameter_name='/foo/name3', value='name3', param_type='String')
        hook.put_parameter(parameter_name="foo", Description="A test parameter", value="bar", param_type="String")
        hook.put_parameter(parameter_name="baz", Description="A test parameter", value="qux", param_type="String")

        filters = [{"Key": "Name", "Values": ["error"]}]

        with pytest.raises(ClientError) as e:
            hook.get_parameters_by_path(path="/baz", ParameterFilters=filters)

        ex = e.value
        assert ex.operation_name == "GetParametersByPath"
        assert ex.response["ResponseMetadata"]["HTTPStatusCode"] == 400
