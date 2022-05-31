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

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from airflow import AirflowException
from airflow.providers.amazon.aws.operators.base import AwsBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

TEST_TASK_ID = "test_task_id"
TEST_AWS_REGION = "eu-west-1"
TEST_AWS_CONN_ID = "aws_test_conn"


class MockAwsBaseOperator(AwsBaseOperator):
    aws_hook_class = MagicMock()

    def execute(self, context: "Context") -> Any:
        pass


class TestAwsBaseOperator:
    def test_region_deprecation_warning(self):
        warning_message = "Parameter `region` is deprecated. Please use `region_name` instead."
        with pytest.warns(DeprecationWarning, match=warning_message):
            test_task = MockAwsBaseOperator(task_id=TEST_TASK_ID, region=TEST_AWS_REGION)
        assert test_task.region_name == TEST_AWS_REGION

    def test_set_region_name(self):
        test_task = MockAwsBaseOperator(task_id=TEST_TASK_ID, region_name=TEST_AWS_REGION)
        assert test_task.region_name == TEST_AWS_REGION

    def test_raise_error_on_region_and_region_name(self):
        raise_message = "Either `region_name` or `region` can be provided, not both."
        with pytest.raises(AirflowException, match=raise_message):
            MockAwsBaseOperator(task_id=TEST_TASK_ID, region_name=TEST_AWS_REGION, region=TEST_AWS_REGION)

    @pytest.mark.parametrize("test_aws_conn_id", [TEST_AWS_CONN_ID, None])
    def test_set_aws_conn_id(self, test_aws_conn_id):
        test_task = MockAwsBaseOperator(task_id=TEST_TASK_ID, aws_conn_id=test_aws_conn_id)
        assert test_task.aws_conn_id == test_aws_conn_id

    def test_hook_conn_name_attr_not_aws_conn_id(self):
        mock_hook_class = MagicMock()
        mock_hook_class.conn_name_attr = "mock_conn_id"
        mock_hook_class.default_conn_name = "mock_default_conn_name"
        MockAwsBaseOperator.aws_hook_class = mock_hook_class

        # Test default connection name
        test_task = MockAwsBaseOperator(task_id=TEST_TASK_ID)
        assert "mock_conn_id" in test_task.hooks_class_args
        assert test_task.hooks_class_args["mock_conn_id"] == "mock_default_conn_name"

        # Test set connection name
        test_task = MockAwsBaseOperator(task_id=TEST_TASK_ID, mock_conn_id="mock_conn_name")
        assert test_task.hooks_class_args["mock_conn_id"] == "mock_conn_name"

    @pytest.mark.parametrize("test_aws_conn_id", [TEST_AWS_CONN_ID, None])
    @pytest.mark.parametrize("test_region_name", [TEST_AWS_REGION, None])
    @pytest.mark.parametrize("test_botocore_config", [MagicMock(), None])
    @pytest.mark.parametrize("test_hook_kwargs", [{"foo": "bar", "spam": "egg"}, {}])
    def test_hook_property(self, test_aws_conn_id, test_region_name, test_botocore_config, test_hook_kwargs):
        mock_hook_class = MagicMock()
        mock_hook_class.conn_name_attr = "aws_conn_id"
        MockAwsBaseOperator.aws_hook_class = mock_hook_class
        if test_hook_kwargs:
            MockAwsBaseOperator.aws_hook_class_kwargs = set(test_hook_kwargs)

        test_task = MockAwsBaseOperator(
            task_id=TEST_TASK_ID,
            aws_conn_id=test_aws_conn_id,
            region_name=test_region_name,
            botocore_config=test_botocore_config,
            **test_hook_kwargs,
        )

        # Test that hook is cached_property
        hook = test_task.hook
        assert hook is test_task.hook
        mock_hook_class.assert_called_once_with(
            aws_conn_id=test_aws_conn_id,
            region_name=test_region_name,
            config=test_botocore_config,
            **test_hook_kwargs,
        )
