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

from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.links.base_aws import BaseAwsLink
from tests.test_utils.mock_operators import MockOperator

XCOM_KEY = "test_xcom_key"
CUSTOM_KEYS = {
    "foo": "bar",
    "spam": "egg",
}


class SimpleBaseAwsLink(BaseAwsLink):
    key = XCOM_KEY


class TestBaseAwsLink:
    @pytest.mark.parametrize(
        "region_name, aws_partition,keywords,expected_value",
        [
            ("eu-central-1", "aws", {}, {"region_name": "eu-central-1", "aws_domain": "aws.amazon.com"}),
            ("cn-north-1", "aws-cn", {}, {"region_name": "cn-north-1", "aws_domain": "amazonaws.cn"}),
            (
                "us-gov-east-1",
                "aws-us-gov",
                {},
                {"region_name": "us-gov-east-1", "aws_domain": "amazonaws-us-gov.com"},
            ),
            (
                "eu-west-1",
                "aws",
                CUSTOM_KEYS,
                {"region_name": "eu-west-1", "aws_domain": "aws.amazon.com", **CUSTOM_KEYS},
            ),
        ],
    )
    def test_persist(self, region_name, aws_partition, keywords, expected_value):
        mock_context = MagicMock()

        SimpleBaseAwsLink.persist(
            context=mock_context,
            operator=MockOperator(task_id="test_task_id"),
            region_name=region_name,
            aws_partition=aws_partition,
            **keywords,
        )

        ti = mock_context["ti"]
        ti.xcom_push.assert_called_once_with(
            execution_date=None,
            key=XCOM_KEY,
            value=expected_value,
        )

    def test_disable_xcom_push(self):
        mock_context = MagicMock()
        SimpleBaseAwsLink.persist(
            context=mock_context,
            operator=MockOperator(task_id="test_task_id", do_xcom_push=False),
            region_name="eu-east-1",
            aws_partition="aws",
        )
        ti = mock_context["ti"]
        ti.xcom_push.assert_not_called()
