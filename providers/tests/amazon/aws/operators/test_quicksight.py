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

from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.operators.quicksight import (
    QuickSightCreateIngestionOperator,
)

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

DATA_SET_ID = "DemoDataSet"
INGESTION_ID = "DemoDataSet_Ingestion"
AWS_ACCOUNT_ID = "123456789012"
INGESTION_TYPE = "FULL_REFRESH"

MOCK_RESPONSE = {
    "Status": 201,
    "Arn": "arn:aws:quicksight:us-east-1:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet_Ingestion",
    "IngestionId": "DemoDataSet_Ingestion",
    "IngestionStatus": "INITIALIZED",
    "RequestId": "fc1f7eea-1327-41d6-9af7-c12f097ed343",
}


class TestQuickSightCreateIngestionOperator:
    def setup_method(self):
        self.default_op_kwargs = {
            "task_id": "quicksight_create",
            "aws_conn_id": None,
            "data_set_id": DATA_SET_ID,
            "ingestion_id": INGESTION_ID,
        }

    def test_init(self):
        self.default_op_kwargs.pop("aws_conn_id", None)

        op = QuickSightCreateIngestionOperator(
            **self.default_op_kwargs,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="cn-north-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.client_type == "quicksight"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "cn-north-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = QuickSightCreateIngestionOperator(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(QuickSightHook, "create_ingestion", return_value=MOCK_RESPONSE)
    def test_execute(self, mock_create_ingestion):
        QuickSightCreateIngestionOperator(**self.default_op_kwargs).execute({})
        mock_create_ingestion.assert_called_once_with(
            data_set_id=DATA_SET_ID,
            ingestion_id=INGESTION_ID,
            ingestion_type="FULL_REFRESH",
            wait_for_completion=True,
            check_interval=30,
        )

    def test_template_fields(self):
        operator = QuickSightCreateIngestionOperator(**self.default_op_kwargs)
        validate_template_fields(operator)
