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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.s3_tables import (
    S3TablesCreateTableOperator,
    S3TablesDeleteTableOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

TABLE_BUCKET_ARN = "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket"
NAMESPACE = "test_namespace"
TABLE_NAME = "test_table"
TABLE_ARN = "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-id"


class TestS3TablesCreateTableOperator:
    def setup_method(self):
        self.operator = S3TablesCreateTableOperator(
            task_id="test-create-table",
            table_bucket_arn=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            table_name=TABLE_NAME,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.create_table.return_value = {"tableARN": TABLE_ARN, "versionToken": "v1"}
        mock_conn.return_value = mock_client

        result = self.operator.execute({})
        mock_client.create_table.assert_called_once_with(
            tableBucketARN=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            name=TABLE_NAME,
            format="ICEBERG",
        )
        assert result == TABLE_ARN

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_metadata(self, mock_conn):
        metadata = {"iceberg": {"schema": {"fields": [{"name": "id", "type": "int", "required": True}]}}}
        op = S3TablesCreateTableOperator(
            task_id="test-with-metadata",
            table_bucket_arn=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            table_name=TABLE_NAME,
            metadata=metadata,
        )
        mock_client = mock.MagicMock()
        mock_client.create_table.return_value = {"tableARN": TABLE_ARN, "versionToken": "v1"}
        mock_conn.return_value = mock_client

        op.execute({})
        mock_client.create_table.assert_called_once_with(
            tableBucketARN=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            name=TABLE_NAME,
            format="ICEBERG",
            metadata=metadata,
        )

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestS3TablesDeleteTableOperator:
    def setup_method(self):
        self.operator = S3TablesDeleteTableOperator(
            task_id="test-delete-table",
            table_bucket_arn=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            table_name=TABLE_NAME,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client

        self.operator.execute({})
        mock_client.delete_table.assert_called_once_with(
            tableBucketARN=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            name=TABLE_NAME,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_version_token(self, mock_conn):
        op = S3TablesDeleteTableOperator(
            task_id="test-delete-with-token",
            table_bucket_arn=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            table_name=TABLE_NAME,
            version_token="v1",
        )
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client

        op.execute({})
        mock_client.delete_table.assert_called_once_with(
            tableBucketARN=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            name=TABLE_NAME,
            versionToken="v1",
        )

    def test_template_fields(self):
        validate_template_fields(self.operator)
