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
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3_tables import S3TablesHook
from airflow.providers.amazon.aws.operators.s3_tables import (
    S3TablesCreateNamespaceOperator,
    S3TablesCreateTableBucketOperator,
    S3TablesCreateTableOperator,
    S3TablesDeleteNamespaceOperator,
    S3TablesDeleteTableBucketOperator,
    S3TablesDeleteTableOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

TABLE_BUCKET_ARN = "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket"
NAMESPACE = "test_namespace"
TABLE_NAME = "test_table"
TABLE_ARN = "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket/table/test-id"
BUCKET_NAME = "test-table-bucket"
BUCKET_ARN = "arn:aws:s3tables:us-east-1:123456789012:bucket/test-table-bucket"


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


class TestS3TablesCreateTableBucketOperator:
    def setup_method(self):
        self.operator = S3TablesCreateTableBucketOperator(
            task_id="create_table_bucket",
            table_bucket_name=BUCKET_NAME,
        )

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.create_table_bucket.return_value = {"arn": BUCKET_ARN}
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.create_table_bucket.assert_called_once_with(name=BUCKET_NAME)
        assert result == BUCKET_ARN

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_with_encryption(self, mock_conn):
        enc = {"sseAlgorithm": "aws:kms", "kmsKeyArn": "arn:aws:kms:us-east-1:123:key/abc"}
        op = S3TablesCreateTableBucketOperator(
            task_id="create_table_bucket",
            table_bucket_name=BUCKET_NAME,
            encryption_configuration=enc,
            tags={"env": "test"},
        )
        mock_client = mock.MagicMock()
        mock_client.create_table_bucket.return_value = {"arn": BUCKET_ARN}
        mock_conn.return_value = mock_client

        result = op.execute({})

        mock_client.create_table_bucket.assert_called_once_with(
            name=BUCKET_NAME,
            encryptionConfiguration=enc,
            tags={"env": "test"},
        )
        assert result == BUCKET_ARN

    @mock.patch.object(S3TablesHook, "get_table_bucket_arn_by_name", return_value=BUCKET_ARN)
    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_skip_existing(self, mock_conn, mock_get_arn):
        mock_client = mock.MagicMock()
        mock_client.create_table_bucket.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateTableBucket",
        )
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        assert result == BUCKET_ARN
        mock_get_arn.assert_called_once_with(BUCKET_NAME)

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_fail_on_conflict(self, mock_conn):
        op = S3TablesCreateTableBucketOperator(
            task_id="create_table_bucket",
            table_bucket_name=BUCKET_NAME,
            if_exists="fail",
        )
        mock_client = mock.MagicMock()
        mock_client.create_table_bucket.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateTableBucket",
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError):
            op.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestS3TablesDeleteTableBucketOperator:
    def setup_method(self):
        self.operator = S3TablesDeleteTableBucketOperator(
            task_id="delete_table_bucket",
            table_bucket_arn=BUCKET_ARN,
        )

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client

        self.operator.execute({})

        mock_client.delete_table_bucket.assert_called_once_with(tableBucketARN=BUCKET_ARN)

    def test_template_fields(self):
        validate_template_fields(self.operator)


NAMESPACE = "test_namespace"


class TestS3TablesCreateNamespaceOperator:
    def setup_method(self):
        self.operator = S3TablesCreateNamespaceOperator(
            task_id="create_namespace",
            table_bucket_arn=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
        )

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.create_namespace.assert_called_once_with(
            tableBucketARN=TABLE_BUCKET_ARN, namespace=[NAMESPACE]
        )
        assert result == NAMESPACE

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_skip_existing(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.create_namespace.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateNamespace",
        )
        mock_conn.return_value = mock_client

        result = self.operator.execute({})
        assert result == NAMESPACE

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_fail_on_conflict(self, mock_conn):
        op = S3TablesCreateNamespaceOperator(
            task_id="create_namespace",
            table_bucket_arn=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
            if_exists="fail",
        )
        mock_client = mock.MagicMock()
        mock_client.create_namespace.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateNamespace",
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError):
            op.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestS3TablesDeleteNamespaceOperator:
    def setup_method(self):
        self.operator = S3TablesDeleteNamespaceOperator(
            task_id="delete_namespace",
            table_bucket_arn=TABLE_BUCKET_ARN,
            namespace=NAMESPACE,
        )

    @mock.patch.object(S3TablesHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client

        self.operator.execute({})

        mock_client.delete_namespace.assert_called_once_with(
            tableBucketARN=TABLE_BUCKET_ARN, namespace=NAMESPACE
        )

    def test_template_fields(self):
        validate_template_fields(self.operator)
