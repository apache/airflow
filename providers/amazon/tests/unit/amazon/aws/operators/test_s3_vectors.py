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
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.operators.s3_vectors import (
    S3VectorsCreateIndexOperator,
    S3VectorsCreateVectorBucketOperator,
    S3VectorsDeleteIndexOperator,
    S3VectorsDeleteVectorBucketOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

BUCKET_NAME = "test-vector-bucket"
BUCKET_ARN = "arn:aws:s3vectors:us-east-1:123456789012:bucket/test-vector-bucket"


class TestS3VectorsCreateVectorBucketOperator:
    def test_execute(self):
        op = S3VectorsCreateVectorBucketOperator(
            task_id="create_vector_bucket",
            vector_bucket_name=BUCKET_NAME,
        )
        mock_conn = MagicMock()
        mock_conn.create_vector_bucket.return_value = {"vectorBucketArn": BUCKET_ARN}
        op.hook.conn = mock_conn

        result = op.execute({})

        mock_conn.create_vector_bucket.assert_called_once_with(vectorBucketName=BUCKET_NAME)
        assert result == BUCKET_ARN

    def test_execute_with_all_params(self):
        encryption = {"sseType": "aws:kms", "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/abc"}
        op = S3VectorsCreateVectorBucketOperator(
            task_id="create_vector_bucket",
            vector_bucket_name=BUCKET_NAME,
            encryption_configuration=encryption,
            tags={"env": "test"},
        )
        mock_conn = MagicMock()
        mock_conn.create_vector_bucket.return_value = {"vectorBucketArn": BUCKET_ARN}
        op.hook.conn = mock_conn

        result = op.execute({})

        mock_conn.create_vector_bucket.assert_called_once_with(
            vectorBucketName=BUCKET_NAME,
            encryptionConfiguration=encryption,
            tags={"env": "test"},
        )
        assert result == BUCKET_ARN

    def test_execute_skip_existing(self):
        op = S3VectorsCreateVectorBucketOperator(
            task_id="create_vector_bucket",
            vector_bucket_name=BUCKET_NAME,
            if_exists="skip",
        )
        mock_conn = MagicMock()
        mock_conn.create_vector_bucket.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateVectorBucket",
        )
        mock_conn.get_vector_bucket.return_value = {"vectorBucketArn": BUCKET_ARN}
        op.hook.conn = mock_conn

        result = op.execute({})

        assert result == BUCKET_ARN

    def test_template_fields(self):
        op = S3VectorsCreateVectorBucketOperator(
            task_id="test",
            vector_bucket_name=BUCKET_NAME,
        )
        validate_template_fields(op)


class TestS3VectorsDeleteVectorBucketOperator:
    def test_execute(self):
        op = S3VectorsDeleteVectorBucketOperator(
            task_id="delete_vector_bucket",
            vector_bucket_name=BUCKET_NAME,
        )
        mock_conn = MagicMock()
        op.hook.conn = mock_conn

        op.execute({})

        mock_conn.delete_vector_bucket.assert_called_once_with(vectorBucketName=BUCKET_NAME)

    def test_template_fields(self):
        op = S3VectorsDeleteVectorBucketOperator(
            task_id="test",
            vector_bucket_name=BUCKET_NAME,
        )
        validate_template_fields(op)


INDEX_NAME = "test-index"
INDEX_ARN = "arn:aws:s3vectors:us-east-1:123456789012:vector-bucket/test-bucket/index/test-index"


class TestS3VectorsCreateIndexOperator:
    def setup_method(self):
        self.operator = S3VectorsCreateIndexOperator(
            task_id="create_index",
            vector_bucket_name=BUCKET_NAME,
            index_name=INDEX_NAME,
            data_type="float32",
            dimension=128,
        )

    def test_execute(self):
        mock_conn = MagicMock()
        mock_conn.create_index.return_value = {"indexArn": INDEX_ARN}
        self.operator.hook.conn = mock_conn

        result = self.operator.execute({})

        mock_conn.create_index.assert_called_once_with(
            vectorBucketName=BUCKET_NAME,
            indexName=INDEX_NAME,
            dataType="float32",
            dimension=128,
            distanceMetric="cosine",
        )
        assert result == INDEX_ARN

    def test_execute_with_metadata_config(self):
        meta_config = {"nonFilterableMetadataKeys": ["key1"]}
        op = S3VectorsCreateIndexOperator(
            task_id="create_index",
            vector_bucket_name=BUCKET_NAME,
            index_name=INDEX_NAME,
            data_type="float32",
            dimension=128,
            distance_metric="euclidean",
            metadata_configuration=meta_config,
        )
        mock_conn = MagicMock()
        mock_conn.create_index.return_value = {"indexArn": INDEX_ARN}
        op.hook.conn = mock_conn

        result = op.execute({})

        call_kwargs = mock_conn.create_index.call_args[1]
        assert call_kwargs["distanceMetric"] == "euclidean"
        assert call_kwargs["metadataConfiguration"] == meta_config
        assert result == INDEX_ARN

    def test_execute_skip_existing(self):
        mock_conn = MagicMock()
        mock_conn.create_index.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateIndex",
        )
        mock_conn.get_index.return_value = {"index": {"indexArn": INDEX_ARN}}
        self.operator.hook.conn = mock_conn

        result = self.operator.execute({})
        assert result == INDEX_ARN

    def test_execute_fail_on_conflict(self):
        op = S3VectorsCreateIndexOperator(
            task_id="create_index",
            vector_bucket_name=BUCKET_NAME,
            index_name=INDEX_NAME,
            data_type="float32",
            dimension=128,
            if_exists="fail",
        )
        mock_conn = MagicMock()
        mock_conn.create_index.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "Already exists"}},
            "CreateIndex",
        )
        op.hook.conn = mock_conn

        with pytest.raises(ClientError):
            op.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestS3VectorsDeleteIndexOperator:
    def setup_method(self):
        self.operator = S3VectorsDeleteIndexOperator(
            task_id="delete_index",
            vector_bucket_name=BUCKET_NAME,
            index_name=INDEX_NAME,
        )

    def test_execute(self):
        mock_conn = MagicMock()
        self.operator.hook.conn = mock_conn

        self.operator.execute({})

        mock_conn.delete_index.assert_called_once_with(vectorBucketName=BUCKET_NAME, indexName=INDEX_NAME)

    def test_template_fields(self):
        validate_template_fields(self.operator)
