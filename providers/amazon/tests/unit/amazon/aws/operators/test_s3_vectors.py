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

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.operators.s3_vectors import (
    S3VectorsCreateVectorBucketOperator,
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
