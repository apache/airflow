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

import io
import unittest
from unittest import mock

import boto3
import pytest
from moto import mock_s3

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3CreateObjectOperator,
    S3DeleteObjectsOperator,
)

S3_BUCKET = "test-airflow-bucket"
S3_KEY = "test-airflow-key"
TASK_ID = "test-s3-operator"


class TestS3CopyObjectOperator(unittest.TestCase):
    def setUp(self):
        self.source_bucket = "bucket1"
        self.source_key = "path1/data.txt"
        self.dest_bucket = "bucket2"
        self.dest_key = "path2/data_copy.txt"

    @mock_s3
    def test_s3_copy_object_arg_combination_1(self):
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket, Key=self.source_key, Fileobj=io.BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        assert "Contents" not in conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)

        op = S3CopyObjectOperator(
            task_id="test_task_s3_copy_object",
            source_bucket_key=self.source_key,
            source_bucket_name=self.source_bucket,
            dest_bucket_key=self.dest_key,
            dest_bucket_name=self.dest_bucket,
        )
        op.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == self.dest_key

    @mock_s3
    def test_s3_copy_object_arg_combination_2(self):
        conn = boto3.client("s3")
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket, Key=self.source_key, Fileobj=io.BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        assert "Contents" not in conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)

        source_key_s3_url = f"s3://{self.source_bucket}/{self.source_key}"
        dest_key_s3_url = f"s3://{self.dest_bucket}/{self.dest_key}"
        op = S3CopyObjectOperator(
            task_id="test_task_s3_copy_object",
            source_bucket_key=source_key_s3_url,
            dest_bucket_key=dest_key_s3_url,
        )
        op.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket, Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == self.dest_key


@mock_s3
class TestS3DeleteObjectsOperator:
    def test_s3_delete_single_object(self):
        bucket = "testbucket"
        key = "path/data.txt"

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key, Fileobj=io.BytesIO(b"input"))

        # The object should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_single_object", bucket=bucket, keys=key)
        op.execute(None)

        # There should be no object found in the bucket created earlier
        assert "Contents" not in conn.list_objects(Bucket=bucket, Prefix=key)

    def test_s3_delete_multiple_objects(self):
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket, Key=k, Fileobj=io.BytesIO(b"input"))

        # The objects should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_pattern)
        assert len(objects_in_dest_bucket["Contents"]) == n_keys
        assert sorted(x["Key"] for x in objects_in_dest_bucket["Contents"]) == sorted(keys)

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_multiple_objects", bucket=bucket, keys=keys)
        op.execute(None)

        # There should be no object found in the bucket created earlier
        assert "Contents" not in conn.list_objects(Bucket=bucket, Prefix=key_pattern)

    def test_s3_delete_prefix(self):
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket, Key=k, Fileobj=io.BytesIO(b"input"))

        # The objects should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_pattern)
        assert len(objects_in_dest_bucket["Contents"]) == n_keys
        assert sorted(x["Key"] for x in objects_in_dest_bucket["Contents"]) == sorted(keys)

        op = S3DeleteObjectsOperator(task_id="test_task_s3_delete_prefix", bucket=bucket, prefix=key_pattern)
        op.execute(None)

        # There should be no object found in the bucket created earlier
        assert "Contents" not in conn.list_objects(Bucket=bucket, Prefix=key_pattern)

    def test_s3_delete_empty_list(self):
        bucket = "testbucket"
        key_of_test = "path/data.txt"
        keys = []

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key_of_test, Fileobj=io.BytesIO(b"input"))

        # The object should be detected before the DELETE action is tested
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_of_test)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

        op = S3DeleteObjectsOperator(task_id="test_s3_delete_empty_list", bucket=bucket, keys=keys)
        op.execute(None)

        # The object found in the bucket created earlier should still be there
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

    def test_s3_delete_empty_string(self):
        bucket = "testbucket"
        key_of_test = "path/data.txt"
        keys = ""

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key_of_test, Fileobj=io.BytesIO(b"input"))

        # The object should be detected before the DELETE action is tested
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_of_test)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

        op = S3DeleteObjectsOperator(task_id="test_s3_delete_empty_string", bucket=bucket, keys=keys)
        op.execute(None)

        # The object found in the bucket created earlier should still be there
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

    @pytest.mark.parametrize(
        "keys, prefix",
        [
            pytest.param("path/data.txt", "path/data", id="single-key-and-prefix"),
            pytest.param(["path/data.txt"], "path/data", id="multiple-keys-and-prefix"),
            pytest.param(None, None, id="both-none"),
        ],
    )
    def test_validate_keys_and_prefix_in_constructor(self, keys, prefix):
        with pytest.raises(AirflowException, match=r"Either keys or prefix should be set\."):
            S3DeleteObjectsOperator(
                task_id="test_validate_keys_and_prefix_in_constructor",
                bucket="foo-bar-bucket",
                keys=keys,
                prefix=prefix,
            )

    @pytest.mark.parametrize(
        "keys, prefix",
        [
            pytest.param("path/data.txt", "path/data", id="single-key-and-prefix"),
            pytest.param(["path/data.txt"], "path/data", id="multiple-keys-and-prefix"),
            pytest.param(None, None, id="both-none"),
        ],
    )
    def test_validate_keys_and_prefix_in_execute(self, keys, prefix):
        bucket = "testbucket"
        key_of_test = "path/data.txt"

        conn = boto3.client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket, Key=key_of_test, Fileobj=io.BytesIO(b"input"))

        # Set valid values for constructor, and change them later for emulate rendering template
        op = S3DeleteObjectsOperator(
            task_id="test_validate_keys_and_prefix_in_execute",
            bucket=bucket,
            keys="keys-exists",
            prefix=None,
        )
        op.keys = keys
        op.prefix = prefix

        # The object should be detected before the DELETE action is tested
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket, Prefix=key_of_test)
        assert len(objects_in_dest_bucket["Contents"]) == 1
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test

        with pytest.raises(AirflowException, match=r"Either keys or prefix should be set\."):
            op.execute(None)

        # The object found in the bucket created earlier should still be there
        assert len(objects_in_dest_bucket["Contents"]) == 1
        # the object found should be consistent with dest_key specified earlier
        assert objects_in_dest_bucket["Contents"][0]["Key"] == key_of_test


class TestS3CreateObjectOperator(unittest.TestCase):
    @mock.patch.object(S3Hook, "load_string")
    def test_execute_if_data_is_string(self, mock_load_string):
        data = "data"
        operator = S3CreateObjectOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY,
            data=data,
        )
        operator.execute(None)

        mock_load_string.assert_called_once_with(data, S3_KEY, S3_BUCKET, False, False, None, None, None)

    @mock.patch.object(S3Hook, "load_bytes")
    def test_execute_if_data_is_bytes(self, mock_load_bytes):
        data = b"data"
        operator = S3CreateObjectOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY,
            data=data,
        )
        operator.execute(None)

        mock_load_bytes.assert_called_once_with(data, S3_KEY, S3_BUCKET, False, False, None)

    @mock.patch.object(S3Hook, "load_string")
    def test_execute_if_s3_bucket_not_provided(self, mock_load_string):
        data = "data"
        operator = S3CreateObjectOperator(
            task_id=TASK_ID,
            s3_key=f"s3://{S3_BUCKET}/{S3_KEY}",
            data=data,
        )
        operator.execute(None)

        mock_load_string.assert_called_once_with(data, S3_KEY, S3_BUCKET, False, False, None, None, None)
