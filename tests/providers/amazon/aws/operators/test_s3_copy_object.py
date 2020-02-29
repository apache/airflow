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

import io
import unittest

import boto3
from moto import mock_s3

from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator


class TestS3CopyObjectOperator(unittest.TestCase):

    def setUp(self):
        self.source_bucket = "bucket1"
        self.source_key = "path1/data.txt"
        self.dest_bucket = "bucket2"
        self.dest_key = "path2/data_copy.txt"

    @mock_s3
    def test_s3_copy_object_arg_combination_1(self):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket,
                            Key=self.source_key,
                            Fileobj=io.BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        self.assertFalse('Contents' in conn.list_objects(Bucket=self.dest_bucket,
                                                         Prefix=self.dest_key))

        op = S3CopyObjectOperator(task_id="test_task_s3_copy_object",
                                  source_bucket_key=self.source_key,
                                  source_bucket_name=self.source_bucket,
                                  dest_bucket_key=self.dest_key,
                                  dest_bucket_name=self.dest_bucket)
        op.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket,
                                                   Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)
        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.dest_key)

    @mock_s3
    def test_s3_copy_object_arg_combination_2(self):
        conn = boto3.client('s3')
        conn.create_bucket(Bucket=self.source_bucket)
        conn.create_bucket(Bucket=self.dest_bucket)
        conn.upload_fileobj(Bucket=self.source_bucket,
                            Key=self.source_key,
                            Fileobj=io.BytesIO(b"input"))

        # there should be nothing found before S3CopyObjectOperator is executed
        self.assertFalse('Contents' in conn.list_objects(Bucket=self.dest_bucket,
                                                         Prefix=self.dest_key))

        source_key_s3_url = "s3://{}/{}".format(self.source_bucket, self.source_key)
        dest_key_s3_url = "s3://{}/{}".format(self.dest_bucket, self.dest_key)
        op = S3CopyObjectOperator(task_id="test_task_s3_copy_object",
                                  source_bucket_key=source_key_s3_url,
                                  dest_bucket_key=dest_key_s3_url)
        op.execute(None)

        objects_in_dest_bucket = conn.list_objects(Bucket=self.dest_bucket,
                                                   Prefix=self.dest_key)
        # there should be object found, and there should only be one object found
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)
        # the object found should be consistent with dest_key specified earlier
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], self.dest_key)
