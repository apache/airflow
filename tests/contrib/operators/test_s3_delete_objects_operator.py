# -*- coding: utf-8 -*-
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

from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator


class TestS3DeleteObjectsOperator(unittest.TestCase):

    @mock_s3
    def test_s3_delete_single_object(self):
        bucket = "testbucket"
        key = "path/data.txt"

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        conn.upload_fileobj(Bucket=bucket,
                            Key=key,
                            Fileobj=io.BytesIO(b"input"))

        # The object should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket,
                                                   Prefix=key)
        self.assertEqual(len(objects_in_dest_bucket['Contents']), 1)
        self.assertEqual(objects_in_dest_bucket['Contents'][0]['Key'], key)

        t = S3DeleteObjectsOperator(task_id="test_task_s3_delete_single_object",
                                    bucket=bucket,
                                    keys=key)
        t.execute(None)

        # There should be no object found in the bucket created earlier
        self.assertFalse('Contents' in conn.list_objects(Bucket=bucket,
                                                         Prefix=key))

    @mock_s3
    def test_s3_delete_multiple_objects(self):
        bucket = "testbucket"
        key_pattern = "path/data"
        n_keys = 3
        keys = [key_pattern + str(i) for i in range(n_keys)]

        conn = boto3.client('s3')
        conn.create_bucket(Bucket=bucket)
        for k in keys:
            conn.upload_fileobj(Bucket=bucket,
                                Key=k,
                                Fileobj=io.BytesIO(b"input"))

        # The objects should be detected before the DELETE action is taken
        objects_in_dest_bucket = conn.list_objects(Bucket=bucket,
                                                   Prefix=key_pattern)
        self.assertEqual(len(objects_in_dest_bucket['Contents']), n_keys)
        self.assertEqual(sorted([x['Key'] for x in objects_in_dest_bucket['Contents']]),
                         sorted(keys))

        t = S3DeleteObjectsOperator(task_id="test_task_s3_delete_multiple_objects",
                                    bucket=bucket,
                                    keys=keys)
        t.execute(None)

        # There should be no object found in the bucket created earlier
        self.assertFalse('Contents' in conn.list_objects(Bucket=bucket,
                                                         Prefix=key_pattern))
