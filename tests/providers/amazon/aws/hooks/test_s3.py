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
#

import unittest

import boto3
import pytest

from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None


@unittest.skipIf(mock_s3 is None, 'moto package not present')
class TestAwsS3Hook(unittest.TestCase):

    def setUp(self):
        self.s3 = boto3.resource('s3')

    def create_bucket(self):
        bucket = 'airflow-test-s3-bucket'
        self.s3.create_bucket(Bucket=bucket)
        return bucket

    @mock_s3
    def test_get_conn(self):
        hook = S3Hook()
        self.assertIsNotNone(hook.get_conn())

    @mock_s3
    def test_delete_objects_key_does_not_exist(self):
        bucket = self.create_bucket()
        hook = S3Hook()
        with pytest.raises(AirflowException) as err:
            hook.delete_objects(bucket=bucket, keys=['key-1'])

        assert isinstance(err.value, AirflowException)
        assert "Errors when deleting: ['key-1']" == str(err.value)

    @mock_s3
    def test_delete_objects_one_key(self):
        bucket = self.create_bucket()
        key = 'key-1'
        self.s3.Object(bucket, key).put(Body=b'Data')
        hook = S3Hook()
        hook.delete_objects(bucket=bucket, keys=[key])
        self.assertListEqual([o.key for o in self.s3.Bucket(bucket).objects.all()], [])

    @mock_s3
    def test_delete_objects_many_keys(self):
        bucket = self.create_bucket()

        num_keys_to_remove = 1001
        keys = []
        for index in range(num_keys_to_remove):
            key = 'key-{}'.format(index)
            self.s3.Object(bucket, key).put(Body=b'Data')
            keys.append(key)

        self.assertEqual(num_keys_to_remove, len([o for o in self.s3.Bucket(bucket).objects.all()]))
        hook = S3Hook()
        hook.delete_objects(bucket=bucket, keys=keys)
        self.assertListEqual([o.key for o in self.s3.Bucket(bucket).objects.all()], [])


if __name__ == '__main__':
    unittest.main()
