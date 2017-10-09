# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import boto3

from airflow import configuration
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from six import StringIO
from boto.s3.key import Key

try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None


@mock_s3
class TestS3Hook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        hook = S3Hook(aws_conn_id='aws_default')
        conn = hook.get_conn()
        conn.create_bucket(Bucket='airflow-test')
        expected_keys = ["key1", "key2", "key3", "key4"]
        for name in expected_keys:
            conn.put_object(Body='test', Bucket='airflow-test', Key=name)
        if len(conn.list_buckets()['Buckets']) == 0:
            raise ValueError('AWS not properly mocked')

    @unittest.skipIf(mock_s3 is None, 'mock_s3 package not present')
    def test_list_objects_by_last_modified_returns_list_of_objects(self):
        hook = S3Hook(aws_conn_id='aws_default')
        objects = hook.list_objects_by_last_modified('airflow-test', reverse=False)
        order = objects[0]['LastModified'] > objects[1]['LastModified']
        self.assertEqual(len(objects), 4)
        self.assertEqual(order, False)

    @unittest.skipIf(mock_s3 is None, 'mock_s3 package not present')
    def test_list_objects_by_last_modified_returns_list_of_objects_reverse(self):
        hook = S3Hook(aws_conn_id='aws_default')
        objects = hook.list_objects_by_last_modified('airflow-test', reverse=True)
        order = objects[0]['LastModified'] > objects[1]['LastModified']
        self.assertEqual(len(objects), 4)
        self.assertEqual(order, True)

    @unittest.skipIf(mock_s3 is None, 'mock_s3 package not present')
    def test_delete_object_returns_delete_marker(self):
        hook = S3Hook(aws_conn_id='aws_default')
        delete_marker = hook.delete_object('airflow-test', key='key1')
        objects = hook.list_objects_by_last_modified('airflow-test')
        self.assertEqual(len(objects), 3)


if __name__ == '__main__':
    unittest.main()
