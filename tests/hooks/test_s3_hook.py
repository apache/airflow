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

from airflow import configuration

try:
    from airflow.hooks.S3_hook import S3Hook
except ImportError:
    S3Hook = None


try:
    import boto3
    from moto import mock_s3
except ImportError:
    mock_s3 = None


@unittest.skipIf(S3Hook is None,
                 "Skipping test because S3Hook is not available")
@unittest.skipIf(mock_s3 is None,
                 "Skipping test because moto.mock_s3 is not available")
class TestS3Hook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.s3_test_url = "s3://test/this/is/not/a-real-key.txt"

    def test_parse_s3_url(self):
        parsed = S3Hook.parse_s3_url(self.s3_test_url)
        self.assertEqual(parsed,
                         ("test", "this/is/not/a-real-key.txt"),
                         "Incorrect parsing of the s3 url")

    @mock_s3
    def test_load_string(self):
        hook = S3Hook(aws_conn_id=None)
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket="mybucket")

        hook.load_string(u"Contént", "my_key", "mybucket")
        body = boto3.resource('s3').Object('mybucket', 'my_key').get()['Body'].read()

        self.assertEqual(body, b'Cont\xC3\xA9nt')

    @mock_s3
    def test_read_key(self):
        hook = S3Hook(aws_conn_id=None)
        conn = hook.get_conn()
        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        conn.create_bucket(Bucket='mybucket')
        conn.put_object(Bucket='mybucket', Key='my_key', Body=b'Cont\xC3\xA9nt')

        self.assertEqual(hook.read_key('my_key', 'mybucket'), u'Contént')


if __name__ == '__main__':
    unittest.main()
