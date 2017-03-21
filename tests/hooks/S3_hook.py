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
import mock
import boto

from airflow import configuration
from airflow import models

try:
    from airflow.hooks.S3_hook import S3Hook
except ImportError:
    S3Hook = None

try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None


@unittest.skipIf(S3Hook is None,
                 "Skipping test because S3Hook is not installed")
class S3HookTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.s3_test_url = "s3://test/this/is/not/a-real-key.txt"

    def test_parse_s3_url(self):
        parsed = S3Hook.parse_s3_url(self.s3_test_url)
        self.assertEqual(parsed,
                         ("test", "this/is/not/a-real-key.txt"),
                         "Incorrect parsing of the s3 url")


@unittest.skipIf(S3Hook is None,
                 "Skipping test because S3Hook is not installed")
@unittest.skipIf(mock_s3 is None, 'Skipping test because mock_s3 package not present')
class S3HookMotoTest(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    @mock.patch('airflow.hooks.S3_hook.S3Hook.get_connections')
    @mock_s3
    def test_s3_hook_load_file(self, mock_get_connections):
        conn = boto.connect_s3()
        conn.create_bucket('test_bucket')

        c = models.Connection(conn_id='s3_conn', conn_type='S3')
        mock_get_connections.return_value = [c]

        s3_hook = S3Hook(s3_conn_id='s3_conn')
        s3_hook.load_file(filename=__file__, key="test_s3_key.tmp",
                          bucket_name="test_bucket", replace=True)

        keys = s3_hook.list_keys(bucket_name="test_bucket")
        self.assertIn("test_s3_key.tmp", keys)


if __name__ == '__main__':
    unittest.main()
