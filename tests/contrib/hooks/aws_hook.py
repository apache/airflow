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
from airflow.contrib.hooks.aws_hook import AwsHook


try:
    from moto import mock_emr
except ImportError:
    mock_emr = None


class TestAwsHook(unittest.TestCase):
    @mock_emr
    def setUp(self):
        configuration.test_mode()

    @unittest.skipIf(mock_emr is None, 'mock_emr package not present')
    @mock_emr
    def test_get_client_type_returns_a_boto3_client_of_the_requested_type(self):
        client = boto3.client('emr', region_name='us-east-1')
        if len(client.list_clusters()['Clusters']):
            raise ValueError('AWS not properly mocked')

        hook = AwsHook(aws_conn_id='aws_default')
        client_from_hook = hook.get_client_type('emr')

        self.assertEqual(client_from_hook.list_clusters()['Clusters'], [])

if __name__ == '__main__':
    unittest.main()
