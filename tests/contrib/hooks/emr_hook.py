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
from airflow.contrib.hooks.emr_hook import EmrHook


try:
    from moto import mock_emr
except ImportError:
    mock_emr = None


class TestEmrHook(unittest.TestCase):
    @mock_emr
    def setUp(self):
        configuration.load_test_config()

    @unittest.skipIf(mock_emr is None, 'mock_emr package not present')
    @mock_emr
    def test_get_conn_returns_a_boto3_connection(self):
        hook = EmrHook(aws_conn_id='aws_default')
        self.assertIsNotNone(hook.get_conn().list_clusters())

    @unittest.skipIf(mock_emr is None, 'mock_emr package not present')
    @mock_emr
    def test_create_job_flow_uses_the_emr_config_to_create_a_cluster(self):
        client = boto3.client('emr', region_name='us-east-1')
        if len(client.list_clusters()['Clusters']):
            raise ValueError('AWS not properly mocked')

        hook = EmrHook(aws_conn_id='aws_default', emr_conn_id='emr_default')
        cluster = hook.create_job_flow({'Name': 'test_cluster'})

        self.assertEqual(client.list_clusters()['Clusters'][0]['Id'], cluster['JobFlowId'])

if __name__ == '__main__':
    unittest.main()
