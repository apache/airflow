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
from airflow.contrib.sensors.aws_redshift_cluster_sensor import AwsRedshiftClusterSensor

try:
    from moto import mock_redshift
except ImportError:
    mock_redshift = None


@mock_redshift
class TestAwsRedshiftClusterSensor(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        client = boto3.client('redshift', region_name='us-east-1')
        client.create_cluster(
            ClusterIdentifier='test_cluster',
            NodeType='dc1.large',
            MasterUsername='admin',
            MasterUserPassword='mock_password'
        )
        if len(client.describe_clusters()['Clusters']) == 0:
            raise ValueError('AWS not properly mocked')

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    def test_poke(self):
        op = AwsRedshiftClusterSensor(task_id='test_cluster_sensor',
                                      poke_interval=1,
                                      timeout=5,
                                      aws_conn_id='aws_default',
                                      cluster_identifier='test_cluster',
                                      target_status='available')
        self.assertTrue(op.poke(None))

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    def test_poke_false(self):
        op = AwsRedshiftClusterSensor(task_id='test_cluster_sensor',
                                      poke_interval=1,
                                      timeout=5,
                                      aws_conn_id='aws_default',
                                      cluster_identifier='test_cluster_not_found',
                                      target_status='available')

        self.assertFalse(op.poke(None))

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    def test_poke_cluster_not_found(self):
        op = AwsRedshiftClusterSensor(task_id='test_cluster_sensor',
                                      poke_interval=1,
                                      timeout=5,
                                      aws_conn_id='aws_default',
                                      cluster_identifier='test_cluster_not_found',
                                      target_status='cluster_not_found')

        self.assertTrue(op.poke(None))


if __name__ == '__main__':
    unittest.main()
