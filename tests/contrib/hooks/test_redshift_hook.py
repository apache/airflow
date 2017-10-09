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
from airflow.contrib.hooks.redshift_hook import RedshiftHook
from airflow.contrib.hooks.aws_hook import AwsHook

try:
    from moto import mock_redshift
except ImportError:
    mock_redshift = None

@mock_redshift
class TestRedshiftHook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        client = boto3.client('redshift', region_name='us-east-1')
        client.create_cluster(
            ClusterIdentifier='test_cluster',
            NodeType='dc1.large',
            MasterUsername='admin',
            MasterUserPassword='mock_password'
        )
        client.create_cluster(
            ClusterIdentifier='test_cluster_2',
            NodeType='dc1.large',
            MasterUsername='admin',
            MasterUserPassword='mock_password'
        )
        if len(client.describe_clusters()['Clusters']) == 0:
            raise ValueError('AWS not properly mocked')

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    def test_get_client_type_returns_a_boto3_client_of_the_requested_type(self):
        hook = AwsHook(aws_conn_id='aws_default')
        client_from_hook = hook.get_client_type('redshift')

        clusters = client_from_hook.describe_clusters()['Clusters']
        self.assertEqual(len(clusters), 2)

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    def test_restore_from_cluster_snapshot_returns_dict_with_cluster_data(self):
        hook = RedshiftHook(aws_conn_id='aws_default')
        snapshot = hook.create_cluster_snapshot('test_snapshot', 'test_cluster')
        self.assertEqual(hook.restore_from_cluster_snapshot('test_cluster_3', 'test_snapshot')['ClusterIdentifier'], 'test_cluster_3')

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    def test_delete_cluster_returns_a_dict_with_cluster_data(self):
        hook = RedshiftHook(aws_conn_id='aws_default')

        cluster = hook.delete_cluster('test_cluster_2')
        self.assertNotEqual(cluster, None)

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    def test_create_cluster_snapshot_returns_snapshot_data(self):
        hook = RedshiftHook(aws_conn_id='aws_default')

        snapshot = hook.create_cluster_snapshot('test_snapshot_2', 'test_cluster')
        self.assertNotEqual(snapshot, None)

if __name__ == '__main__':
    unittest.main()
