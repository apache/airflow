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

import json
import unittest
from unittest import mock

import boto3
from parameterized import parameterized

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook, RedshiftSQLHook

try:
    from moto import mock_redshift
except ImportError:
    mock_redshift = None


class TestRedshiftHook(unittest.TestCase):
    @staticmethod
    def _create_clusters():
        client = boto3.client('redshift', region_name='us-east-1')
        client.create_cluster(
            ClusterIdentifier='test_cluster',
            NodeType='dc1.large',
            MasterUsername='admin',
            MasterUserPassword='mock_password',
        )
        client.create_cluster(
            ClusterIdentifier='test_cluster_2',
            NodeType='dc1.large',
            MasterUsername='admin',
            MasterUserPassword='mock_password',
        )
        if not client.describe_clusters()['Clusters']:
            raise ValueError('AWS not properly mocked')

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    @mock_redshift
    def test_get_client_type_returns_a_boto3_client_of_the_requested_type(self):
        self._create_clusters()
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='redshift')
        client_from_hook = hook.get_conn()

        clusters = client_from_hook.describe_clusters()['Clusters']
        assert len(clusters) == 2

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    @mock_redshift
    def test_restore_from_cluster_snapshot_returns_dict_with_cluster_data(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id='aws_default')
        hook.create_cluster_snapshot('test_snapshot', 'test_cluster')
        assert (
            hook.restore_from_cluster_snapshot('test_cluster_3', 'test_snapshot')['ClusterIdentifier']
            == 'test_cluster_3'
        )

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    @mock_redshift
    def test_delete_cluster_returns_a_dict_with_cluster_data(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id='aws_default')

        cluster = hook.delete_cluster('test_cluster_2')
        assert cluster is not None

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    @mock_redshift
    def test_create_cluster_snapshot_returns_snapshot_data(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id='aws_default')

        snapshot = hook.create_cluster_snapshot('test_snapshot_2', 'test_cluster')
        assert snapshot is not None

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    @mock_redshift
    def test_cluster_status_returns_cluster_not_found(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id='aws_default')
        status = hook.cluster_status('test_cluster_not_here')
        assert status == 'cluster_not_found'

    @unittest.skipIf(mock_redshift is None, 'mock_redshift package not present')
    @mock_redshift
    def test_cluster_status_returns_available_cluster(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id='aws_default')
        status = hook.cluster_status('test_cluster')
        assert status == 'available'


class TestRedshiftSQLHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(
            conn_type='redshift', login='login', password='password', host='host', port=5439, schema="dev"
        )

        self.db_hook = RedshiftSQLHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    def test_get_uri(self):
        expected = 'redshift+redshift_connector://login:password@host:5439/dev'
        x = self.db_hook.get_uri()
        assert x == expected

    @mock.patch('airflow.providers.amazon.aws.hooks.redshift.redshift_connector.connect')
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user='login', password='password', host='host', port=5439, database='dev'
        )

    @mock.patch('airflow.providers.amazon.aws.hooks.redshift.redshift_connector.connect')
    def test_get_conn_extra(self, mock_connect):
        self.connection.extra = json.dumps(
            {
                "iam": True,
                "cluster_identifier": "my-test-cluster",
                "profile": "default",
            }
        )
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user='login',
            password='password',
            host='host',
            port=5439,
            cluster_identifier="my-test-cluster",
            profile="default",
            database='dev',
            iam=True,
        )

    @parameterized.expand(
        [
            ({}, {}, {}),
            ({"login": "test"}, {}, {"user": "test"}),
            ({}, {"user": "test"}, {"user": "test"}),
            ({"login": "original"}, {"user": "overridden"}, {"user": "overridden"}),
            ({"login": "test1"}, {"password": "test2"}, {"user": "test1", "password": "test2"}),
        ],
    )
    @mock.patch('airflow.providers.amazon.aws.hooks.redshift.redshift_connector.connect')
    def test_get_conn_overrides_correctly(self, conn_params, conn_extra, expected_call_args, mock_connect):
        with mock.patch(
            'airflow.providers.amazon.aws.hooks.redshift.RedshiftSQLHook.conn',
            Connection(conn_type='redshift', extra=conn_extra, **conn_params),
        ):
            self.db_hook.get_conn()
            mock_connect.assert_called_once_with(**expected_call_args)
