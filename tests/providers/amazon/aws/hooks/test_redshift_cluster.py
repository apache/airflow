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
from __future__ import annotations

import boto3
from moto import mock_redshift

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook


class TestRedshiftHook:
    @staticmethod
    def _create_clusters():
        client = boto3.client("redshift", region_name="us-east-1")
        client.create_cluster(
            ClusterIdentifier="test_cluster",
            NodeType="dc1.large",
            MasterUsername="admin",
            MasterUserPassword="mock_password",
        )
        client.create_cluster(
            ClusterIdentifier="test_cluster_2",
            NodeType="dc1.large",
            MasterUsername="admin",
            MasterUserPassword="mock_password",
        )
        if not client.describe_clusters()["Clusters"]:
            raise ValueError("AWS not properly mocked")

    @mock_redshift
    def test_get_client_type_returns_a_boto3_client_of_the_requested_type(self):
        self._create_clusters()
        hook = AwsBaseHook(aws_conn_id="aws_default", client_type="redshift")
        client_from_hook = hook.get_conn()

        clusters = client_from_hook.describe_clusters()["Clusters"]
        assert len(clusters) == 2

    @mock_redshift
    def test_restore_from_cluster_snapshot_returns_dict_with_cluster_data(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id="aws_default")
        hook.create_cluster_snapshot("test_snapshot", "test_cluster")
        assert (
            hook.restore_from_cluster_snapshot("test_cluster_3", "test_snapshot")["ClusterIdentifier"]
            == "test_cluster_3"
        )

    @mock_redshift
    def test_delete_cluster_returns_a_dict_with_cluster_data(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id="aws_default")

        cluster = hook.delete_cluster("test_cluster_2")
        assert cluster is not None

    @mock_redshift
    def test_create_cluster_snapshot_returns_snapshot_data(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id="aws_default")

        snapshot = hook.create_cluster_snapshot("test_snapshot_2", "test_cluster")
        assert snapshot is not None

    @mock_redshift
    def test_cluster_status_returns_cluster_not_found(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id="aws_default")
        status = hook.cluster_status("test_cluster_not_here")
        assert status == "cluster_not_found"

    @mock_redshift
    def test_cluster_status_returns_available_cluster(self):
        self._create_clusters()
        hook = RedshiftHook(aws_conn_id="aws_default")
        status = hook.cluster_status("test_cluster")
        assert status == "available"
