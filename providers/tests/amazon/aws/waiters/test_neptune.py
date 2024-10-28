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

from unittest import mock

import boto3
import botocore
import pytest

from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook


class TestCustomNeptuneWaiters:
    """Test waiters from ``amazon/aws/waiters/neptune.json``."""

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("neptune", region_name="eu-west-3")
        monkeypatch.setattr(NeptuneHook, "conn", self.client)

    def test_service_waiters(self):
        hook_waiters = NeptuneHook(aws_conn_id=None).list_waiters()
        assert "cluster_available" in hook_waiters

    @pytest.fixture
    def mock_describe_clusters(self):
        with mock.patch.object(self.client, "describe_db_clusters") as m:
            yield m

    @staticmethod
    def get_status_response(status):
        return {"DBClusters": [{"Status": status}]}

    def test_cluster_available(self, mock_describe_clusters):
        mock_describe_clusters.return_value = {"DBClusters": [{"Status": "available"}]}
        waiter = NeptuneHook(aws_conn_id=None).get_waiter("cluster_available")
        waiter.wait(DBClusterIdentifier="test_cluster")

    def test_cluster_available_failed(self, mock_describe_clusters):
        mock_describe_clusters.return_value = {
            "DBClusters": [{"Status": "migration-failed"}]
        }
        waiter = NeptuneHook(aws_conn_id=None).get_waiter("cluster_available")
        with pytest.raises(botocore.exceptions.WaiterError):
            waiter.wait(DBClusterIdentifier="test_cluster")

    def test_starting_up(self, mock_describe_clusters):
        """Test job succeeded"""
        mock_describe_clusters.side_effect = [
            self.get_status_response("stopped"),
            self.get_status_response("starting"),
            self.get_status_response("available"),
        ]
        waiter = NeptuneHook(aws_conn_id=None).get_waiter("cluster_available")
        waiter.wait(
            cluster_identifier="test_cluster",
            WaiterConfig={"Delay": 0.2, "MaxAttempts": 4},
        )

    def test_cluster_stopped(self, mock_describe_clusters):
        mock_describe_clusters.return_value = {"DBClusters": [{"Status": "stopped"}]}
        waiter = NeptuneHook(aws_conn_id=None).get_waiter("cluster_stopped")
        waiter.wait(DBClusterIdentifier="test_cluster")

    def test_cluster_stopped_failed(self, mock_describe_clusters):
        mock_describe_clusters.return_value = {
            "DBClusters": [{"Status": "migration-failed"}]
        }
        waiter = NeptuneHook(aws_conn_id=None).get_waiter("cluster_stopped")
        with pytest.raises(botocore.exceptions.WaiterError):
            waiter.wait(DBClusterIdentifier="test_cluster")

    def test_stopping(self, mock_describe_clusters):
        mock_describe_clusters.side_effect = [
            self.get_status_response("available"),
            self.get_status_response("stopping"),
            self.get_status_response("stopped"),
        ]
        waiter = NeptuneHook(aws_conn_id=None).get_waiter("cluster_stopped")
        waiter.wait(
            cluster_identifier="test_cluster",
            WaiterConfig={"Delay": 0.2, "MaxAttempts": 4},
        )
