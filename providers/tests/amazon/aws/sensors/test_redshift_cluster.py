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
import pytest
from moto import mock_aws

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.triggers.redshift_cluster import RedshiftClusterTrigger

MODULE = "airflow.providers.amazon.aws.sensors.redshift_cluster"


@pytest.fixture
def deferrable_op():
    return RedshiftClusterSensor(
        task_id="test_cluster_sensor",
        cluster_identifier="test_cluster",
        target_status="available",
        deferrable=True,
    )


class TestRedshiftClusterSensor:
    @staticmethod
    def _create_cluster():
        client = boto3.client("redshift", region_name="us-east-1")
        client.create_cluster(
            ClusterIdentifier="test_cluster",
            NodeType="dc1.large",
            MasterUsername="admin",
            MasterUserPassword="mock_password",
        )
        if not client.describe_clusters()["Clusters"]:
            raise ValueError("AWS not properly mocked")

    @mock_aws
    def test_poke(self):
        self._create_cluster()
        op = RedshiftClusterSensor(
            task_id="test_cluster_sensor",
            poke_interval=1,
            timeout=5,
            aws_conn_id="aws_default",
            cluster_identifier="test_cluster",
            target_status="available",
        )
        assert op.poke({})

    @mock_aws
    def test_poke_false(self):
        self._create_cluster()
        op = RedshiftClusterSensor(
            task_id="test_cluster_sensor",
            poke_interval=1,
            timeout=5,
            aws_conn_id="aws_default",
            cluster_identifier="test_cluster_not_found",
            target_status="available",
        )

        assert not op.poke({})

    @mock_aws
    def test_poke_cluster_not_found(self):
        self._create_cluster()
        op = RedshiftClusterSensor(
            task_id="test_cluster_sensor",
            poke_interval=1,
            timeout=5,
            aws_conn_id="aws_default",
            cluster_identifier="test_cluster_not_found",
            target_status="cluster_not_found",
        )

        assert op.poke({})

    @mock.patch(f"{MODULE}.RedshiftClusterSensor.defer")
    @mock.patch(f"{MODULE}.RedshiftClusterSensor.poke", return_value=True)
    def test_execute_finish_before_deferred(
        self,
        mock_poke,
        mock_defer,
        deferrable_op,
    ):
        """Assert task is not deferred when it receives a finish status before deferring"""

        deferrable_op.execute({})
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.RedshiftClusterSensor.poke", return_value=False)
    def test_execute(self, mock_poke, deferrable_op):
        """Test RedshiftClusterSensor that a task with wildcard=True
        is deferred and an RedshiftClusterTrigger will be fired when executed method is called"""

        with pytest.raises(TaskDeferred) as exc:
            deferrable_op.execute(None)
        assert isinstance(
            exc.value.trigger, RedshiftClusterTrigger
        ), "Trigger is not a RedshiftClusterTrigger"

    def test_redshift_sensor_async_execute_failure(self, deferrable_op):
        """Test RedshiftClusterSensor with an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            deferrable_op.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    def test_redshift_sensor_async_execute_complete(self, deferrable_op):
        """Asserts that logging occurs as expected"""

        with mock.patch.object(deferrable_op.log, "info") as mock_log_info:
            deferrable_op.execute_complete(
                context=None, event={"status": "success", "cluster_state": "available"}
            )
        mock_log_info.assert_called_with(
            "Cluster Identifier %s is in %s state", "test_cluster", "available"
        )
