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
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.dms import DmsHook


class TestCustomDmsWaiters:
    """Test waiters from ``amazon/aws/waiters/dms.json```."""

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("dms", region_name="us-east-1")
        monkeypatch.setattr(DmsHook, "conn", self.client)

    def test_service_waiters(self):
        hook_waiters = DmsHook(aws_conn_id=None).list_waiters()
        assert "replication_terminal_status" in hook_waiters
        assert "replication_config_deleted" in hook_waiters
        assert "replication_stopped" in hook_waiters
        assert "replication_complete" in hook_waiters
        assert "replication_task_modified" in hook_waiters
        assert "table_reload_complete" in hook_waiters
        assert "table_validation_complete" in hook_waiters

    @pytest.fixture
    def mock_describe_replication(self):
        with mock.patch.object(self.client, "describe_replications") as m:
            yield m

    @pytest.fixture
    def mock_describe_replication_tasks(self):
        with mock.patch.object(self.client, "describe_replication_tasks") as m:
            yield m

    @pytest.fixture
    def mock_describe_replication_configs(self):
        with mock.patch.object(self.client, "describe_replication_configs") as m:
            yield m

    @pytest.fixture
    def mock_describe_table_statistics(self):
        with mock.patch.object(self.client, "describe_table_statistics") as m:
            yield m

    def test_wait_for_replication_terminal_status(self, mock_describe_replication):
        mock_describe_replication.return_value = {
            "Replications": [
                {
                    "ReplicationConfigArn": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                    "Status": "failed",
                }
            ]
        }

        hook = DmsHook(aws_conn_id=None)
        waiter = hook.get_waiter("replication_terminal_status")
        waiter.wait(
            Filters=[
                {
                    "Name": "replication-instance-arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ]
        )

        mock_describe_replication.assert_called_once_with(
            Filters=[
                {
                    "Name": "replication-instance-arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ]
        )

    def test_wait_for_replication_config_delete(self, mock_describe_replication_configs):
        mock_describe_replication_configs.side_effect = [
            {"Replications": [{"ReplicationArn": "MyArn"}]},
            {"Error": {"Code": "ResourceNotFoundFault"}},
        ]

        hook = DmsHook(aws_conn_id=None)
        waiter = hook.get_waiter("replication_config_deleted")
        waiter.wait(
            Filters=[
                {
                    "Name": "replication-config-arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ],
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

        mock_describe_replication_configs.assert_called_with(
            Filters=[
                {
                    "Name": "replication-config-arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ]
        )

    def test_wait_for_replication_stopped(self, mock_describe_replication):
        mock_describe_replication.return_value = {
            "Replications": [
                {
                    "ReplicationConfigArn": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                    "Status": "stopped",
                }
            ]
        }

        hook = DmsHook(aws_conn_id=None)
        waiter = hook.get_waiter("replication_stopped")
        waiter.wait(
            Filters=[
                {
                    "Name": "replication_config_arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ],
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

        mock_describe_replication.assert_called_once_with(
            Filters=[
                {
                    "Name": "replication_config_arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ]
        )

    @pytest.mark.parametrize("status", ["stopped", "ready", "failed"])
    def test_wait_for_replication_task_modified(self, mock_describe_replication_tasks, status):
        mock_describe_replication_tasks.return_value = {
            "ReplicationTasks": [
                {
                    "ReplicationTaskArn": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
                    "Status": status,
                }
            ]
        }

        hook = DmsHook(aws_conn_id=None)
        waiter = hook.get_waiter("replication_task_modified")
        waiter.wait(
            Filters=[
                {
                    "Name": "replication-task-arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ],
            WithoutSettings=True,
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

        mock_describe_replication_tasks.assert_called_once_with(
            Filters=[
                {
                    "Name": "replication-task-arn",
                    "Values": ["XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"],
                }
            ],
            WithoutSettings=True,
        )

    def test_wait_for_table_reload_complete(self, mock_describe_table_statistics):
        mock_describe_table_statistics.side_effect = [
            {"TableStatistics": [{"TableState": "Before load"}]},
            {"TableStatistics": [{"TableState": "Table is being reloaded"}]},
            {"TableStatistics": [{"TableState": "Full load"}]},
            {"TableStatistics": [{"TableState": "Table completed"}]},
        ]

        hook = DmsHook(aws_conn_id=None)
        waiter = hook.get_waiter("table_reload_complete")
        waiter.wait(
            ReplicationTaskArn="task-arn",
            Filters=[
                {"Name": "schema-name", "Values": ["dmsreload"]},
                {"Name": "table-name", "Values": ["reload_test"]},
            ],
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 4},
        )

        assert mock_describe_table_statistics.call_count == 4
        mock_describe_table_statistics.assert_called_with(
            ReplicationTaskArn="task-arn",
            Filters=[
                {"Name": "schema-name", "Values": ["dmsreload"]},
                {"Name": "table-name", "Values": ["reload_test"]},
            ],
        )

    @pytest.mark.parametrize("table_state", ["Table cancelled", "Table does not exist", "Table error"])
    def test_wait_for_table_reload_failure(self, mock_describe_table_statistics, table_state):
        mock_describe_table_statistics.return_value = {"TableStatistics": [{"TableState": table_state}]}

        waiter = DmsHook(aws_conn_id=None).get_waiter("table_reload_complete")

        with pytest.raises(WaiterError, match="terminal failure"):
            waiter.wait(
                ReplicationTaskArn="task-arn",
                Filters=[
                    {"Name": "schema-name", "Values": ["dmsreload"]},
                    {"Name": "table-name", "Values": ["reload_test"]},
                ],
                WaiterConfig={"Delay": 0.01, "MaxAttempts": 1},
            )

    def test_wait_for_table_validation_complete(self, mock_describe_table_statistics):
        mock_describe_table_statistics.side_effect = [
            {"TableStatistics": [{"ValidationState": "Pending validation"}]},
            {"TableStatistics": [{"ValidationState": "Preparing table"}]},
            {"TableStatistics": [{"ValidationState": "Pending revalidation"}]},
            {"TableStatistics": [{"ValidationState": "Pending records"}]},
            {"TableStatistics": [{"ValidationState": "Validated"}]},
        ]

        waiter = DmsHook(aws_conn_id=None).get_waiter("table_validation_complete")
        waiter.wait(
            ReplicationTaskArn="task-arn",
            Filters=[
                {"Name": "schema-name", "Values": ["dmsreload"]},
                {"Name": "table-name", "Values": ["reload_test"]},
            ],
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 5},
        )

        assert mock_describe_table_statistics.call_count == 5

    @pytest.mark.parametrize(
        "validation_state",
        [
            "Mismatched records",
            "Suspended records",
            "No primary key",
            "Table error",
            "Error",
            "Not enabled",
        ],
    )
    def test_wait_for_table_validation_failure(self, mock_describe_table_statistics, validation_state):
        mock_describe_table_statistics.return_value = {
            "TableStatistics": [{"ValidationState": validation_state}]
        }

        waiter = DmsHook(aws_conn_id=None).get_waiter("table_validation_complete")

        with pytest.raises(WaiterError, match="terminal failure"):
            waiter.wait(
                ReplicationTaskArn="task-arn",
                Filters=[
                    {"Name": "schema-name", "Values": ["dmsreload"]},
                    {"Name": "table-name", "Values": ["reload_test"]},
                ],
                WaiterConfig={"Delay": 0.01, "MaxAttempts": 1},
            )
