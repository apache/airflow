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

import json
import unittest
from typing import Any
from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.dms import DmsHook, DmsTaskWaiterStatus

MOCK_DATA = {
    "replication_task_id": "test_task",
    "source_endpoint_arn": "source-endpoint-arn",
    "target_endpoint_arn": "target-endpoint-arn",
    "replication_instance_arn": "replication-instance-arn",
    "migration_type": "full-load",
    "table_mappings": {
        "rules": [
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "1",
                "object-locator": {
                    "schema-name": "test",
                    "table-name": "%",
                },
                "rule-action": "include",
            }
        ]
    },
}
MOCK_TASK_ARN = "task-arn"
MOCK_TASK_RESPONSE_DATA = {
    "ReplicationTaskIdentifier": MOCK_DATA["replication_task_id"],
    "SourceEndpointArn": MOCK_DATA["source_endpoint_arn"],
    "TargetEndpointArn": MOCK_DATA["target_endpoint_arn"],
    "ReplicationInstanceArn": MOCK_DATA["replication_instance_arn"],
    "MigrationType": MOCK_DATA["migration_type"],
    "TableMappings": json.dumps(MOCK_DATA["table_mappings"]),
    "ReplicationTaskArn": MOCK_TASK_ARN,
    "Status": "creating",
}
MOCK_DESCRIBE_RESPONSE: dict[str, Any] = {"ReplicationTasks": [MOCK_TASK_RESPONSE_DATA]}
MOCK_DESCRIBE_RESPONSE_WITH_MARKER: dict[str, Any] = {
    "ReplicationTasks": [MOCK_TASK_RESPONSE_DATA],
    "Marker": "marker",
}
MOCK_CREATE_RESPONSE: dict[str, Any] = {"ReplicationTask": MOCK_TASK_RESPONSE_DATA}
MOCK_START_RESPONSE: dict[str, Any] = {"ReplicationTask": {**MOCK_TASK_RESPONSE_DATA, "Status": "starting"}}
MOCK_STOP_RESPONSE: dict[str, Any] = {"ReplicationTask": {**MOCK_TASK_RESPONSE_DATA, "Status": "stopping"}}
MOCK_DELETE_RESPONSE: dict[str, Any] = {"ReplicationTask": {**MOCK_TASK_RESPONSE_DATA, "Status": "deleting"}}


class TestDmsHook(unittest.TestCase):
    def setUp(self):
        self.dms = DmsHook()

    def test_init(self):
        assert self.dms.aws_conn_id == "aws_default"

    @mock.patch.object(DmsHook, "get_conn")
    def test_describe_replication_tasks_with_no_tasks_found(self, mock_conn):
        mock_conn.return_value.describe_replication_tasks.return_value = {}

        marker, tasks = self.dms.describe_replication_tasks()

        mock_conn.return_value.describe_replication_tasks.assert_called_once()
        assert marker is None
        assert len(tasks) == 0

    @mock.patch.object(DmsHook, "get_conn")
    def test_describe_replication_tasks(self, mock_conn):
        mock_conn.return_value.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE
        describe_tasks_kwargs = {
            "Filters": [{"Name": "replication-task-id", "Values": [MOCK_DATA["replication_task_id"]]}]
        }

        marker, tasks = self.dms.describe_replication_tasks(**describe_tasks_kwargs)

        mock_conn.return_value.describe_replication_tasks.assert_called_with(**describe_tasks_kwargs)
        assert marker is None
        assert len(tasks) == 1
        assert tasks[0]["ReplicationTaskArn"] == MOCK_TASK_ARN

    @mock.patch.object(DmsHook, "get_conn")
    def test_describe_teplication_tasks_with_marker(self, mock_conn):
        mock_conn.return_value.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE_WITH_MARKER
        describe_tasks_kwargs = {
            "Filters": [{"Name": "replication-task-id", "Values": [MOCK_DATA["replication_task_id"]]}]
        }

        marker, tasks = self.dms.describe_replication_tasks(**describe_tasks_kwargs)

        mock_conn.return_value.describe_replication_tasks.assert_called_with(**describe_tasks_kwargs)
        assert marker == MOCK_DESCRIBE_RESPONSE_WITH_MARKER["Marker"]
        assert len(tasks) == 1
        assert tasks[0]["ReplicationTaskArn"] == MOCK_TASK_ARN

    @mock.patch.object(DmsHook, "get_conn")
    def test_find_replication_tasks_by_arn(self, mock_conn):
        mock_conn.return_value.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE

        tasks = self.dms.find_replication_tasks_by_arn(MOCK_TASK_ARN)

        expected_call_params = {
            "Filters": [{"Name": "replication-task-arn", "Values": [MOCK_TASK_ARN]}],
            "WithoutSettings": False,
        }

        mock_conn.return_value.describe_replication_tasks.assert_called_with(**expected_call_params)
        assert len(tasks) == 1
        assert tasks[0]["ReplicationTaskArn"] == MOCK_TASK_ARN

    @mock.patch.object(DmsHook, "get_conn")
    def test_get_task_status(self, mock_conn):
        mock_conn.return_value.describe_replication_tasks.return_value = MOCK_DESCRIBE_RESPONSE

        status = self.dms.get_task_status(MOCK_TASK_ARN)

        expected_call_params = {
            "Filters": [{"Name": "replication-task-arn", "Values": [MOCK_TASK_ARN]}],
            "WithoutSettings": True,
        }

        mock_conn.return_value.describe_replication_tasks.assert_called_with(**expected_call_params)
        assert status == MOCK_TASK_RESPONSE_DATA["Status"]

    @mock.patch.object(DmsHook, "get_conn")
    def test_get_task_status_with_no_task_found(self, mock_conn):
        mock_conn.return_value.describe_replication_tasks.return_value = {}
        status = self.dms.get_task_status(MOCK_TASK_ARN)

        mock_conn.return_value.describe_replication_tasks.assert_called_once()
        assert status is None

    @mock.patch.object(DmsHook, "get_conn")
    def test_create_replication_task(self, mock_conn):
        mock_conn.return_value.create_replication_task.return_value = MOCK_CREATE_RESPONSE
        result = self.dms.create_replication_task(**MOCK_DATA)
        expected_call_params = {
            "ReplicationTaskIdentifier": MOCK_DATA["replication_task_id"],
            "SourceEndpointArn": MOCK_DATA["source_endpoint_arn"],
            "TargetEndpointArn": MOCK_DATA["target_endpoint_arn"],
            "ReplicationInstanceArn": MOCK_DATA["replication_instance_arn"],
            "MigrationType": MOCK_DATA["migration_type"],
            "TableMappings": json.dumps(MOCK_DATA["table_mappings"]),
        }
        mock_conn.return_value.create_replication_task.assert_called_with(**expected_call_params)
        assert result == MOCK_CREATE_RESPONSE["ReplicationTask"]["ReplicationTaskArn"]

    @mock.patch.object(DmsHook, "get_conn")
    def test_start_replication_task(self, mock_conn):
        mock_conn.return_value.start_replication_task.return_value = MOCK_START_RESPONSE
        start_type = "start-replication"

        self.dms.start_replication_task(
            replication_task_arn=MOCK_TASK_ARN,
            start_replication_task_type=start_type,
        )

        expected_call_params = {
            "ReplicationTaskArn": MOCK_TASK_ARN,
            "StartReplicationTaskType": start_type,
        }
        mock_conn.return_value.start_replication_task.assert_called_with(**expected_call_params)

    @mock.patch.object(DmsHook, "get_conn")
    def test_stop_replication_task(self, mock_conn):
        mock_conn.return_value.stop_replication_task.return_value = MOCK_STOP_RESPONSE

        self.dms.stop_replication_task(replication_task_arn=MOCK_TASK_ARN)

        expected_call_params = {"ReplicationTaskArn": MOCK_TASK_ARN}
        mock_conn.return_value.stop_replication_task.assert_called_with(**expected_call_params)

    @mock.patch.object(DmsHook, "get_conn")
    def test_delete_replication_task(self, mock_conn):
        mock_conn.return_value.delete_replication_task.return_value = MOCK_DELETE_RESPONSE

        self.dms.delete_replication_task(replication_task_arn=MOCK_TASK_ARN)

        expected_call_params = {"ReplicationTaskArn": MOCK_TASK_ARN}
        mock_conn.return_value.delete_replication_task.assert_called_with(**expected_call_params)

    @mock.patch.object(DmsHook, "get_conn")
    def test_wait_for_task_status_with_unknown_target_status(self, mock_conn):
        with pytest.raises(TypeError, match="Status must be an instance of DmsTaskWaiterStatus"):
            self.dms.wait_for_task_status(MOCK_TASK_ARN, "unknown_status")

    @mock.patch.object(DmsHook, "get_conn")
    def test_wait_for_task_status(self, mock_conn):
        self.dms.wait_for_task_status(replication_task_arn=MOCK_TASK_ARN, status=DmsTaskWaiterStatus.DELETED)

        expected_waiter_call_params = {
            "Filters": [{"Name": "replication-task-arn", "Values": [MOCK_TASK_ARN]}],
            "WithoutSettings": True,
        }
        mock_conn.return_value.get_waiter.assert_called_with("replication_task_deleted")
        mock_conn.return_value.get_waiter.return_value.wait.assert_called_with(**expected_waiter_call_params)
