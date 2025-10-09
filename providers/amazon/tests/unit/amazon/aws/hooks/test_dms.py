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
from datetime import datetime
from typing import Any
from unittest import mock

import pytest
from dateutil import parser

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

MOCK_CONFIG_RESPONSE: dict[str, Any] = {
    "Marker": "xxxxx",
    "ReplicationConfigs": [
        {
            "ReplicationConfigIdentifier": "1111",
            "ReplicationConfigArn": "arn:aws:my-arn",
            "SourceEndpointArn": "source-endpoint-arn",
            "TargetEndpointArn": "target-endpoint-arn",
            "ReplicationType": "cdc",
            "ComputeConfig": {
                "AvailabilityZone": "az1",
                "MaxCapacityUnits": 10,
                "MinCapacityUnits": 20,
                "MultiAZ": True,
                "PreferredMaintenanceWindow": "string",
                "ReplicationSubnetGroupId": "string",
                "VpcSecurityGroupIds": [
                    "string",
                ],
            },
            "ReplicationSettings": "string",
            "SupplementalSettings": "string",
            "TableMappings": "string",
            "ReplicationConfigCreateTime": datetime(2015, 1, 1),
            "ReplicationConfigUpdateTime": datetime(2015, 1, 1),
        },
        {
            "ReplicationConfigIdentifier": "2222",
            "ReplicationConfigArn": "arn:aws:my-arn",
            "SourceEndpointArn": "source-endpoint-arn",
            "TargetEndpointArn": "target-endpoint-arn",
            "ReplicationType": "full-load-and-cdc",
            "ComputeConfig": {
                "AvailabilityZone": "string",
                "DnsNameServers": "string",
                "KmsKeyId": "string",
                "MaxCapacityUnits": 1,
                "MinCapacityUnits": 30,
                "MultiAZ": False,
                "PreferredMaintenanceWindow": "string",
                "ReplicationSubnetGroupId": "string",
                "VpcSecurityGroupIds": [
                    "string",
                ],
            },
            "ReplicationSettings": "string",
            "SupplementalSettings": "string",
            "TableMappings": "string",
            "ReplicationConfigCreateTime": datetime(2015, 1, 1),
            "ReplicationConfigUpdateTime": datetime(2015, 2, 1),
        },
    ],
}

MOCK_REPLICATION_CONFIG: dict[str, Any] = {
    "ReplicationConfigIdentifier": "test-config",
    "SourceEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:RZZK4EZW5UANC7Y3P4E776WHBE",
    "TargetEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:GVBUJQXJZASXWHTWCLN2WNT57E",
    "ComputeConfig": {
        "MaxCapacityUnits": 2,
        "MinCapacityUnits": 4,
    },
    "ReplicationType": "full-load",
    "TableMappings": json.dumps(
        {
            "TableMappings": [
                {
                    "Type": "Selection",
                    "RuleId": 123,
                    "RuleName": "test-rule",
                    "SourceSchema": "/",
                    "SourceTable": "/",
                }
            ]
        }
    ),
    "ReplicationSettings": "string",
    "SupplementalSettings": "string",
    "ResourceIdentifier": "string",
}

MOCK_REPLICATION_CONFIG_RESP: dict[str, Any] = {
    "ReplicationConfig": {
        "ReplicationConfigIdentifier": "test-config",
        "ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config/test-config",
        "SourceEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:RZZK4EZW5UANC7Y3P4E776WHBE",
        "TargetEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:GVBUJQXJZASXWHTWCLN2WNT57E",
        "ReplicationType": "full-load",
    }
}

MOCK_DESCRIBE_REPLICATIONS_RESP = {
    "Marker": "string",
    "Replications": [
        {
            "ReplicationConfigIdentifier": "test-config",
            "ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config/test-config",
            "SourceEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:RZZK4EZW5UANC7Y3P4E776WHBE",
            "TargetEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:GVBUJQXJZASXWHTWCLN2WNT57E",
            "ReplicationType": "full-load",
            "Status": "CREATED",
            "ProvisionData": {
                "ProvisionState": "string",
                "ProvisionedCapacityUnits": 123,
                "DateProvisioned": datetime(2015, 1, 1),
                "IsNewProvisioningAvailable": False,
                "DateNewProvisioningDataAvailable": datetime(2015, 1, 1),
                "ReasonForNewProvisioningData": "string",
            },
            "StopReason": "string",
            "FailureMessages": [
                "string",
            ],
            "ReplicationStats": {
                "FullLoadProgressPercent": 123,
                "ElapsedTimeMillis": 123,
                "TablesLoaded": 123,
                "TablesLoading": 123,
                "TablesQueued": 123,
                "TablesErrored": 123,
                "FreshStartDate": datetime(2015, 1, 1),
                "StartDate": datetime(2015, 1, 1),
                "StopDate": datetime(2015, 1, 1),
                "FullLoadStartDate": datetime(2015, 1, 1),
                "FullLoadFinishDate": datetime(2015, 1, 1),
            },
            "StartReplicationType": "string",
            "CdcStartTime": datetime(2015, 1, 1),
            "CdcStartPosition": "string",
            "CdcStopPosition": "string",
            "RecoveryCheckpoint": "string",
            "ReplicationCreateTime": datetime(2015, 1, 1),
            "ReplicationUpdateTime": datetime(2015, 1, 1),
            "ReplicationLastStopTime": datetime(2015, 1, 1),
            "ReplicationDeprovisionTime": datetime(2015, 1, 1),
        },
        {
            "ReplicationConfigIdentifier": "test-config-2",
            "ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config/test-config-2",
            "SourceEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:RZZK4EZW5UANC7Y3P4E776WHBE",
            "TargetEndpointArn": "arn:aws:dms:us-east-1:123456789012:endpoint:GVBUJQXJZASXWHTWCLN2WNT57E",
            "ReplicationType": "cdc-only",
            "Status": "CREATED",
            "ProvisionData": {
                "ProvisionState": "string",
                "ProvisionedCapacityUnits": 123,
                "DateProvisioned": datetime(2015, 1, 1),
                "IsNewProvisioningAvailable": False,
                "DateNewProvisioningDataAvailable": datetime(2015, 1, 1),
                "ReasonForNewProvisioningData": "string",
            },
            "StopReason": "string",
            "FailureMessages": [
                "string",
            ],
            "ReplicationStats": {
                "FullLoadProgressPercent": 123,
                "ElapsedTimeMillis": 123,
                "TablesLoaded": 123,
                "TablesLoading": 123,
                "TablesQueued": 123,
                "TablesErrored": 123,
                "FreshStartDate": datetime(2015, 1, 1),
                "StartDate": datetime(2015, 1, 1),
                "StopDate": datetime(2015, 1, 1),
                "FullLoadStartDate": datetime(2015, 1, 1),
                "FullLoadFinishDate": datetime(2015, 1, 1),
            },
            "StartReplicationType": "string",
            "CdcStartTime": datetime(2015, 1, 1),
            "CdcStartPosition": "string",
            "CdcStopPosition": "string",
            "RecoveryCheckpoint": "string",
            "ReplicationCreateTime": datetime(2015, 1, 1),
            "ReplicationUpdateTime": datetime(2015, 1, 1),
            "ReplicationLastStopTime": datetime(2015, 1, 1),
            "ReplicationDeprovisionTime": datetime(2015, 1, 1),
        },
    ],
}


class TestDmsHook:
    def setup_method(self):
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

    @mock.patch.object(DmsHook, "conn")
    def test_describe_config_no_filter(self, mock_conn):
        mock_conn.describe_replication_configs.return_value = MOCK_CONFIG_RESPONSE
        resp = self.dms.describe_replication_configs()

        assert len(resp) == 2

    @mock.patch.object(DmsHook, "conn")
    def test_describe_config_filter(self, mock_conn):
        filter = [{"Name": "replication-type", "Values": ["cdc"]}]
        self.dms.describe_replication_configs(filters=filter)
        mock_conn.describe_replication_configs.assert_called_with(Filters=filter)

    @mock.patch.object(DmsHook, "conn")
    def test_create_repl_config_kwargs(self, mock_conn):
        self.dms.create_replication_config(
            replication_config_id=MOCK_REPLICATION_CONFIG["ReplicationConfigIdentifier"],
            source_endpoint_arn=MOCK_REPLICATION_CONFIG["SourceEndpointArn"],
            target_endpoint_arn=MOCK_REPLICATION_CONFIG["TargetEndpointArn"],
            compute_config=MOCK_REPLICATION_CONFIG["ComputeConfig"],
            replication_type=MOCK_REPLICATION_CONFIG["ReplicationType"],
            table_mappings=MOCK_REPLICATION_CONFIG["TableMappings"],
            additional_config_kwargs={
                "ReplicationSettings": MOCK_REPLICATION_CONFIG["ReplicationSettings"],
                "SupplementalSettings": MOCK_REPLICATION_CONFIG["SupplementalSettings"],
            },
        )

        mock_conn.create_replication_config.assert_called_with(
            ReplicationConfigIdentifier=MOCK_REPLICATION_CONFIG["ReplicationConfigIdentifier"],
            SourceEndpointArn=MOCK_REPLICATION_CONFIG["SourceEndpointArn"],
            TargetEndpointArn=MOCK_REPLICATION_CONFIG["TargetEndpointArn"],
            ComputeConfig=MOCK_REPLICATION_CONFIG["ComputeConfig"],
            ReplicationType=MOCK_REPLICATION_CONFIG["ReplicationType"],
            TableMappings=MOCK_REPLICATION_CONFIG["TableMappings"],
            ReplicationSettings=MOCK_REPLICATION_CONFIG["ReplicationSettings"],
            SupplementalSettings=MOCK_REPLICATION_CONFIG["SupplementalSettings"],
        )

        self.dms.create_replication_config(
            replication_config_id=MOCK_REPLICATION_CONFIG["ReplicationConfigIdentifier"],
            source_endpoint_arn=MOCK_REPLICATION_CONFIG["SourceEndpointArn"],
            target_endpoint_arn=MOCK_REPLICATION_CONFIG["TargetEndpointArn"],
            compute_config=MOCK_REPLICATION_CONFIG["ComputeConfig"],
            replication_type=MOCK_REPLICATION_CONFIG["ReplicationType"],
            table_mappings=MOCK_REPLICATION_CONFIG["TableMappings"],
        )
        mock_conn.create_replication_config.assert_called_with(
            ReplicationConfigIdentifier=MOCK_REPLICATION_CONFIG["ReplicationConfigIdentifier"],
            SourceEndpointArn=MOCK_REPLICATION_CONFIG["SourceEndpointArn"],
            TargetEndpointArn=MOCK_REPLICATION_CONFIG["TargetEndpointArn"],
            ComputeConfig=MOCK_REPLICATION_CONFIG["ComputeConfig"],
            ReplicationType=MOCK_REPLICATION_CONFIG["ReplicationType"],
            TableMappings=MOCK_REPLICATION_CONFIG["TableMappings"],
        )

    @mock.patch.object(DmsHook, "conn")
    def test_create_repl_config(self, mock_conn):
        mock_conn.create_replication_config.return_value = MOCK_REPLICATION_CONFIG_RESP

        resp = self.dms.create_replication_config(
            replication_config_id=MOCK_REPLICATION_CONFIG["ReplicationConfigIdentifier"],
            source_endpoint_arn=MOCK_REPLICATION_CONFIG["SourceEndpointArn"],
            target_endpoint_arn=MOCK_REPLICATION_CONFIG["TargetEndpointArn"],
            compute_config=MOCK_REPLICATION_CONFIG["ComputeConfig"],
            replication_type=MOCK_REPLICATION_CONFIG["ReplicationType"],
            table_mappings=MOCK_REPLICATION_CONFIG["TableMappings"],
        )

        assert resp == MOCK_REPLICATION_CONFIG_RESP["ReplicationConfig"]["ReplicationConfigArn"]

    @mock.patch.object(DmsHook, "conn")
    def test_describe_replications(self, mock_conn):
        mock_conn.describe_replication_tasks.return_value = MOCK_DESCRIBE_REPLICATIONS_RESP
        resp = self.dms.describe_replication_tasks()
        assert len(resp) == 2

    @mock.patch.object(DmsHook, "conn")
    def test_describe_replications_filter(self, mock_conn):
        filter = [
            {
                "Name": "replication-task-id",
                "Values": MOCK_DESCRIBE_REPLICATIONS_RESP["Replications"][0]["ReplicationConfigArn"],
            }
        ]
        self.dms.describe_replication_tasks(filters=filter)
        mock_conn.describe_replication_tasks.assert_called_with(filters=filter)

    @mock.patch.object(DmsHook, "conn")
    def test_start_replication_args(self, mock_conn):
        self.dms.start_replication(
            replication_config_arn=MOCK_TASK_ARN,
            start_replication_type="cdc",
        )
        mock_conn.start_replication.assert_called_with(
            ReplicationConfigArn=MOCK_TASK_ARN,
            StartReplicationType="cdc",
        )

    @mock.patch.object(DmsHook, "conn")
    def test_start_replication_kwargs(self, mock_conn):
        self.dms.start_replication(
            replication_config_arn=MOCK_TASK_ARN,
            start_replication_type="cdc",
            cdc_start_time="2022-01-01T00:00:00Z",
            cdc_start_pos=None,
            cdc_stop_pos=None,
        )
        mock_conn.start_replication.assert_called_with(
            ReplicationConfigArn=MOCK_TASK_ARN,
            StartReplicationType="cdc",
            CdcStartTime=parser.parse("2022-01-01T00:00:00Z"),
        )
