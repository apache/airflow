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

import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.dms import DmsCreateTaskOperator

TASK_ARN = "test_arn"
TASK_DATA = {
    "replication_task_id": "task_id",
    "source_endpoint_arn": "source_endpoint",
    "target_endpoint_arn": "target_endpoint",
    "replication_instance_arn": "replication_arn",
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


class TestDmsCreateTaskOperator(unittest.TestCase):
    def test_init(self):
        create_operator = DmsCreateTaskOperator(task_id="create_task", **TASK_DATA)

        assert create_operator.replication_task_id == TASK_DATA["replication_task_id"]
        assert create_operator.source_endpoint_arn == TASK_DATA["source_endpoint_arn"]
        assert create_operator.target_endpoint_arn == TASK_DATA["target_endpoint_arn"]
        assert create_operator.replication_instance_arn == TASK_DATA["replication_instance_arn"]
        assert create_operator.migration_type == "full-load"
        assert create_operator.table_mappings == TASK_DATA["table_mappings"]

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("ready",))
    @mock.patch.object(DmsHook, "create_replication_task", return_value=TASK_ARN)
    @mock.patch.object(DmsHook, "get_conn")
    def test_create_task(self, mock_conn, mock_create_replication_task, mock_get_task_status):
        dms_hook = DmsHook()

        create_task = DmsCreateTaskOperator(task_id="create_task", **TASK_DATA)
        create_task.execute(None)

        mock_create_replication_task.assert_called_once_with(**TASK_DATA, migration_type="full-load")

        assert dms_hook.get_task_status(TASK_ARN) == "ready"

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("ready",))
    @mock.patch.object(DmsHook, "create_replication_task", return_value=TASK_ARN)
    @mock.patch.object(DmsHook, "get_conn")
    def test_create_task_with_migration_type(
        self, mock_conn, mock_create_replication_task, mock_get_task_status
    ):
        migration_type = "cdc"
        dms_hook = DmsHook()

        create_task = DmsCreateTaskOperator(task_id="create_task", migration_type=migration_type, **TASK_DATA)
        create_task.execute(None)

        mock_create_replication_task.assert_called_once_with(**TASK_DATA, migration_type=migration_type)

        assert dms_hook.get_task_status(TASK_ARN) == "ready"
