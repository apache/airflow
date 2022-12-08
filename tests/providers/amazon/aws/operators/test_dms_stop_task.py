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

from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.dms import DmsStopTaskOperator

TASK_ARN = "test_arn"
TASK_DATA = {
    "replication_task_id": "task_id",
    "source_endpoint_arn": "source_endpoint",
    "target_endpoint_arn": "target_endpoint",
    "replication_instance_arn": "replication_arn",
    "migration_type": "full-load",
    "table_mappings": {},
}


class TestDmsStopTaskOperator:
    def test_init(self):
        dms_operator = DmsStopTaskOperator(task_id="stop_task", replication_task_arn=TASK_ARN)

        assert dms_operator.replication_task_arn == TASK_ARN

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("stopping",))
    @mock.patch.object(DmsHook, "stop_replication_task")
    @mock.patch.object(DmsHook, "create_replication_task", return_value=TASK_ARN)
    @mock.patch.object(DmsHook, "get_conn")
    def test_stop_task(
        self, mock_conn, mock_create_replication_task, mock_stop_replication_task, mock_get_task_status
    ):
        dms_hook = DmsHook()
        task = dms_hook.create_replication_task(**TASK_DATA)

        stop_task = DmsStopTaskOperator(task_id="stop_task", replication_task_arn=task)
        stop_task.execute(None)

        mock_stop_replication_task.assert_called_once_with(replication_task_arn=TASK_ARN)

        assert dms_hook.get_task_status(TASK_ARN) == "stopping"
