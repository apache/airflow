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
from unittest import mock

import pendulum
import pytest

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.dms import (
    DmsCreateTaskOperator,
    DmsDeleteTaskOperator,
    DmsDescribeTasksOperator,
    DmsStartTaskOperator,
    DmsStopTaskOperator,
)
from airflow.utils import timezone

TASK_ARN = "test_arn"


class TestDmsCreateTaskOperator:
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

    def test_init(self):
        create_operator = DmsCreateTaskOperator(task_id="create_task", **self.TASK_DATA)

        assert create_operator.replication_task_id == self.TASK_DATA["replication_task_id"]
        assert create_operator.source_endpoint_arn == self.TASK_DATA["source_endpoint_arn"]
        assert create_operator.target_endpoint_arn == self.TASK_DATA["target_endpoint_arn"]
        assert create_operator.replication_instance_arn == self.TASK_DATA["replication_instance_arn"]
        assert create_operator.migration_type == "full-load"
        assert create_operator.table_mappings == self.TASK_DATA["table_mappings"]

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("ready",))
    @mock.patch.object(DmsHook, "create_replication_task", return_value=TASK_ARN)
    @mock.patch.object(DmsHook, "get_conn")
    def test_create_task(self, mock_conn, mock_create_replication_task, mock_get_task_status):
        dms_hook = DmsHook()

        create_task = DmsCreateTaskOperator(task_id="create_task", **self.TASK_DATA)
        create_task.execute(None)

        mock_create_replication_task.assert_called_once_with(**self.TASK_DATA, migration_type="full-load")

        assert dms_hook.get_task_status(TASK_ARN) == "ready"

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("ready",))
    @mock.patch.object(DmsHook, "create_replication_task", return_value=TASK_ARN)
    @mock.patch.object(DmsHook, "get_conn")
    def test_create_task_with_migration_type(
        self, mock_conn, mock_create_replication_task, mock_get_task_status
    ):
        migration_type = "cdc"
        dms_hook = DmsHook()

        create_task = DmsCreateTaskOperator(
            task_id="create_task", migration_type=migration_type, **self.TASK_DATA
        )
        create_task.execute(None)

        mock_create_replication_task.assert_called_once_with(**self.TASK_DATA, migration_type=migration_type)

        assert dms_hook.get_task_status(TASK_ARN) == "ready"


class TestDmsDeleteTaskOperator:
    TASK_DATA = {
        "replication_task_id": "task_id",
        "source_endpoint_arn": "source_endpoint",
        "target_endpoint_arn": "target_endpoint",
        "replication_instance_arn": "replication_arn",
        "migration_type": "full-load",
        "table_mappings": {},
    }

    def test_init(self):
        dms_operator = DmsDeleteTaskOperator(task_id="delete_task", replication_task_arn=TASK_ARN)

        assert dms_operator.replication_task_arn == TASK_ARN

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("deleting",))
    @mock.patch.object(DmsHook, "delete_replication_task")
    @mock.patch.object(DmsHook, "create_replication_task", return_value=TASK_ARN)
    @mock.patch.object(DmsHook, "get_conn")
    def test_delete_task(
        self, mock_conn, mock_create_replication_task, mock_delete_replication_task, mock_get_task_status
    ):
        dms_hook = DmsHook()
        task = dms_hook.create_replication_task(**self.TASK_DATA)

        delete_task = DmsDeleteTaskOperator(task_id="delete_task", replication_task_arn=task)
        delete_task.execute(None)

        mock_delete_replication_task.assert_called_once_with(replication_task_arn=TASK_ARN)

        assert dms_hook.get_task_status(TASK_ARN) == "deleting"


class TestDmsDescribeTasksOperator:
    FILTER = {"Name": "replication-task-arn", "Values": [TASK_ARN]}
    MOCK_DATA = {
        "replication_task_id": "test_task",
        "source_endpoint_arn": "source-endpoint-arn",
        "target_endpoint_arn": "target-endpoint-arn",
        "replication_instance_arn": "replication-instance-arn",
        "migration_type": "full-load",
        "table_mappings": {},
    }
    MOCK_RESPONSE = [
        {
            "ReplicationTaskIdentifier": MOCK_DATA["replication_task_id"],
            "SourceEndpointArn": MOCK_DATA["source_endpoint_arn"],
            "TargetEndpointArn": MOCK_DATA["target_endpoint_arn"],
            "ReplicationInstanceArn": MOCK_DATA["replication_instance_arn"],
            "MigrationType": MOCK_DATA["migration_type"],
            "TableMappings": json.dumps(MOCK_DATA["table_mappings"]),
            "ReplicationTaskArn": TASK_ARN,
            "Status": "creating",
        }
    ]

    def setup_method(self):
        args = {
            "owner": "airflow",
            "start_date": pendulum.datetime(2018, 1, 1, tz="UTC"),
        }

        self.dag = DAG("dms_describe_tasks_operator", default_args=args, schedule="@once")

    def test_init(self):
        dms_operator = DmsDescribeTasksOperator(
            task_id="describe_tasks", describe_tasks_kwargs={"Filters": [self.FILTER]}
        )

        assert dms_operator.describe_tasks_kwargs == {"Filters": [self.FILTER]}

    @mock.patch.object(DmsHook, "describe_replication_tasks", return_value=(None, MOCK_RESPONSE))
    @mock.patch.object(DmsHook, "get_conn")
    def test_describe_tasks(self, mock_conn, mock_describe_replication_tasks):
        describe_tasks_kwargs = {"Filters": [self.FILTER]}
        describe_task = DmsDescribeTasksOperator(
            task_id="describe_tasks", describe_tasks_kwargs=describe_tasks_kwargs
        )
        describe_task.execute(None)

        mock_describe_replication_tasks.assert_called_once_with(**describe_tasks_kwargs)

    @pytest.mark.db_test
    @mock.patch.object(DmsHook, "describe_replication_tasks", return_value=(None, MOCK_RESPONSE))
    @mock.patch.object(DmsHook, "get_conn")
    def test_describe_tasks_return_value(self, mock_conn, mock_describe_replication_tasks):
        describe_task = DmsDescribeTasksOperator(
            task_id="describe_tasks", dag=self.dag, describe_tasks_kwargs={"Filters": [self.FILTER]}
        )

        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=describe_task)
        ti.dag_run = dag_run
        marker, response = describe_task.execute(ti.get_template_context())

        assert marker is None
        assert response == self.MOCK_RESPONSE


class TestDmsStartTaskOperator:
    TASK_DATA = {
        "replication_task_id": "task_id",
        "source_endpoint_arn": "source_endpoint",
        "target_endpoint_arn": "target_endpoint",
        "replication_instance_arn": "replication_arn",
        "migration_type": "full-load",
        "table_mappings": {},
    }

    def test_init(self):
        dms_operator = DmsStartTaskOperator(task_id="start_task", replication_task_arn=TASK_ARN)

        assert dms_operator.replication_task_arn == TASK_ARN
        assert dms_operator.start_replication_task_type == "start-replication"

    @mock.patch.object(DmsHook, "get_task_status", side_effect=("starting",))
    @mock.patch.object(DmsHook, "start_replication_task")
    @mock.patch.object(DmsHook, "create_replication_task", return_value=TASK_ARN)
    @mock.patch.object(DmsHook, "get_conn")
    def test_start_task(
        self, mock_conn, mock_create_replication_task, mock_start_replication_task, mock_get_task_status
    ):
        dms_hook = DmsHook()
        task = dms_hook.create_replication_task(**self.TASK_DATA)

        start_task = DmsStartTaskOperator(task_id="start_task", replication_task_arn=task)
        start_task.execute(None)

        mock_start_replication_task.assert_called_once_with(
            replication_task_arn=TASK_ARN,
            start_replication_task_type="start-replication",
        )

        assert dms_hook.get_task_status(TASK_ARN) == "starting"


class TestDmsStopTaskOperator:
    TASK_DATA = {
        "replication_task_id": "task_id",
        "source_endpoint_arn": "source_endpoint",
        "target_endpoint_arn": "target_endpoint",
        "replication_instance_arn": "replication_arn",
        "migration_type": "full-load",
        "table_mappings": {},
    }

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
        task = dms_hook.create_replication_task(**self.TASK_DATA)

        stop_task = DmsStopTaskOperator(task_id="stop_task", replication_task_arn=task)
        stop_task.execute(None)

        mock_stop_replication_task.assert_called_once_with(replication_task_arn=TASK_ARN)

        assert dms_hook.get_task_status(TASK_ARN) == "stopping"
