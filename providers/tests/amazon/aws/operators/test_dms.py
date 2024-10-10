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
from airflow.utils.types import DagRunType

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

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
        op = DmsCreateTaskOperator(
            task_id="create_task",
            **self.TASK_DATA,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="ca-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert op.replication_task_id == self.TASK_DATA["replication_task_id"]
        assert op.source_endpoint_arn == self.TASK_DATA["source_endpoint_arn"]
        assert op.target_endpoint_arn == self.TASK_DATA["target_endpoint_arn"]
        assert op.replication_instance_arn == self.TASK_DATA["replication_instance_arn"]
        assert op.migration_type == "full-load"
        assert op.table_mappings == self.TASK_DATA["table_mappings"]
        assert op.hook.client_type == "dms"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "ca-west-1"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = DmsCreateTaskOperator(task_id="create_task", **self.TASK_DATA)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

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

    def test_template_fields(self):
        op = DmsCreateTaskOperator(
            task_id="create_task",
            **self.TASK_DATA,
            aws_conn_id="fake-conn-id",
            region_name="ca-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        validate_template_fields(op)


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
        op = DmsDeleteTaskOperator(
            task_id="delete_task",
            replication_task_arn=TASK_ARN,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="us-east-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.replication_task_arn == TASK_ARN
        assert op.hook.client_type == "dms"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "us-east-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = DmsDeleteTaskOperator(task_id="describe_tasks", replication_task_arn=TASK_ARN)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

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

    def test_template_fields(self):
        op = DmsDeleteTaskOperator(
            task_id="delete_task",
            replication_task_arn=TASK_ARN,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="us-east-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )

        validate_template_fields(op)


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
        op = DmsDescribeTasksOperator(
            task_id="describe_tasks",
            describe_tasks_kwargs={"Filters": [self.FILTER]},
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify="/foo/bar/spam.egg",
            botocore_config={"read_timeout": 42},
        )
        assert op.describe_tasks_kwargs == {"Filters": [self.FILTER]}
        assert op.hook.client_type == "dms"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-2"
        assert op.hook._verify == "/foo/bar/spam.egg"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = DmsDescribeTasksOperator(
            task_id="describe_tasks", describe_tasks_kwargs={"Filters": [self.FILTER]}
        )
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

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
    def test_describe_tasks_return_value(self, mock_conn, mock_describe_replication_tasks, session):
        describe_task = DmsDescribeTasksOperator(
            task_id="describe_tasks", dag=self.dag, describe_tasks_kwargs={"Filters": [self.FILTER]}
        )

        dag_run = DagRun(
            dag_id=self.dag.dag_id,
            execution_date=timezone.utcnow(),
            run_id="test",
            run_type=DagRunType.MANUAL,
        )
        ti = TaskInstance(task=describe_task)
        ti.dag_run = dag_run
        session.add(ti)
        session.commit()
        marker, response = describe_task.execute(ti.get_template_context())

        assert marker is None
        assert response == self.MOCK_RESPONSE

    def test_template_fields(self):
        op = DmsDescribeTasksOperator(
            task_id="describe_tasks",
            describe_tasks_kwargs={"Filters": [self.FILTER]},
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify="/foo/bar/spam.egg",
            botocore_config={"read_timeout": 42},
        )
        validate_template_fields(op)


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
        op = DmsStartTaskOperator(
            task_id="start_task",
            replication_task_arn=TASK_ARN,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="us-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.replication_task_arn == TASK_ARN
        assert op.start_replication_task_type == "start-replication"
        assert op.hook.client_type == "dms"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "us-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = DmsStartTaskOperator(task_id="start_task", replication_task_arn=TASK_ARN)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

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

    def test_template_fields(self):
        op = DmsStartTaskOperator(
            task_id="start_task",
            replication_task_arn=TASK_ARN,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="us-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )

        validate_template_fields(op)


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
        op = DmsStopTaskOperator(
            task_id="stop_task",
            replication_task_arn=TASK_ARN,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="eu-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert op.replication_task_arn == TASK_ARN
        assert op.hook.client_type == "dms"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = DmsStopTaskOperator(task_id="stop_task", replication_task_arn=TASK_ARN)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

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

    def test_template_fields(self):
        op = DmsStopTaskOperator(
            task_id="stop_task",
            replication_task_arn=TASK_ARN,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="eu-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )

        validate_template_fields(op)
