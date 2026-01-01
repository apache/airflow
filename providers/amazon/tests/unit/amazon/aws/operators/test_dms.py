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
from typing import Any
from unittest import mock

import pendulum
import pytest

from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.dms import (
    DmsCreateReplicationConfigOperator,
    DmsCreateTaskOperator,
    DmsDeleteReplicationConfigOperator,
    DmsDeleteTaskOperator,
    DmsDescribeReplicationConfigsOperator,
    DmsDescribeReplicationsOperator,
    DmsDescribeTasksOperator,
    DmsStartReplicationOperator,
    DmsStartTaskOperator,
    DmsStopReplicationOperator,
    DmsStopTaskOperator,
)
from airflow.providers.amazon.aws.triggers.dms import (
    DmsReplicationDeprovisionedTrigger,
    DmsReplicationTerminalStatusTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.taskinstance import create_task_instance, render_template_fields
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS
from unit.amazon.aws.utils.test_template_fields import validate_template_fields

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

if AIRFLOW_V_3_0_PLUS:
    from airflow.models.dag_version import DagVersion

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

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = DmsCreateTaskOperator(
            task_id="dms_create_task_operator",
            **self.TASK_DATA,
            aws_conn_id=OVERWRITTEN_CONN,
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == OVERWRITTEN_CONN

    def test_default_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = DmsCreateTaskOperator(
            task_id="dms_create_task_operator",
            **self.TASK_DATA,
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == DEFAULT_CONN


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
    def test_describe_tasks_return_value(
        self,
        mock_conn,
        mock_describe_replication_tasks,
        session,
        clean_dags_dagruns_and_dagbundles,
        testing_dag_bundle,
    ):
        describe_task = DmsDescribeTasksOperator(
            task_id="describe_tasks", dag=self.dag, describe_tasks_kwargs={"Filters": [self.FILTER]}
        )

        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(self.dag)
            dag_version = DagVersion.get_latest_version(self.dag.dag_id)
            ti = create_task_instance(task=describe_task, dag_version_id=dag_version.id)
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                logical_date=timezone.utcnow(),
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
            )
        else:
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                execution_date=timezone.utcnow(),
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
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


class TestDmsDescribeReplicationConfigsOperator:
    filter = [{"Name": "replication-type", "Values": ["cdc"]}]

    def test_init(self):
        op = DmsDescribeReplicationConfigsOperator(task_id="test_task")
        assert op.filter is None

    @pytest.mark.db_test
    @mock.patch.object(DmsHook, "conn")
    def test_template_fields_native(
        self, mock_conn, session, clean_dags_dagruns_and_dagbundles, testing_dag_bundle
    ):
        logical_date = timezone.datetime(2020, 1, 1)
        Variable.set("test_filter", self.filter, session=session)

        dag = DAG(
            "test_dms",
            schedule=None,
            start_date=logical_date,
            render_template_as_native_obj=True,
        )
        op = DmsDescribeReplicationConfigsOperator(
            task_id="test_task", filter="{{ var.value.test_filter }}", dag=dag
        )

        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(dag)
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            ti = create_task_instance(task=op, dag_version_id=dag_version.id)
            dag_run = DagRun(
                dag_id=dag.dag_id,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
                logical_date=logical_date,
                run_after=timezone.utcnow(),
            )
        else:
            dag_run = DagRun(
                dag_id=dag.dag_id,
                run_id="test",
                run_type=DagRunType.MANUAL,
                state=DagRunState.RUNNING,
                execution_date=logical_date,
            )
            ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        render_template_fields(ti, op)
        assert op.filter == self.filter


class TestDmsCreateReplicationConfigOperator:
    TASK_DATA = {
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

    def test_init(self):
        DmsCreateReplicationConfigOperator(
            task_id="create_replication_config",
            replication_config_id=self.TASK_DATA["ReplicationConfigIdentifier"],
            source_endpoint_arn=self.TASK_DATA["SourceEndpointArn"],
            target_endpoint_arn=self.TASK_DATA["TargetEndpointArn"],
            replication_type=self.TASK_DATA["ReplicationType"],
            table_mappings=self.TASK_DATA["TableMappings"],
            compute_config=self.TASK_DATA["ComputeConfig"],
        )

    @mock.patch.object(DmsHook, "conn")
    def test_operator(self, mock_hook):
        mock_hook.create_replication_config.return_value = self.MOCK_REPLICATION_CONFIG_RESP
        op = DmsCreateReplicationConfigOperator(
            task_id="create_replication_config",
            replication_config_id=self.TASK_DATA["ReplicationConfigIdentifier"],
            source_endpoint_arn=self.TASK_DATA["SourceEndpointArn"],
            target_endpoint_arn=self.TASK_DATA["TargetEndpointArn"],
            replication_type=self.TASK_DATA["ReplicationType"],
            table_mappings=self.TASK_DATA["TableMappings"],
            compute_config=self.TASK_DATA["ComputeConfig"],
        )
        resp = op.execute(None)
        assert resp == self.MOCK_REPLICATION_CONFIG_RESP["ReplicationConfig"]["ReplicationConfigArn"]


class TestDmsDeleteReplicationConfigOperator:
    TASK_DATA = {
        "ReplicationConfigIdentifier": "test-config",
        "ReplicationConfigArn": "arn:xxxxxx",
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

    def get_replication_status(self, status: str, deprovisioned: str = "deprovisioned"):
        return [
            {
                "Status": status,
                "ReplicationArn": "XXXXXXXXXXXXXXXXXXXXXXXXX",
                "ReplicationIdentifier": "test-config",
                "SourceEndpointArn": "XXXXXXXXXXXXXXXXXXXXXXXXX",
                "TargetEndpointArn": "XXXXXXXXXXXXXXXXXXXXXXXXX",
                "ProvisionData": {"ProvisionState": deprovisioned, "ProvisionedCapacityUnits": 2},
            }
        ]

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsDeleteReplicationConfigOperator, "handle_delete_wait")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_happy_path(self, mock_waiter, mock_handle, mock_describe_replications, mock_conn):
        # testing all good statuses and no waiting
        mock_describe_replications.return_value = self.get_replication_status(
            status="stopped", deprovisioned="deprovisioned"
        )
        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=False,
            wait_for_completion=False,
        )
        op.execute({})

        mock_conn.delete_replication_config.assert_called_once()
        mock_waiter.assert_has_calls(
            [
                mock.call("replication_terminal_status"),
                mock.call().wait(
                    Filters=[{"Name": "replication-config-arn", "Values": ["arn:xxxxxx"]}],
                    WaiterConfig={"Delay": 60, "MaxAttempts": 60},
                ),
            ]
        )
        mock_handle.assert_called_once()

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    def test_defer_not_ready(self, mock_describe, mock_conn):
        mock_describe.return_value = self.get_replication_status("running")

        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as defer:
            op.execute({})

        assert isinstance(defer.value.trigger, DmsReplicationTerminalStatusTrigger)

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_wait_for_completion(self, mock_waiter, mock_describe_replications, mock_conn):
        mock_describe_replications.return_value = self.get_replication_status(
            status="failed", deprovisioned="deprovisioned"
        )

        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=False,
            wait_for_completion=True,
        )
        op.execute({})

        mock_waiter.assert_has_calls(
            [
                mock.call("replication_terminal_status"),
                mock.call().wait(
                    Filters=[{"Name": "replication-config-arn", "Values": ["arn:xxxxxx"]}],
                    WaiterConfig={"Delay": 60, "MaxAttempts": 60},
                ),
            ]
        )

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_wait_for_completion_not_ready(self, mock_waiter, mock_describe_replications, mock_conn):
        mock_describe_replications.return_value = self.get_replication_status(
            status="failed", deprovisioned="xxx"
        )

        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=False,
            wait_for_completion=True,
        )
        op.execute({})

        mock_waiter.assert_has_calls(
            [
                mock.call("replication_terminal_status"),
                mock.call().wait(
                    Filters=[{"Name": "replication-config-arn", "Values": ["arn:xxxxxx"]}],
                    WaiterConfig={"Delay": 60, "MaxAttempts": 60},
                ),
            ]
        )

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsDeleteReplicationConfigOperator, "handle_delete_wait")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_not_ready_state(self, mock_waiter, mock_handle, mock_describe, mock_conn):
        mock_describe.return_value = self.get_replication_status("running")

        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=False,
            wait_for_completion=False,
        )
        op.execute({})

        mock_waiter.assert_has_calls(
            [
                mock.call("replication_terminal_status"),
                mock.call().wait(
                    Filters=[{"Name": "replication-config-arn", "Values": ["arn:xxxxxx"]}],
                    WaiterConfig={"Delay": 60, "MaxAttempts": 60},
                ),
            ]
        )
        mock_handle.assert_called_once()
        mock_conn.delete_replication_config.assert_called_once()

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsDeleteReplicationConfigOperator, "handle_delete_wait")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_not_deprovisioned(self, mock_waiter, mock_handle, mock_describe, mock_conn):
        mock_describe.return_value = self.get_replication_status("stopped", "deprovisioning")
        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=False,
            wait_for_completion=False,
        )
        op.execute({})

        mock_waiter.assert_has_calls(
            [
                mock.call("replication_terminal_status"),
                mock.call().wait(
                    Filters=[{"Name": "replication-config-arn", "Values": ["arn:xxxxxx"]}],
                    WaiterConfig={"Delay": 60, "MaxAttempts": 60},
                ),
            ]
        )
        mock_handle.assert_called_once()

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_config_not_found(self, mock_waiter, mock_describe, mock_conn):
        mock_describe.return_value = []

        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=False,
            wait_for_completion=False,
        )
        with pytest.raises(IndexError):
            op.execute({})
        mock_waiter.assert_not_called()
        mock_conn.delete_replication_config.assert_not_called()

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_defer_not_deprovisioned(self, mock_waiter, mock_describe, mock_conn):
        # not deprovisioned
        mock_describe.return_value = self.get_replication_status("stopped", "deprovisioning")
        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred) as defer:
            op.execute({})

        assert isinstance(defer.value.trigger, DmsReplicationDeprovisionedTrigger)

        # not in terminal status
        mock_describe.return_value = self.get_replication_status("running", "deprovisioning")
        op = DmsDeleteReplicationConfigOperator(
            task_id="delete_replication_config",
            replication_config_arn=self.TASK_DATA["ReplicationConfigArn"],
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred) as defer:
            op.execute({})

        assert isinstance(defer.value.trigger, DmsReplicationTerminalStatusTrigger)


class TestDmsDescribeReplicationsOperator:
    FILTER = [{"Name": "replication-type", "Values": ["cdc"]}]

    def get_replications(self):
        return {
            "Replications": [
                {
                    "Status": "test",
                    "ReplicationArn": "XXXXXXXXXXXXXXXXXXXXXXXXX",
                    "ReplicationIdentifier": "test-config",
                    "SourceEndpointArn": "XXXXXXXXXXXXXXXXXXXXXXXXX",
                    "TargetEndpointArn": "XXXXXXXXXXXXXXXXXXXXXXXXX",
                }
            ]
        }

    @mock.patch.object(DmsHook, "conn")
    def test_filter(self, mock_conn):
        mock_conn.describe_replications.return_value = self.get_replications()

        op = DmsDescribeReplicationsOperator(
            task_id="test_task",
            filter=self.FILTER,
        )

        res = op.execute({})

        mock_conn.describe_replications.assert_called_once_with(Filters=self.FILTER)
        assert isinstance(res, list)

    @mock.patch.object(DmsHook, "conn")
    def test_filter_none(self, mock_conn):
        mock_conn.describe_replications.return_value = self.get_replications()

        op = DmsDescribeReplicationsOperator(
            task_id="test_task",
        )

        res = op.execute({})

        mock_conn.describe_replications.assert_called_once_with(Filters=[])
        assert isinstance(res, list)


class TestDmsStartReplicationOperator:
    def mock_describe_replication_response(self, status: str):
        return [
            {
                "ReplicationConfigIdentifier": "string",
                "ReplicationConfigArn": "string",
                "SourceEndpointArn": "string",
                "TargetEndpointArn": "string",
                "ReplicationType": "full-load",
                "Status": status,
            }
        ]

    def mock_replication_response(self, status: str):
        return {
            "Replication": {
                "ReplicationConfigIdentifier": "xxxx",
                "ReplicationConfigArn": "xxxx",
                "Status": status,
            }
        }

    def test_arg_validation(self):
        with pytest.raises(AirflowException):
            DmsStartReplicationOperator(
                task_id="start_replication",
                replication_config_arn="XXXXXXXXXXXXXXX",
                replication_start_type="cdc",
                cdc_start_pos=1,
                cdc_start_time="2024-01-01 00:00:00",
            )
        DmsStartReplicationOperator(
            task_id="start_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            replication_start_type="cdc",
            cdc_start_pos=1,
        )

        DmsStartReplicationOperator(
            task_id="start_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            replication_start_type="cdc",
            cdc_start_time="2024-01-01 00:00:00",
        )

    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsHook, "start_replication")
    def test_already_running(self, mock_replication, mock_describe):
        mock_describe.return_value = self.mock_describe_replication_response("test")

        op = DmsStartReplicationOperator(
            task_id="start_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            replication_start_type="cdc",
            cdc_start_pos=1,
            wait_for_completion=False,
            deferrable=False,
        )

        op.execute({})
        assert mock_replication.call_count == 0

        mock_describe.return_value = self.mock_describe_replication_response("failed")
        op.execute({})
        mock_replication.return_value = self.mock_replication_response("running")
        assert mock_replication.call_count == 1

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "describe_replications")
    def test_wait_for_completion(self, mock_describe, mock_waiter, mock_conn):
        mock_describe.return_value = self.mock_describe_replication_response("stopped")
        op = DmsStartReplicationOperator(
            task_id="start_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            replication_start_type="cdc",
            cdc_start_pos=1,
            wait_for_completion=True,
            deferrable=False,
        )

        op.execute({})
        mock_waiter.assert_called_with("replication_complete")
        mock_waiter.assert_called_once()

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    def test_execute(self, mock_describe, mock_conn):
        mock_describe.return_value = self.mock_describe_replication_response("stopped")

        op = DmsStartReplicationOperator(
            task_id="start_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            replication_start_type="cdc",
            cdc_start_pos=1,
            wait_for_completion=False,
            deferrable=False,
        )
        op.execute({})
        assert mock_conn.start_replication.call_count == 1


class TestDmsStopReplicationOperator:
    def mock_describe_replication_response(self, status: str):
        return [
            {
                "ReplicationConfigIdentifier": "string",
                "ReplicationConfigArn": "string",
                "SourceEndpointArn": "string",
                "TargetEndpointArn": "string",
                "ReplicationType": "full-load",
                "Status": status,
            }
        ]

    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsHook, "conn")
    def test_already_stopped(self, mock_conn, mock_describe_replications):
        mock_describe_replications.return_value = self.mock_describe_replication_response("stopped")

        op = DmsStopReplicationOperator(
            task_id="stop_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            wait_for_completion=False,
            deferrable=False,
        )
        op.execute({})

        assert mock_conn.stop_replication.call_count == 0

    @mock.patch.object(DmsHook, "stop_replication")
    @mock.patch.object(DmsHook, "describe_replications")
    def test_execute(self, mock_describe_replications, mock_stop):
        mock_describe_replications.return_value = self.mock_describe_replication_response("started")

        op = DmsStopReplicationOperator(
            task_id="stop_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            wait_for_completion=False,
            deferrable=False,
        )
        op.execute({})
        assert mock_stop.call_count == 1

    @mock.patch.object(DmsHook, "conn")
    @mock.patch.object(DmsHook, "describe_replications")
    @mock.patch.object(DmsHook, "get_waiter")
    def test_wait_for_completion(self, mock_get_waiter, mock_describe_replications, mock_conn):
        mock_describe_replications.return_value = self.mock_describe_replication_response("started")
        op = DmsStopReplicationOperator(
            task_id="stop_replication",
            replication_config_arn="XXXXXXXXXXXXXXX",
            wait_for_completion=True,
            deferrable=False,
        )

        op.execute({})
        mock_get_waiter.assert_called_with("replication_stopped")
        mock_get_waiter.assert_called_once()
