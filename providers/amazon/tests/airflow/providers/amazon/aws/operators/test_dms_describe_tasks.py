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

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.operators.dms import DmsDescribeTasksOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = datetime(2018, 1, 1)
MOCK_TASK_ARN = "test_arn"
FILTER = {"Name": "replication-task-arn", "Values": [MOCK_TASK_ARN]}
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
        "ReplicationTaskArn": MOCK_TASK_ARN,
        "Status": "creating",
    }
]


class TestDmsDescribeTasksOperator:
    def setup_method(self):
        args = {
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        }

        self.dag = DAG(f"{TEST_DAG_ID}test_schedule_dag_once", default_args=args, schedule="@once")

    def test_init(self):
        dms_operator = DmsDescribeTasksOperator(
            task_id="describe_tasks", describe_tasks_kwargs={"Filters": [FILTER]}
        )

        assert dms_operator.describe_tasks_kwargs == {"Filters": [FILTER]}

    @mock.patch.object(DmsHook, "describe_replication_tasks", return_value=(None, MOCK_RESPONSE))
    @mock.patch.object(DmsHook, "get_conn")
    def test_describe_tasks(self, mock_conn, mock_describe_replication_tasks):
        describe_tasks_kwargs = {"Filters": [FILTER]}
        describe_task = DmsDescribeTasksOperator(
            task_id="describe_tasks", describe_tasks_kwargs=describe_tasks_kwargs
        )
        describe_task.execute(None)

        mock_describe_replication_tasks.assert_called_once_with(**describe_tasks_kwargs)

    @mock.patch.object(DmsHook, "describe_replication_tasks", return_value=(None, MOCK_RESPONSE))
    @mock.patch.object(DmsHook, "get_conn")
    def test_describe_tasks_return_value(self, mock_conn, mock_describe_replication_tasks):
        describe_task = DmsDescribeTasksOperator(
            task_id="describe_tasks", dag=self.dag, describe_tasks_kwargs={"Filters": [FILTER]}
        )

        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=timezone.utcnow(), run_id="test")
        ti = TaskInstance(task=describe_task)
        ti.dag_run = dag_run
        marker, response = describe_task.execute(ti.get_template_context())

        assert marker is None
        assert response == MOCK_RESPONSE
