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

from datetime import datetime
from unittest import mock
from unittest.mock import ANY

import pytest

from airflow.providers.amazon.aws.operators.appflow import (
    AppflowRecordsShortCircuitOperator,
    AppflowRunAfterOperator,
    AppflowRunBeforeOperator,
    AppflowRunDailyOperator,
    AppflowRunFullOperator,
    AppflowRunOperator,
)
from airflow.utils import timezone

CONN_ID = "aws_default"
DAG_ID = "dag_id"
TASK_ID = "task_id"
SHORT_CIRCUIT_TASK_ID = "short_circuit_task_id"
FLOW_NAME = "flow0"
EXECUTION_ID = "ex_id"
CONNECTION_TYPE = "Salesforce"
SOURCE = "salesforce"

DUMP_COMMON_ARGS = {"aws_conn_id": CONN_ID, "task_id": TASK_ID, "source": SOURCE, "flow_name": FLOW_NAME}


@pytest.fixture
def ctx(create_task_instance):
    ti = create_task_instance(
        dag_id=DAG_ID,
        task_id=TASK_ID,
        schedule_interval="0 12 * * *",
    )
    yield {"task_instance": ti}


@pytest.fixture
def appflow_conn():
    with mock.patch("airflow.providers.amazon.aws.hooks.appflow.AppflowHook.conn") as mock_conn:
        mock_conn.describe_flow.return_value = {
            'sourceFlowConfig': {'connectorType': CONNECTION_TYPE},
            'tasks': [],
            'triggerConfig': {'triggerProperties': None},
            'flowName': FLOW_NAME,
            'destinationFlowConfigList': {},
            'lastRunExecutionDetails': {
                'mostRecentExecutionStatus': 'Successful',
                'mostRecentExecutionTime': datetime(3000, 1, 1, tzinfo=timezone.utc),
            },
        }
        mock_conn.update_flow.return_value = {}
        mock_conn.start_flow.return_value = {"executionId": EXECUTION_ID}
        mock_conn.describe_flow_execution_records.return_value = {
            "flowExecutions": [{"executionId": EXECUTION_ID, "executionResult": {"recordsProcessed": 1}}]
        }
        yield mock_conn


def run_assertions_base(appflow_conn, tasks):
    appflow_conn.describe_flow.assert_called_with(flowName=FLOW_NAME)
    assert appflow_conn.describe_flow.call_count == 3
    appflow_conn.update_flow.assert_called_once_with(
        flowName=FLOW_NAME,
        tasks=tasks,
        description=ANY,
        destinationFlowConfigList=ANY,
        sourceFlowConfig=ANY,
        triggerConfig=ANY,
    )
    appflow_conn.start_flow.assert_called_once_with(flowName=FLOW_NAME)


def test_run(appflow_conn, ctx):
    operator = AppflowRunOperator(**DUMP_COMMON_ARGS)
    operator.execute(ctx)  # type: ignore
    appflow_conn.describe_flow.assert_called_with(flowName=FLOW_NAME)
    assert appflow_conn.describe_flow.call_count == 2
    appflow_conn.start_flow.assert_called_once_with(flowName=FLOW_NAME)


def test_run_full(appflow_conn, ctx):
    operator = AppflowRunFullOperator(**DUMP_COMMON_ARGS)
    operator.execute(ctx)  # type: ignore
    run_assertions_base(appflow_conn, [])


def test_run_after(appflow_conn, ctx):
    operator = AppflowRunAfterOperator(source_field="col0", filter_date="2022-05-26", **DUMP_COMMON_ARGS)
    operator.execute(ctx)  # type: ignore
    run_assertions_base(
        appflow_conn,
        [
            {
                'taskType': 'Filter',
                'connectorOperator': {'Salesforce': 'GREATER_THAN'},
                'sourceFields': ['col0'],
                'taskProperties': {'DATA_TYPE': 'datetime', 'VALUE': '1653523200000'},
            }
        ],
    )


def test_run_before(appflow_conn, ctx):
    operator = AppflowRunBeforeOperator(source_field="col0", filter_date="2022-05-26", **DUMP_COMMON_ARGS)
    operator.execute(ctx)  # type: ignore
    run_assertions_base(
        appflow_conn,
        [
            {
                'taskType': 'Filter',
                'connectorOperator': {'Salesforce': 'LESS_THAN'},
                'sourceFields': ['col0'],
                'taskProperties': {'DATA_TYPE': 'datetime', 'VALUE': '1653523200000'},
            }
        ],
    )


def test_run_daily(appflow_conn, ctx):
    operator = AppflowRunDailyOperator(source_field="col0", filter_date="2022-05-26", **DUMP_COMMON_ARGS)
    operator.execute(ctx)  # type: ignore
    run_assertions_base(
        appflow_conn,
        [
            {
                'taskType': 'Filter',
                'connectorOperator': {'Salesforce': 'BETWEEN'},
                'sourceFields': ['col0'],
                'taskProperties': {
                    'DATA_TYPE': 'datetime',
                    'LOWER_BOUND': '1653523199999',
                    'UPPER_BOUND': '1653609600000',
                },
            }
        ],
    )


def test_short_circuit(appflow_conn, ctx):
    with mock.patch("airflow.models.TaskInstance.xcom_pull") as mock_xcom_pull:
        with mock.patch("airflow.models.TaskInstance.xcom_push") as mock_xcom_push:
            mock_xcom_pull.return_value = EXECUTION_ID
            operator = AppflowRecordsShortCircuitOperator(
                task_id=SHORT_CIRCUIT_TASK_ID,
                flow_name=FLOW_NAME,
                appflow_run_task_id=TASK_ID,
            )
            operator.execute(ctx)  # type: ignore
            appflow_conn.describe_flow_execution_records.assert_called_once_with(
                flowName=FLOW_NAME, maxResults=100
            )
            mock_xcom_push.assert_called_with("records_processed", 1)
