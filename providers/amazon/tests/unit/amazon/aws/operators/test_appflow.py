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

from datetime import datetime
from unittest import mock
from unittest.mock import ANY

import pytest

from airflow.providers.amazon.aws.operators.appflow import (
    AppflowBaseOperator,
    AppflowRecordsShortCircuitOperator,
    AppflowRunAfterOperator,
    AppflowRunBeforeOperator,
    AppflowRunDailyOperator,
    AppflowRunFullOperator,
    AppflowRunOperator,
)

from tests_common.test_utils.compat import timezone

CONN_ID = "aws_default"
DAG_ID = "dag_id"
TASK_ID = "task_id"
SHORT_CIRCUIT_TASK_ID = "short_circuit_task_id"
FLOW_NAME = "flow0"
EXECUTION_ID = "ex_id"
CONNECTION_TYPE = "Salesforce"
SOURCE = "salesforce"

DUMP_COMMON_ARGS = {
    "aws_conn_id": CONN_ID,
    "task_id": TASK_ID,
    "source": SOURCE,
    "flow_name": FLOW_NAME,
    "poll_interval": 0,
}
AppflowBaseOperator.UPDATE_PROPAGATION_TIME = 0  # avoid wait


@pytest.fixture
def ctx(create_task_instance, session):
    ti = create_task_instance(
        dag_id=DAG_ID,
        task_id=TASK_ID,
        schedule="0 12 * * *",
    )
    session.add(ti)
    session.commit()
    return {"task_instance": ti}


@pytest.fixture
def appflow_conn():
    with mock.patch("airflow.providers.amazon.aws.hooks.appflow.AppflowHook.conn") as mock_conn:
        mock_conn.describe_flow.return_value = {
            "sourceFlowConfig": {"connectorType": CONNECTION_TYPE},
            "tasks": [],
            "triggerConfig": {"triggerProperties": None},
            "flowName": FLOW_NAME,
            "destinationFlowConfigList": {},
            "lastRunExecutionDetails": {
                "mostRecentExecutionStatus": "Successful",
                "mostRecentExecutionTime": datetime(3000, 1, 1, tzinfo=timezone.utc),
            },
        }
        mock_conn.update_flow.return_value = {}
        mock_conn.start_flow.return_value = {"executionId": EXECUTION_ID}
        mock_conn.describe_flow_execution_records.return_value = {
            "flowExecutions": [
                {
                    "executionId": EXECUTION_ID,
                    "executionResult": {"recordsProcessed": 1},
                    "executionStatus": "Successful",
                }
            ]
        }
        yield mock_conn


@pytest.fixture
def waiter_mock():
    with mock.patch("airflow.providers.amazon.aws.waiters.base_waiter.BaseBotoWaiter.waiter") as waiter:
        yield waiter


def run_assertions_base(appflow_conn, tasks):
    appflow_conn.describe_flow.assert_called_with(flowName=FLOW_NAME)
    assert appflow_conn.describe_flow.call_count == 2
    appflow_conn.describe_flow_execution_records.assert_called_once()
    appflow_conn.update_flow.assert_called_once_with(
        flowName=FLOW_NAME,
        tasks=tasks,
        description=ANY,
        destinationFlowConfigList=ANY,
        sourceFlowConfig=ANY,
        triggerConfig=ANY,
    )
    appflow_conn.start_flow.assert_called_once_with(flowName=FLOW_NAME)


@pytest.mark.db_test
def test_run(appflow_conn, ctx, waiter_mock):
    args = DUMP_COMMON_ARGS.copy()
    args.pop("source")
    operator = AppflowRunOperator(**args)
    operator.execute(ctx)
    appflow_conn.start_flow.assert_called_once_with(flowName=FLOW_NAME)
    appflow_conn.describe_flow_execution_records.assert_called_once()


@pytest.mark.db_test
def test_run_full(appflow_conn, ctx, waiter_mock):
    operator = AppflowRunFullOperator(**DUMP_COMMON_ARGS)
    operator.execute(ctx)
    run_assertions_base(appflow_conn, [])


@pytest.mark.db_test
def test_run_after(appflow_conn, ctx, waiter_mock):
    operator = AppflowRunAfterOperator(
        source_field="col0", filter_date="2022-05-26T00:00+00:00", **DUMP_COMMON_ARGS
    )
    operator.execute(ctx)
    run_assertions_base(
        appflow_conn,
        [
            {
                "taskType": "Filter",
                "connectorOperator": {"Salesforce": "GREATER_THAN"},
                "sourceFields": ["col0"],
                "taskProperties": {"DATA_TYPE": "datetime", "VALUE": "1653523200000"},
            }
        ],
    )


@pytest.mark.db_test
def test_run_before(appflow_conn, ctx, waiter_mock):
    operator = AppflowRunBeforeOperator(
        source_field="col0", filter_date="2022-05-26T00:00+00:00", **DUMP_COMMON_ARGS
    )
    operator.execute(ctx)
    run_assertions_base(
        appflow_conn,
        [
            {
                "taskType": "Filter",
                "connectorOperator": {"Salesforce": "LESS_THAN"},
                "sourceFields": ["col0"],
                "taskProperties": {"DATA_TYPE": "datetime", "VALUE": "1653523200000"},
            }
        ],
    )


@pytest.mark.db_test
def test_run_daily(appflow_conn, ctx, waiter_mock):
    operator = AppflowRunDailyOperator(
        source_field="col0", filter_date="2022-05-26T00:00+00:00", **DUMP_COMMON_ARGS
    )
    operator.execute(ctx)
    run_assertions_base(
        appflow_conn,
        [
            {
                "taskType": "Filter",
                "connectorOperator": {"Salesforce": "BETWEEN"},
                "sourceFields": ["col0"],
                "taskProperties": {
                    "DATA_TYPE": "datetime",
                    "LOWER_BOUND": "1653523199999",
                    "UPPER_BOUND": "1653609600000",
                },
            }
        ],
    )


@pytest.mark.db_test
def test_short_circuit(appflow_conn, ctx):
    with mock.patch("airflow.models.TaskInstance.xcom_pull") as mock_xcom_pull:
        with mock.patch("airflow.models.TaskInstance.xcom_push") as mock_xcom_push:
            mock_xcom_pull.return_value = EXECUTION_ID
            operator = AppflowRecordsShortCircuitOperator(
                task_id=SHORT_CIRCUIT_TASK_ID,
                flow_name=FLOW_NAME,
                appflow_run_task_id=TASK_ID,
            )
            operator.execute(ctx)
            appflow_conn.describe_flow_execution_records.assert_called_once_with(
                flowName=FLOW_NAME, maxResults=100
            )
            mock_xcom_push.assert_called_with("records_processed", 1)


@pytest.mark.parametrize(
    ("op_class", "op_base_args"),
    [
        pytest.param(
            AppflowRunAfterOperator,
            dict(**DUMP_COMMON_ARGS, source_field="col0", filter_date="2022-05-26T00:00+00:00"),
            id="run-after-op",
        ),
        pytest.param(
            AppflowRunBeforeOperator,
            dict(**DUMP_COMMON_ARGS, source_field="col1", filter_date="2077-10-23T00:03+00:00"),
            id="run-before-op",
        ),
        pytest.param(
            AppflowRunDailyOperator,
            dict(**DUMP_COMMON_ARGS, source_field="col2", filter_date="2023-10-20T12:22+00:00"),
            id="run-daily-op",
        ),
        pytest.param(AppflowRunFullOperator, DUMP_COMMON_ARGS, id="run-full-op"),
        pytest.param(
            AppflowRunOperator,
            dict((i, DUMP_COMMON_ARGS[i]) for i in DUMP_COMMON_ARGS if i != "source"),
            id="run-op",
        ),
        pytest.param(
            AppflowRecordsShortCircuitOperator,
            dict(task_id=SHORT_CIRCUIT_TASK_ID, flow_name=FLOW_NAME, appflow_run_task_id=TASK_ID),
            id="records-short-circuit",
        ),
    ],
)
def test_base_aws_op_attributes(op_class, op_base_args):
    op = op_class(**op_base_args)
    hook = op.hook
    assert hook is op.hook
    assert hook.aws_conn_id == CONN_ID
    assert hook._region_name is None
    assert hook._verify is None
    assert hook._config is None

    op = op_class(**op_base_args, region_name="eu-west-1", verify=False, botocore_config={"read_timeout": 42})
    hook = op.hook
    assert hook is op.hook
    assert hook.aws_conn_id == CONN_ID
    assert hook._region_name == "eu-west-1"
    assert hook._verify is False
    assert hook._config.read_timeout == 42
