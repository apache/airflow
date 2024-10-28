#
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

from airflow.providers.amazon.aws.hooks.appflow import AppflowHook
from airflow.utils import timezone

FLOW_NAME = "flow0"
EXECUTION_ID = "ex_id"
CONNECTION_TYPE = "Salesforce"
REGION_NAME = "us-east-1"
AWS_CONN_ID = "aws_default"


@pytest.fixture
def hook():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.appflow.AppflowHook.conn"
    ) as mock_conn:
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
        yield AppflowHook(aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME)


def test_conn_attributes(hook):
    assert hasattr(hook, "conn")
    conn = hook.conn
    assert conn is hook.conn, "AppflowHook conn property non-cached"


def test_run_flow(hook):
    with mock.patch(
        "airflow.providers.amazon.aws.waiters.base_waiter.BaseBotoWaiter.waiter"
    ):
        hook.run_flow(flow_name=FLOW_NAME, poll_interval=0)
    hook.conn.describe_flow_execution_records.assert_called_with(flowName=FLOW_NAME)
    assert hook.conn.describe_flow_execution_records.call_count == 1
    hook.conn.start_flow.assert_called_once_with(flowName=FLOW_NAME)


def test_update_flow_filter(hook):
    tasks = [
        {
            "taskType": "Filter",
            "connectorOperator": {"Salesforce": "GREATER_THAN"},
            "sourceFields": ["col0"],
            "taskProperties": {"DATA_TYPE": "datetime", "VALUE": "1653523200000"},
        }
    ]
    hook.update_flow_filter(
        flow_name=FLOW_NAME, filter_tasks=tasks, set_trigger_ondemand=True
    )
    hook.conn.describe_flow.assert_called_with(flowName=FLOW_NAME)
    assert hook.conn.describe_flow.call_count == 1
    hook.conn.update_flow.assert_called_once_with(
        flowName=FLOW_NAME,
        tasks=tasks,
        description=ANY,
        destinationFlowConfigList=ANY,
        sourceFlowConfig=ANY,
        triggerConfig=ANY,
    )
