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

import os
from datetime import datetime
from typing import cast

from google.protobuf.field_mask_pb2 import FieldMask

from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.workflows import (
    WorkflowsCancelExecutionOperator,
    WorkflowsCreateExecutionOperator,
    WorkflowsCreateWorkflowOperator,
    WorkflowsDeleteWorkflowOperator,
    WorkflowsGetExecutionOperator,
    WorkflowsGetWorkflowOperator,
    WorkflowsListExecutionsOperator,
    WorkflowsListWorkflowsOperator,
    WorkflowsUpdateWorkflowOperator,
)
from airflow.providers.google.cloud.sensors.workflows import WorkflowExecutionSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

DAG_ID = "cloud_workflows"

LOCATION = "us-central1"
WORKFLOW_ID = f"workflow-{DAG_ID}-{ENV_ID}".replace("_", "-")

# [START how_to_define_workflow]
WORKFLOW_CONTENT = """
- getLanguage:
    assign:
        - inputLanguage: "English"
- readWikipedia:
    call: http.get
    args:
        url: https://www.wikipedia.org/
        query:
            action: opensearch
            search: ${inputLanguage}
    result: wikiResult
- returnResult:
    return: ${wikiResult}
"""

WORKFLOW = {
    "description": "Test workflow",
    "labels": {"airflow-version": "dev"},
    "source_contents": WORKFLOW_CONTENT,
}
# [END how_to_define_workflow]

EXECUTION = {"argument": ""}

SLEEP_WORKFLOW_ID = f"sleep-workflow-{DAG_ID}-{ENV_ID}".replace("_", "-")
SLEEP_WORKFLOW_CONTENT = """
- someSleep:
    call: sys.sleep
    args:
        seconds: 120
"""

SLEEP_WORKFLOW = {
    "description": "Test workflow",
    "labels": {"airflow-version": "dev"},
    "source_contents": SLEEP_WORKFLOW_CONTENT,
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "workflows"],
) as dag:
    # [START how_to_create_workflow]
    create_workflow = WorkflowsCreateWorkflowOperator(
        task_id="create_workflow",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow=WORKFLOW,
        workflow_id=WORKFLOW_ID,
    )
    # [END how_to_create_workflow]

    # [START how_to_update_workflow]
    update_workflow = WorkflowsUpdateWorkflowOperator(
        task_id="update_workflow",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        update_mask=FieldMask(paths=["name", "description"]),
    )
    # [END how_to_update_workflow]

    # [START how_to_get_workflow]
    get_workflow = WorkflowsGetWorkflowOperator(
        task_id="get_workflow",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
    )
    # [END how_to_get_workflow]

    # [START how_to_list_workflows]
    list_workflows = WorkflowsListWorkflowsOperator(
        task_id="list_workflows",
        location=LOCATION,
        project_id=PROJECT_ID,
    )
    # [END how_to_list_workflows]

    # [START how_to_delete_workflow]
    delete_workflow = WorkflowsDeleteWorkflowOperator(
        task_id="delete_workflow",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_delete_workflow]

    # [START how_to_create_execution]
    create_execution = WorkflowsCreateExecutionOperator(
        task_id="create_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        execution=EXECUTION,
        workflow_id=WORKFLOW_ID,
    )
    # [END how_to_create_execution]

    create_execution_id = cast(str, XComArg(create_execution, key="execution_id"))

    # [START how_to_wait_for_execution]
    wait_for_execution = WorkflowExecutionSensor(
        task_id="wait_for_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id=create_execution_id,
    )
    # [END how_to_wait_for_execution]

    # [START how_to_get_execution]
    get_execution = WorkflowsGetExecutionOperator(
        task_id="get_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id=create_execution_id,
    )
    # [END how_to_get_execution]

    # [START how_to_list_executions]
    list_executions = WorkflowsListExecutionsOperator(
        task_id="list_executions",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
    )
    # [END how_to_list_executions]

    create_workflow_for_cancel = WorkflowsCreateWorkflowOperator(
        task_id="create_workflow_for_cancel",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow=SLEEP_WORKFLOW,
        workflow_id=SLEEP_WORKFLOW_ID,
    )

    create_execution_for_cancel = WorkflowsCreateExecutionOperator(
        task_id="create_execution_for_cancel",
        location=LOCATION,
        project_id=PROJECT_ID,
        execution=EXECUTION,
        workflow_id=SLEEP_WORKFLOW_ID,
    )

    cancel_execution_id = cast(
        str, XComArg(create_execution_for_cancel, key="execution_id")
    )

    # [START how_to_cancel_execution]
    cancel_execution = WorkflowsCancelExecutionOperator(
        task_id="cancel_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=SLEEP_WORKFLOW_ID,
        execution_id=cancel_execution_id,
    )
    # [END how_to_cancel_execution]

    delete_workflow_for_cancel = WorkflowsDeleteWorkflowOperator(
        task_id="delete_workflow_for_cancel",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=SLEEP_WORKFLOW_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_workflow >> update_workflow >> [get_workflow, list_workflows]
    update_workflow >> [create_execution, create_execution_for_cancel]

    wait_for_execution >> [get_execution, list_executions]
    (
        create_workflow_for_cancel
        >> create_execution_for_cancel
        >> cancel_execution
        >> delete_workflow_for_cancel
    )

    [cancel_execution, list_executions] >> delete_workflow

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
