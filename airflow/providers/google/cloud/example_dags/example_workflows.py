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

import os

from airflow import DAG
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
from airflow.utils.dates import days_ago

LOCATION = os.environ.get("GCP_WORKFLOWS_LOCATION", "us-central1")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")

WORKFLOW_ID = os.getenv("GCP_WORKFLOWS_WORKFLOW_ID", "airflow-test-workflow")

# [START how_to_define_workflow]
WORKFLOW_CONTENT = """
- getCurrentTime:
    call: http.get
    args:
        url: https://us-central1-workflowsample.cloudfunctions.net/datetime
    result: currentTime
- readWikipedia:
    call: http.get
    args:
        url: https://en.wikipedia.org/w/api.php
        query:
            action: opensearch
            search: ${currentTime.body.dayOfTheWeek}
    result: wikiResult
- returnResult:
    return: ${wikiResult.body[1]}
"""

WORKFLOW = {
    "description": "Test workflow",
    "labels": {"airflow-version": "dev"},
    "source_contents": WORKFLOW_CONTENT,
}
# [END how_to_define_workflow]

EXECUTION = {"argument": ""}

SLEEP_WORKFLOW_ID = os.getenv("GCP_WORKFLOWS_SLEEP_WORKFLOW_ID", "sleep_workflow")
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


with DAG("example_cloud_workflows", start_date=days_ago(1), schedule_interval=None) as dag:
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
    update_workflows = WorkflowsUpdateWorkflowOperator(
        task_id="update_workflows",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        update_mask={"paths": ["name", "description"]},
    )
    # [END how_to_update_workflow]

    # [START how_to_get_workflow]
    get_workflow = WorkflowsGetWorkflowOperator(
        task_id="get_workflow", location=LOCATION, project_id=PROJECT_ID, workflow_id=WORKFLOW_ID
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
        task_id="delete_workflow", location=LOCATION, project_id=PROJECT_ID, workflow_id=WORKFLOW_ID
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

    # [START how_to_wait_for_execution]
    wait_for_execution = WorkflowExecutionSensor(
        task_id="wait_for_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id='{{ task_instance.xcom_pull("create_execution", key="execution_id") }}',
    )
    # [END how_to_wait_for_execution]

    # [START how_to_get_execution]
    get_execution = WorkflowsGetExecutionOperator(
        task_id="get_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id='{{ task_instance.xcom_pull("create_execution", key="execution_id") }}',
    )
    # [END how_to_get_execution]

    # [START how_to_list_executions]
    list_executions = WorkflowsListExecutionsOperator(
        task_id="list_executions", location=LOCATION, project_id=PROJECT_ID, workflow_id=WORKFLOW_ID
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

    # [START how_to_cancel_execution]
    cancel_execution = WorkflowsCancelExecutionOperator(
        task_id="cancel_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=SLEEP_WORKFLOW_ID,
        execution_id='{{ task_instance.xcom_pull("create_execution_for_cancel", key="execution_id") }}',
    )
    # [END how_to_cancel_execution]

    create_workflow >> update_workflows >> [get_workflow, list_workflows]
    update_workflows >> [create_execution, create_execution_for_cancel]

    create_execution >> wait_for_execution >> [get_execution, list_executions]
    create_workflow_for_cancel >> create_execution_for_cancel >> cancel_execution

    [cancel_execution, list_executions] >> delete_workflow


if __name__ == '__main__':
    dag.clear(dag_run_state=None)
    dag.run()
