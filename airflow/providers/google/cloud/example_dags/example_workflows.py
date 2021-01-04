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
    WorkflowExecutionsCancelExecutionOperator,
    WorkflowExecutionsCreateExecutionOperator,
    WorkflowExecutionsGetExecutionOperator,
    WorkflowExecutionsListExecutionsOperator,
    WorkflowsCreateWorkflowOperator,
    WorkflowsDeleteWorkflowOperator,
    WorkflowsGetWorkflowOperator,
    WorkflowsListWorkflowsOperator,
    WorkflowsUpdateWorkflowOperator,
)
from airflow.providers.google.cloud.sensors.workflows import WorkflowExecutionSensor
from airflow.utils.dates import days_ago

LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")

WORKFLOW_ID = "airflow-test-workflow"

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

EXECUTION = {"argument": ""}


with DAG("example_cloud_workflows", start_date=days_ago(1), schedule_interval=None) as dag:
    create_workflow = WorkflowsCreateWorkflowOperator(
        task_id="create_workflow",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow=WORKFLOW,
        workflow_id=WORKFLOW_ID,
    )
    update_workflows = WorkflowsUpdateWorkflowOperator(
        task_id="update_workflows",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
    )

    get_workflow = WorkflowsGetWorkflowOperator(
        task_id="get_workflow", location=LOCATION, project_id=PROJECT_ID, workflow_id=WORKFLOW_ID
    )
    list_workflows = WorkflowsListWorkflowsOperator(
        task_id="list_workflows", location=LOCATION, project_id=PROJECT_ID, filter_="airflow"
    )
    delete_workflow = WorkflowsDeleteWorkflowOperator(
        task_id="delete_workflow", location=LOCATION, project_id=PROJECT_ID, workflow_id=WORKFLOW_ID
    )

    create_execution = WorkflowExecutionsCreateExecutionOperator(
        task_id="create_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        execution=EXECUTION,
        workflow_id=WORKFLOW_ID,
    )
    wait_for_execution = WorkflowExecutionSensor(
        task_id="wait_for_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id=create_execution.output["execution_id"],
    )
    get_execution = WorkflowExecutionsGetExecutionOperator(
        task_id="get_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id=create_execution.output["execution_id"],
    )
    list_executions = WorkflowExecutionsListExecutionsOperator(
        task_id="list_executions", location=LOCATION, project_id=PROJECT_ID, workflow_id=WORKFLOW_ID
    )

    create_execution_for_cancel = WorkflowExecutionsCreateExecutionOperator(
        task_id="create_execution_for_cancel",
        location=LOCATION,
        project_id=PROJECT_ID,
        execution=EXECUTION,
        workflow_id=WORKFLOW_ID,
    )
    cancel_execution = WorkflowExecutionsCancelExecutionOperator(
        task_id="cancel_execution",
        location=LOCATION,
        project_id=PROJECT_ID,
        workflow_id=WORKFLOW_ID,
        execution_id=create_execution_for_cancel.output["execution_id"],
    )

    create_workflow >> update_workflows >> [get_workflow, list_workflows]
    update_workflows >> [create_execution, create_execution_for_cancel]

    create_execution >> wait_for_execution >> [get_execution, list_executions]
    create_execution_for_cancel >> cancel_execution

    [cancel_execution, list_executions] >> delete_workflow


if __name__ == '__main__':
    dag.clear(dag_run_state=None)
    dag.run()
