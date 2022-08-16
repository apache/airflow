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

"""
Example Airflow DAG for Google Cloud Dataform service
"""
import os
from datetime import datetime

from google.cloud.dataform_v1beta1 import WorkflowInvocation

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "test-project-id")

DAG_ID = "dataform"

REPOSITORY_ID = "dataform-test2"
REGION = "us-central1"
WORKSPACE_ID = "testing"

# This DAG is not self-run we need to do some extra configuration to execute it in automation process

with models.DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'dataform'],
) as dag:
    # [START howto_operator_create_compilation_result]
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                f"workspaces/{WORKSPACE_ID}"
            ),
        },
    )
    # [END howto_operator_create_compilation_result]

    # [START howto_operator_get_compilation_result]
    get_compilation_result = DataformGetCompilationResultOperator(
        task_id="get_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result_id=(
            "{{ task_instance.xcom_pull('create_compilation_result')['name'].split('/')[-1] }}"
        ),
    )
    # [END howto_operator_get_compilation_result]]

    # [START howto_operator_create_workflow_invocation]
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )
    # [END howto_operator_create_workflow_invocation]

    # [START howto_operator_create_workflow_invocation_async]
    create_workflow_invocation_async = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation_async',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        asynchronous=True,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )

    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
        task_id="is_workflow_invocation_done",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"
        ),
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
    )
    # [END howto_operator_create_workflow_invocation_async]

    # [START howto_operator_get_workflow_invocation]
    get_workflow_invocation = DataformGetWorkflowInvocationOperator(
        task_id='get_workflow_invocation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}"
        ),
    )
    # [END howto_operator_get_workflow_invocation]

    create_second_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_second_workflow_invocation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )

    # [START howto_operator_cancel_workflow_invocation]
    cancel_workflow_invocation = DataformCancelWorkflowInvocationOperator(
        task_id='cancel_workflow_incoation',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create_second_workflow_invocation')['name'].split('/')[-1] }}"
        ),
    )
    # [END howto_operator_cancel_workflow_invocation]

    chain(
        create_compilation_result,
        get_compilation_result,
        create_workflow_invocation >> get_workflow_invocation,
        create_workflow_invocation_async >> is_workflow_invocation_done,
        create_second_workflow_invocation >> cancel_workflow_invocation,
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
