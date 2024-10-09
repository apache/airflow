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

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.dataform_v1beta1 import WorkflowInvocation

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteDatasetOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateRepositoryOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformCreateWorkspaceOperator,
    DataformDeleteRepositoryOperator,
    DataformDeleteWorkspaceOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
    DataformInstallNpmPackagesOperator,
    DataformMakeDirectoryOperator,
    DataformQueryWorkflowInvocationActionsOperator,
    DataformRemoveDirectoryOperator,
    DataformRemoveFileOperator,
    DataformWriteFileOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from airflow.providers.google.cloud.utils.dataform import make_initialization_workspace_flow
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dataform"

REPOSITORY_ID = f"example_dataform_repository_{ENV_ID}"
REGION = "us-central1"
WORKSPACE_ID = f"example_dataform_workspace_{ENV_ID}"
DATAFORM_SCHEMA_NAME = f"schema_{DAG_ID}_{ENV_ID}"

# This DAG is not self-run we need to do some extra configuration to execute it in automation process
with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataform"],
) as dag:
    # [START howto_operator_create_repository]
    make_repository = DataformCreateRepositoryOperator(
        task_id="make-repository",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
    )
    # [END howto_operator_create_repository]

    # [START howto_operator_create_workspace]
    make_workspace = DataformCreateWorkspaceOperator(
        task_id="make-workspace",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
    )
    # [END howto_operator_create_workspace]

    # [START howto_initialize_workspace]
    first_initialization_step, last_initialization_step = make_initialization_workspace_flow(
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        package_name=f"dataform_package_{ENV_ID}",
        without_installation=True,
        dataform_schema_name=DATAFORM_SCHEMA_NAME,
    )
    # [END howto_initialize_workspace]

    # [START howto_operator_install_npm_packages]
    install_npm_packages = DataformInstallNpmPackagesOperator(
        task_id="install-npm-packages",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
    )
    # [END howto_operator_install_npm_packages]

    # [START howto_operator_create_compilation_result]
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create-compilation-result",
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
        task_id="get-compilation-result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result_id=(
            "{{ task_instance.xcom_pull('create-compilation-result')['name'].split('/')[-1] }}"
        ),
    )
    # [END howto_operator_get_compilation_result]]

    # [START howto_operator_create_workflow_invocation]
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
    )
    # [END howto_operator_create_workflow_invocation]

    # [START howto_operator_create_workflow_invocation_async]
    create_workflow_invocation_async = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation-async",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        asynchronous=True,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
    )

    is_workflow_invocation_done = DataformWorkflowInvocationStateSensor(
        task_id="is-workflow-invocation-done",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create-workflow-invocation')['name'].split('/')[-1] }}"
        ),
        expected_statuses={WorkflowInvocation.State.SUCCEEDED},
    )
    # [END howto_operator_create_workflow_invocation_async]

    # [START howto_operator_get_workflow_invocation]
    get_workflow_invocation = DataformGetWorkflowInvocationOperator(
        task_id="get-workflow-invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create-workflow-invocation')['name'].split('/')[-1] }}"
        ),
    )
    # [END howto_operator_get_workflow_invocation]

    # [START howto_operator_query_workflow_invocation_actions]
    query_workflow_invocation_actions = DataformQueryWorkflowInvocationActionsOperator(
        task_id="query-workflow-invocation-actions",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create-workflow-invocation')['name'].split('/')[-1] }}"
        ),
    )
    # [END howto_operator_query_workflow_invocation_actions]

    create_workflow_invocation_for_cancel = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation-for-cancel",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
        asynchronous=True,
    )

    # [START howto_operator_cancel_workflow_invocation]
    cancel_workflow_invocation = DataformCancelWorkflowInvocationOperator(
        task_id="cancel-workflow-invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=(
            "{{ task_instance.xcom_pull('create-workflow-invocation-for-cancel')['name'].split('/')[-1] }}"
        ),
    )
    # [END howto_operator_cancel_workflow_invocation]

    # [START howto_operator_make_directory]
    make_test_directory = DataformMakeDirectoryOperator(
        task_id="make-test-directory",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        directory_path="test",
    )
    # [END howto_operator_make_directory]

    # [START howto_operator_write_file]
    test_file_content = b"""
    test test for test file
    """
    write_test_file = DataformWriteFileOperator(
        task_id="make-test-file",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        filepath="test/test.txt",
        contents=test_file_content,
    )
    # [END howto_operator_write_file]

    # [START howto_operator_remove_file]
    remove_test_file = DataformRemoveFileOperator(
        task_id="remove-test-file",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        filepath="test/test.txt",
    )
    # [END howto_operator_remove_file]

    # [START howto_operator_remove_directory]
    remove_test_directory = DataformRemoveDirectoryOperator(
        task_id="remove-test-directory",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        directory_path="test",
    )
    # [END howto_operator_remove_directory]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete-dataset",
        dataset_id=DATAFORM_SCHEMA_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_delete_workspace]
    delete_workspace = DataformDeleteWorkspaceOperator(
        task_id="delete-workspace",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_delete_workspace]

    # [START howto_operator_delete_repository]
    delete_repository = DataformDeleteRepositoryOperator(
        task_id="delete-repository",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_delete_repository]

    (
        # TEST SETUP
        make_repository
        >> make_workspace
        # TEST BODY
        >> first_initialization_step
    )
    (
        last_initialization_step
        >> install_npm_packages
        >> create_compilation_result
        >> get_compilation_result
        >> create_workflow_invocation
        >> get_workflow_invocation
        >> query_workflow_invocation_actions
        >> create_workflow_invocation_async
        >> is_workflow_invocation_done
        >> create_workflow_invocation_for_cancel
        >> cancel_workflow_invocation
        >> make_test_directory
        >> write_test_file
        # TEST TEARDOWN
        >> remove_test_file
        >> remove_test_directory
        >> delete_dataset
        >> delete_workspace
        >> delete_repository
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
