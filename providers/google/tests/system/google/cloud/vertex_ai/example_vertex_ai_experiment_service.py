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

from google.cloud.aiplatform.compat.types import execution_v1 as gca_execution

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.vertex_ai.experiment_service import (
    CreateExperimentOperator,
    CreateExperimentRunOperator,
    DeleteExperimentOperator,
    DeleteExperimentRunOperator,
    ListExperimentRunsOperator,
    UpdateExperimentRunStateOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_experiment_service_dag"
REGION = "us-central1"
EXPERIMENT_NAME = f"test-experiment-airflow-operator-{ENV_ID}"
EXPERIMENT_RUN_NAME_1 = f"test-experiment-run-airflow-operator-1-{ENV_ID}"
EXPERIMENT_RUN_NAME_2 = f"test-experiment-run-airflow-operator-2-{ENV_ID}"

with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with using experiment service.",
    schedule="@once",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=["example", "vertex_ai", "experiment_service"],
) as dag:
    # [START how_to_cloud_vertex_ai_create_experiment_operator]
    create_experiment_task = CreateExperimentOperator(
        task_id="create_experiment_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
    )
    # [END how_to_cloud_vertex_ai_create_experiment_operator]

    # [START how_to_cloud_vertex_ai_create_experiment_run_operator]
    create_experiment_run_1_task = CreateExperimentRunOperator(
        task_id="create_experiment_run_1_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME_1,
    )
    # [END how_to_cloud_vertex_ai_create_experiment_run_operator]

    create_experiment_run_2_task = CreateExperimentRunOperator(
        task_id="create_experiment_run_2_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME_2,
    )

    # [START how_to_cloud_vertex_ai_list_experiment_run_operator]
    list_experiment_runs_task = ListExperimentRunsOperator(
        task_id="list_experiment_runs_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
    )
    # [END how_to_cloud_vertex_ai_list_experiment_run_operator]

    # [START how_to_cloud_vertex_ai_update_experiment_run_state_operator]
    update_experiment_run_state_task = UpdateExperimentRunStateOperator(
        task_id="update_experiment_run_state_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME_2,
        new_state=gca_execution.Execution.State.COMPLETE,
    )
    # [END how_to_cloud_vertex_ai_update_experiment_run_state_operator]

    # [START how_to_cloud_vertex_ai_delete_experiment_run_operator]
    delete_experiment_run_1_task = DeleteExperimentRunOperator(
        task_id="delete_experiment_run_1_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME_1,
    )
    # [END how_to_cloud_vertex_ai_delete_experiment_run_operator]

    delete_experiment_run_2_task = DeleteExperimentRunOperator(
        task_id="delete_experiment_run_2_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
        experiment_run_name=EXPERIMENT_RUN_NAME_2,
    )

    # [START how_to_cloud_vertex_ai_delete_experiment_operator]
    delete_experiment_task = DeleteExperimentOperator(
        task_id="delete_experiment_task",
        project_id=PROJECT_ID,
        location=REGION,
        experiment_name=EXPERIMENT_NAME,
    )
    # [END how_to_cloud_vertex_ai_delete_experiment_operator]

    (
        create_experiment_task
        >> [create_experiment_run_1_task, create_experiment_run_2_task]
        >> list_experiment_runs_task
        >> update_experiment_run_state_task
        >> [delete_experiment_run_1_task, delete_experiment_run_2_task]
        >> delete_experiment_task
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
