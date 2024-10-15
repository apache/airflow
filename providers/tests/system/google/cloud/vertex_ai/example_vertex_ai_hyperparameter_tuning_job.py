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
Example Airflow DAG for Google Vertex AI service testing Hyperparameter Tuning Job operations.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud import aiplatform

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vertex_ai.hyperparameter_tuning_job import (
    CreateHyperparameterTuningJobOperator,
    DeleteHyperparameterTuningJobOperator,
    GetHyperparameterTuningJobOperator,
    ListHyperparameterTuningJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_hyperparameter_tuning_job_operations"
REGION = "us-central1"
DISPLAY_NAME = f"hyperparameter-tuning-job-{ENV_ID}"

DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_hyperparameter_tuning_job_{ENV_ID}"
STAGING_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}"
REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
WORKER_POOL_SPECS = [
    {
        "machine_spec": {
            "machine_type": MACHINE_TYPE,
            "accelerator_type": ACCELERATOR_TYPE,
            "accelerator_count": ACCELERATOR_COUNT,
        },
        "replica_count": REPLICA_COUNT,
        "container_spec": {
            "image_uri": "us-docker.pkg.dev/composer-256318/horse-human/horse-human-image:latest",
        },
    }
]
PARAM_SPECS = {
    "learning_rate": aiplatform.hyperparameter_tuning.DoubleParameterSpec(min=0.01, max=1, scale="log"),
    "momentum": aiplatform.hyperparameter_tuning.DoubleParameterSpec(min=0, max=1, scale="linear"),
    "num_neurons": aiplatform.hyperparameter_tuning.DiscreteParameterSpec(
        values=[64, 128, 512], scale="linear"
    ),
}
METRIC_SPEC = {
    "accuracy": "maximize",
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "hyperparameter_tuning_job"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )

    # [START how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator]
    create_hyperparameter_tuning_job = CreateHyperparameterTuningJobOperator(
        task_id="create_hyperparameter_tuning_job",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME,
        worker_pool_specs=WORKER_POOL_SPECS,
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
        parameter_spec=PARAM_SPECS,
        metric_spec=METRIC_SPEC,
        max_trial_count=15,
        parallel_trial_count=3,
    )
    # [END how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator]

    # [START how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator_deferrable]
    create_hyperparameter_tuning_job_def = CreateHyperparameterTuningJobOperator(
        task_id="create_hyperparameter_tuning_job_def",
        staging_bucket=STAGING_BUCKET,
        display_name=DISPLAY_NAME,
        worker_pool_specs=WORKER_POOL_SPECS,
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
        parameter_spec=PARAM_SPECS,
        metric_spec=METRIC_SPEC,
        max_trial_count=15,
        parallel_trial_count=3,
        deferrable=True,
    )
    # [END how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator_deferrable]

    # [START how_to_cloud_vertex_ai_get_hyperparameter_tuning_job_operator]
    get_hyperparameter_tuning_job = GetHyperparameterTuningJobOperator(
        task_id="get_hyperparameter_tuning_job",
        project_id=PROJECT_ID,
        region=REGION,
        hyperparameter_tuning_job_id="{{ task_instance.xcom_pull("
        "task_ids='create_hyperparameter_tuning_job', key='hyperparameter_tuning_job_id') }}",
    )
    # [END how_to_cloud_vertex_ai_get_hyperparameter_tuning_job_operator]

    # [START how_to_cloud_vertex_ai_delete_hyperparameter_tuning_job_operator]
    delete_hyperparameter_tuning_job = DeleteHyperparameterTuningJobOperator(
        task_id="delete_hyperparameter_tuning_job",
        project_id=PROJECT_ID,
        region=REGION,
        hyperparameter_tuning_job_id="{{ task_instance.xcom_pull("
        "task_ids='create_hyperparameter_tuning_job', key='hyperparameter_tuning_job_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_hyperparameter_tuning_job_operator]

    delete_hyperparameter_tuning_job_def = DeleteHyperparameterTuningJobOperator(
        task_id="delete_hyperparameter_tuning_job_def",
        project_id=PROJECT_ID,
        region=REGION,
        hyperparameter_tuning_job_id="{{ task_instance.xcom_pull("
        "task_ids='create_hyperparameter_tuning_job_def', "
        "key='hyperparameter_tuning_job_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START how_to_cloud_vertex_ai_list_hyperparameter_tuning_job_operator]
    list_hyperparameter_tuning_job = ListHyperparameterTuningJobOperator(
        task_id="list_hyperparameter_tuning_job",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_hyperparameter_tuning_job_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> [create_hyperparameter_tuning_job, create_hyperparameter_tuning_job_def]
        >> get_hyperparameter_tuning_job
        >> [delete_hyperparameter_tuning_job, delete_hyperparameter_tuning_job_def]
        >> list_hyperparameter_tuning_job
        # TEST TEARDOWN
        >> delete_bucket
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
