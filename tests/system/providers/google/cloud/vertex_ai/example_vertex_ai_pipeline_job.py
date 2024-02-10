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
Example Airflow DAG for Google Vertex AI service testing Pipeline Job operations.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSListObjectsOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    DeletePipelineJobOperator,
    GetPipelineJobOperator,
    ListPipelineJobOperator,
    RunPipelineJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "example_vertex_ai_pipeline_job_operations"
REGION = "us-central1"
DISPLAY_NAME = f"pipeline-job-{ENV_ID}"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
TEMPLATE_PATH = "https://us-kfp.pkg.dev/ml-pipeline/google-cloud-registry/automl-tabular/sha256:85e4218fc6604ee82353c9d2ebba20289eb1b71930798c0bb8ce32d8a10de146"
OUTPUT_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}"

PARAMETER_VALUES = {
    "train_budget_milli_node_hours": 2000,
    "optimization_objective": "minimize-log-loss",
    "project": PROJECT_ID,
    "location": REGION,
    "root_dir": OUTPUT_BUCKET,
    "target_column": "Adopted",
    "training_fraction": 0.8,
    "validation_fraction": 0.1,
    "test_fraction": 0.1,
    "prediction_type": "classification",
    "data_source_csv_filenames": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/tabular-dataset.csv",
    "transformations": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/column_transformations.json",
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "pipeline_job"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )

    move_pipeline_files = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/pipeline",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )

    # [START how_to_cloud_vertex_ai_run_pipeline_job_operator]
    run_pipeline_job = RunPipelineJobOperator(
        task_id="run_pipeline_job",
        display_name=DISPLAY_NAME,
        template_path=TEMPLATE_PATH,
        parameter_values=PARAMETER_VALUES,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_run_pipeline_job_operator]

    # [START how_to_cloud_vertex_ai_get_pipeline_job_operator]
    get_pipeline_job = GetPipelineJobOperator(
        task_id="get_pipeline_job",
        project_id=PROJECT_ID,
        region=REGION,
        pipeline_job_id="{{ task_instance.xcom_pull("
        "task_ids='run_pipeline_job', key='pipeline_job_id') }}",
    )
    # [END how_to_cloud_vertex_ai_get_pipeline_job_operator]

    # [START how_to_cloud_vertex_ai_delete_pipeline_job_operator]
    delete_pipeline_job = DeletePipelineJobOperator(
        task_id="delete_pipeline_job",
        project_id=PROJECT_ID,
        region=REGION,
        pipeline_job_id="{{ task_instance.xcom_pull("
        "task_ids='run_pipeline_job', key='pipeline_job_id') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_pipeline_job_operator]

    # [START how_to_cloud_vertex_ai_list_pipeline_job_operator]
    list_pipeline_job = ListPipelineJobOperator(
        task_id="list_pipeline_job",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_pipeline_job_operator]

    list_buckets = GCSListObjectsOperator(task_id="list_buckets", bucket=DATA_SAMPLE_GCS_BUCKET_NAME)

    delete_files = GCSDeleteObjectsOperator(
        task_id="delete_files", bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME, objects=list_buckets.output
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        >> move_pipeline_files
        # TEST BODY
        >> run_pipeline_job
        >> get_pipeline_job
        >> delete_pipeline_job
        >> list_pipeline_job
        # TEST TEARDOWN
        >> list_buckets
        >> delete_files
        >> delete_bucket
    )


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
