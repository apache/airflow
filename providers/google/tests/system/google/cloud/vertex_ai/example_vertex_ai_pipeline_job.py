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

from google.cloud.aiplatform import schema
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSListObjectsOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import CreateDatasetOperator
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    DeletePipelineJobOperator,
    GetPipelineJobOperator,
    ListPipelineJobOperator,
    RunPipelineJobOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_pipeline_job_operations"
REGION = "us-central1"
DISPLAY_NAME = f"pipeline-job-{ENV_ID}"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
TEMPLATE_PATH = "https://us-kfp.pkg.dev/ml-pipeline/google-cloud-registry/get-vertex-dataset/sha256:f4eb4a2b0aab482c487c1cd62b3c735baaf914be8fa8c4687c06077c1d815a5d"
OUTPUT_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}"

PARAMETER_VALUES = {
    "dataset_resource_name": f"projects/{PROJECT_ID}/locations/{REGION}/datasets/tabular-dataset-{ENV_ID}",
}

DATA_SAMPLE_GCS_OBJECT_NAME = "vertex-ai/california_housing_train.csv"
TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {"uri": [f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/{DATA_SAMPLE_GCS_OBJECT_NAME}"]}
            }
        },
        Value(),
    ),
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

    move_dataset_files = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/california-housing-data",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )

    create_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
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
        pipeline_job_id="{{ task_instance.xcom_pull(task_ids='run_pipeline_job', key='pipeline_job_id') }}",
    )
    # [END how_to_cloud_vertex_ai_get_pipeline_job_operator]

    # [START how_to_cloud_vertex_ai_delete_pipeline_job_operator]
    delete_pipeline_job = DeletePipelineJobOperator(
        task_id="delete_pipeline_job",
        project_id=PROJECT_ID,
        region=REGION,
        pipeline_job_id="{{ task_instance.xcom_pull(task_ids='run_pipeline_job', key='pipeline_job_id') }}",
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
        >> move_dataset_files
        >> create_dataset
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

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
