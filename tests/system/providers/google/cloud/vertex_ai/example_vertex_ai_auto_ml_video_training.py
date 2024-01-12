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
Example Airflow DAG for Google Vertex AI service testing Auto ML operations.
"""
from __future__ import annotations

import os
from datetime import datetime

from google.cloud.aiplatform import schema
from google.protobuf.struct_pb2 import Value

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLVideoTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ImportDataOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "example_vertex_ai_auto_ml_operations"
REGION = "us-central1"
VIDEO_DISPLAY_NAME = f"auto-ml-video-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-video-model-{ENV_ID}"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
VIDEO_GCS_BUCKET_NAME = f"bucket_video_{DAG_ID}_{ENV_ID}".replace("_", "-")

VIDEO_DATASET = {
    "display_name": f"video-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.video,
    "metadata": Value(string_value="video-dataset"),
}
VIDEO_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.video.classification,
        "gcs_source": {"uris": [f"gs://{VIDEO_GCS_BUCKET_NAME}/vertex-ai/video-dataset.csv"]},
    },
]

with DAG(
    f"{DAG_ID}_video_training_job",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "auto_ml"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=VIDEO_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )

    move_dataset_file = GCSSynchronizeBucketsOperator(
        task_id="move_dataset_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/datasets",
        destination_bucket=VIDEO_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )

    create_video_dataset = CreateDatasetOperator(
        task_id="video_dataset",
        dataset=VIDEO_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    video_dataset_id = create_video_dataset.output["dataset_id"]

    import_video_dataset = ImportDataOperator(
        task_id="import_video_data",
        dataset_id=video_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=VIDEO_DATA_CONFIG,
    )

    # [START how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]
    create_auto_ml_video_training_job = CreateAutoMLVideoTrainingJobOperator(
        task_id="auto_ml_video_task",
        display_name=VIDEO_DISPLAY_NAME,
        prediction_type="classification",
        model_type="CLOUD",
        dataset_id=video_dataset_id,
        model_display_name=MODEL_DISPLAY_NAME,
        region=REGION,
        project_id=PROJECT_ID,
    )
    model_id_v1 = create_auto_ml_video_training_job.output["model_id"]
    # [END how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_auto_ml_video_training_job_v2_operator]
    create_auto_ml_video_training_job_v2 = CreateAutoMLVideoTrainingJobOperator(
        task_id="auto_ml_video_v2_task",
        display_name=VIDEO_DISPLAY_NAME,
        prediction_type="classification",
        model_type="CLOUD",
        dataset_id=video_dataset_id,
        model_display_name=MODEL_DISPLAY_NAME,
        parent_model=model_id_v1,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_video_training_job_v2_operator]

    delete_auto_ml_video_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_video_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='auto_ml_video_task', "
        "key='training_id') }}",
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_video_dataset = DeleteDatasetOperator(
        task_id="delete_video_dataset",
        dataset_id=video_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=VIDEO_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        [
            create_bucket >> move_dataset_file,
            create_video_dataset,
        ]
        >> import_video_dataset
        # TEST BODY
        >> create_auto_ml_video_training_job
        >> create_auto_ml_video_training_job_v2
        # TEST TEARDOWN
        >> delete_auto_ml_video_training_job
        >> delete_video_dataset
        >> delete_bucket
    )


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
