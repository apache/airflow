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
Example Airflow DAG for Google Vertex AI service testing Dataset operations.
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
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ExportDataOperator,
    GetDatasetOperator,
    ImportDataOperator,
    ListDatasetsOperator,
    UpdateDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_dataset_operations"
REGION = "us-central1"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")

TIME_SERIES_DATASET = {
    "display_name": f"time-series-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.time_series,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {
                    "uri": [
                        f"gs://{RESOURCE_DATA_BUCKET}/vertex-ai/datasets/forecast-dataset.csv"
                    ]
                }
            }
        },
        Value(),
    ),
}
IMAGE_DATASET = {
    "display_name": f"image-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.image,
    "metadata": Value(string_value="image-dataset"),
}
TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {
            "input_config": {
                "gcs_source": {
                    "uri": [
                        f"gs://{RESOURCE_DATA_BUCKET}/vertex-ai/datasets/tabular-dataset.csv"
                    ]
                }
            }
        },
        Value(),
    ),
}
TEXT_DATASET = {
    "display_name": f"text-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.text,
    "metadata": Value(string_value="text-dataset"),
}
VIDEO_DATASET = {
    "display_name": f"video-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.video,
    "metadata": Value(string_value="video-dataset"),
}
TEST_EXPORT_CONFIG = {
    "gcs_destination": {
        "output_uri_prefix": f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/exports"
    }
}
TEST_IMPORT_CONFIG = [
    {
        "data_item_labels": {
            "test-labels-name": "test-labels-value",
        },
        "import_schema_uri": schema.dataset.ioformat.image.single_label_classification,
        "gcs_source": {
            "uris": [
                f"gs://{RESOURCE_DATA_BUCKET}/vertex-ai/datasets/image-dataset-flowers.csv"
            ]
        },
    },
]
DATASET_TO_UPDATE = {"display_name": "test-name"}
TEST_UPDATE_MASK = {"paths": ["displayName"]}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "vertex_ai", "dataset"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )

    # [START how_to_cloud_vertex_ai_create_dataset_operator]
    create_image_dataset_job = CreateDatasetOperator(
        task_id="image_dataset",
        dataset=IMAGE_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_tabular_dataset_job = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_text_dataset_job = CreateDatasetOperator(
        task_id="text_dataset",
        dataset=TEXT_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_video_dataset_job = CreateDatasetOperator(
        task_id="video_dataset",
        dataset=VIDEO_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    create_time_series_dataset_job = CreateDatasetOperator(
        task_id="time_series_dataset",
        dataset=TIME_SERIES_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_dataset_operator]

    # [START how_to_cloud_vertex_ai_delete_dataset_operator]
    delete_dataset_job = DeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=create_text_dataset_job.output["dataset_id"],
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_dataset_operator]

    # [START how_to_cloud_vertex_ai_get_dataset_operator]
    get_dataset = GetDatasetOperator(
        task_id="get_dataset",
        project_id=PROJECT_ID,
        region=REGION,
        dataset_id=create_tabular_dataset_job.output["dataset_id"],
    )
    # [END how_to_cloud_vertex_ai_get_dataset_operator]

    # [START how_to_cloud_vertex_ai_export_data_operator]
    export_data_job = ExportDataOperator(
        task_id="export_data",
        dataset_id=create_image_dataset_job.output["dataset_id"],
        region=REGION,
        project_id=PROJECT_ID,
        export_config=TEST_EXPORT_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_export_data_operator]

    # [START how_to_cloud_vertex_ai_import_data_operator]
    import_data_job = ImportDataOperator(
        task_id="import_data",
        dataset_id=create_image_dataset_job.output["dataset_id"],
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=TEST_IMPORT_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_import_data_operator]

    # [START how_to_cloud_vertex_ai_list_dataset_operator]
    list_dataset_job = ListDatasetsOperator(
        task_id="list_dataset",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_dataset_operator]

    # [START how_to_cloud_vertex_ai_update_dataset_operator]
    update_dataset_job = UpdateDatasetOperator(
        task_id="update_dataset",
        project_id=PROJECT_ID,
        region=REGION,
        dataset_id=create_video_dataset_job.output["dataset_id"],
        dataset=DATASET_TO_UPDATE,
        update_mask=TEST_UPDATE_MASK,
    )
    # [END how_to_cloud_vertex_ai_update_dataset_operator]

    delete_time_series_dataset_job = DeleteDatasetOperator(
        task_id="delete_time_series_dataset",
        dataset_id=create_time_series_dataset_job.output["dataset_id"],
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_tabular_dataset_job = DeleteDatasetOperator(
        task_id="delete_tabular_dataset",
        dataset_id=create_tabular_dataset_job.output["dataset_id"],
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_image_dataset_job = DeleteDatasetOperator(
        task_id="delete_image_dataset",
        dataset_id=create_image_dataset_job.output["dataset_id"],
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_video_dataset_job = DeleteDatasetOperator(
        task_id="delete_video_dataset",
        dataset_id=create_video_dataset_job.output["dataset_id"],
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> [
            create_time_series_dataset_job >> delete_time_series_dataset_job,
            create_text_dataset_job >> delete_dataset_job,
            create_tabular_dataset_job >> get_dataset >> delete_tabular_dataset_job,
            create_image_dataset_job
            >> import_data_job
            >> export_data_job
            >> delete_image_dataset_job,
            create_video_dataset_job >> update_dataset_job >> delete_video_dataset_job,
            list_dataset_job,
        ]
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
