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

# mypy ignore arg types (for templated fields)
# type: ignore[arg-type]

"""
Example Airflow DAG for Google Vertex AI service testing Endpoint Service operations.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from google.cloud import aiplatform
from google.cloud.aiplatform import schema
from google.protobuf.struct_pb2 import Value

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLImageTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
    ImportDataOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.endpoint_service import (
    CreateEndpointOperator,
    DeleteEndpointOperator,
    DeployModelOperator,
    ListEndpointsOperator,
    UndeployModelOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_endpoint_service_operations"
REGION = "us-central1"
IMAGE_DISPLAY_NAME = f"auto-ml-image-{ENV_ID}"
MODEL_DISPLAY_NAME = f"auto-ml-image-model-{ENV_ID}"

DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATA_SAMPLE_GCS_OBJECT_NAME = "vertex-ai/image-dataset.csv"
IMAGE_ZIP_CSV_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / "image-dataset.csv.zip")
IMAGE_CSV_FILE_LOCAL_PATH = "/endpoint/image-dataset.csv"

IMAGE_DATASET = {
    "display_name": f"image-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.image,
    "metadata": Value(string_value="image-dataset"),
}
IMAGE_DATA_CONFIG = [
    {
        "import_schema_uri": schema.dataset.ioformat.image.single_label_classification,
        "gcs_source": {"uris": [f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}/vertex-ai/image-dataset.csv"]},
    },
]

ENDPOINT_CONF = {
    "display_name": f"endpoint_test_{ENV_ID}",
}


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["example", "vertex_ai", "endpoint_service"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )
    unzip_file = BashOperator(
        task_id="unzip_csv_data_file",
        bash_command=f"unzip {IMAGE_ZIP_CSV_FILE_LOCAL_PATH} -d /endpoint/",
    )
    upload_files = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=IMAGE_CSV_FILE_LOCAL_PATH,
        dst=DATA_SAMPLE_GCS_OBJECT_NAME,
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
    )
    create_image_dataset = CreateDatasetOperator(
        task_id="image_dataset",
        dataset=IMAGE_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    image_dataset_id = create_image_dataset.output["dataset_id"]

    import_image_dataset = ImportDataOperator(
        task_id="import_image_data",
        dataset_id=image_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        import_configs=IMAGE_DATA_CONFIG,
    )

    create_auto_ml_image_training_job = CreateAutoMLImageTrainingJobOperator(
        task_id="auto_ml_image_task",
        display_name=IMAGE_DISPLAY_NAME,
        dataset_id=image_dataset_id,
        prediction_type="classification",
        multi_label=False,
        model_type="CLOUD",
        training_fraction_split=0.6,
        validation_fraction_split=0.2,
        test_fraction_split=0.2,
        budget_milli_node_hours=8000,
        model_display_name=MODEL_DISPLAY_NAME,
        disable_early_stopping=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    DEPLOYED_MODEL = {
        # format: 'projects/{project}/locations/{location}/models/{model}'
        "model": "{{ti.xcom_pull('auto_ml_image_task')['name']}}",
        "display_name": f"temp_endpoint_test_{ENV_ID}",
        "dedicated_resources": {
            "machine_spec": {
                "machine_type": "n1-standard-2",
                "accelerator_type": aiplatform.gapic.AcceleratorType.NVIDIA_TESLA_K80,
                "accelerator_count": 1,
            },
            "min_replica_count": 1,
            "max_replica_count": 1,
        },
    }

    # [START how_to_cloud_vertex_ai_create_endpoint_operator]
    create_endpoint = CreateEndpointOperator(
        task_id="create_endpoint",
        endpoint=ENDPOINT_CONF,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_endpoint_operator]

    # [START how_to_cloud_vertex_ai_delete_endpoint_operator]
    delete_endpoint = DeleteEndpointOperator(
        task_id="delete_endpoint",
        endpoint_id=create_endpoint.output["endpoint_id"],
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_endpoint_operator]

    # [START how_to_cloud_vertex_ai_list_endpoints_operator]
    list_endpoints = ListEndpointsOperator(
        task_id="list_endpoints",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_endpoints_operator]

    # [START how_to_cloud_vertex_ai_deploy_model_operator]
    deploy_model = DeployModelOperator(
        task_id="deploy_model",
        endpoint_id=create_endpoint.output["endpoint_id"],
        deployed_model=DEPLOYED_MODEL,
        traffic_split={"0": 100},
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_deploy_model_operator]

    # [START how_to_cloud_vertex_ai_undeploy_model_operator]
    undeploy_model = UndeployModelOperator(
        task_id="undeploy_model",
        endpoint_id=create_endpoint.output["endpoint_id"],
        deployed_model_id=deploy_model.output["deployed_model_id"],
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_undeploy_model_operator]

    delete_auto_ml_image_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_training_job",
        training_pipeline_id=create_auto_ml_image_training_job.output["training_id"],
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_image_dataset = DeleteDatasetOperator(
        task_id="delete_image_dataset",
        dataset_id=image_dataset_id,
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    clear_folder = BashOperator(
        task_id="clear_folder",
        bash_command="rm -r /endpoint/*",
    )

    (
        # TEST SETUP
        [
            create_bucket,
            create_image_dataset,
        ]
        >> unzip_file
        >> upload_files
        >> import_image_dataset
        >> create_auto_ml_image_training_job
        # TEST BODY
        >> create_endpoint
        >> deploy_model
        >> undeploy_model
        >> delete_endpoint
        >> list_endpoints
        # TEST TEARDOWN
        >> delete_auto_ml_image_training_job
        >> delete_image_dataset
        >> delete_bucket
        >> clear_folder
    )


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
