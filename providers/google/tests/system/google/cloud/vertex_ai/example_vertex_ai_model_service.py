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
Example Airflow DAG for Google Vertex AI service testing Model Service operations.
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
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomTrainingJobOperator,
    DeleteCustomTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.model_service import (
    AddVersionAliasesOnModelOperator,
    DeleteModelOperator,
    DeleteModelVersionOperator,
    DeleteVersionAliasesOnModelOperator,
    ExportModelOperator,
    GetModelOperator,
    ListModelsOperator,
    ListModelVersionsOperator,
    SetDefaultVersionOnModelOperator,
    UploadModelOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "vertex_ai_model_service_operations"
REGION = "us-central1"
TRAIN_DISPLAY_NAME = f"train-housing-custom-{ENV_ID}"
MODEL_DISPLAY_NAME = f"custom-housing-model-{ENV_ID}"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
DATA_SAMPLE_GCS_BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
STAGING_BUCKET = f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}"

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

CONTAINER_URI = "us-docker.pkg.dev/vertex-ai/training/tf-cpu.2-2:latest"

# LOCAL_TRAINING_SCRIPT_PATH should be set for Airflow which is running on distributed system.
# For example in Composer the correct path is `gcs/data/california_housing_training_script.py`.
# Because `gcs/data/` is shared folder for Airflow's workers.
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))
LOCAL_TRAINING_SCRIPT_PATH = (
    "gcs/data/california_housing_training_script.py"
    if IS_COMPOSER
    else "california_housing_training_script.py"
)

MODEL_OUTPUT_CONFIG = {
    "artifact_destination": {
        "output_uri_prefix": STAGING_BUCKET,
    },
    "export_format_id": "custom-trained",
}
MODEL_SERVING_CONTAINER_URI = "us-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-2:latest"
MODEL_OBJ = {
    "display_name": f"model-{ENV_ID}",
    "artifact_uri": "{{ti.xcom_pull('custom_task')['artifactUri']}}",
    "container_spec": {
        "image_uri": MODEL_SERVING_CONTAINER_URI,
        "command": [],
        "args": [],
        "env": [],
        "ports": [],
        "predict_route": "",
        "health_route": "",
    },
}
MODEL_OBJ_V2 = {
    "display_name": f"model-{ENV_ID}-v2",
    "artifact_uri": "{{ti.xcom_pull('custom_task')['artifactUri']}}",
    "container_spec": {
        "image_uri": MODEL_SERVING_CONTAINER_URI,
        "command": [],
        "args": [],
        "env": [],
        "ports": [],
        "predict_route": "",
        "health_route": "",
    },
}


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["example", "vertex_ai", "model_service"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=DATA_SAMPLE_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )

    move_data_files = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/california-housing-data",
        destination_bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )

    download_training_script_file = GCSToLocalFilesystemOperator(
        task_id="download_training_script_file",
        object_name="vertex-ai/california_housing_training_script.py",
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        filename=LOCAL_TRAINING_SCRIPT_PATH,
    )

    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output["dataset_id"]

    create_custom_training_job = CreateCustomTrainingJobOperator(
        task_id="custom_task",
        staging_bucket=f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}",
        display_name=TRAIN_DISPLAY_NAME,
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=tabular_dataset_id,
        replica_count=1,
        model_display_name=MODEL_DISPLAY_NAME,
        region=REGION,
        project_id=PROJECT_ID,
    )
    model_id_v1 = create_custom_training_job.output["model_id"]

    create_custom_training_job_v2 = CreateCustomTrainingJobOperator(
        task_id="custom_task_v2",
        staging_bucket=f"gs://{DATA_SAMPLE_GCS_BUCKET_NAME}",
        display_name=TRAIN_DISPLAY_NAME,
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        parent_model=model_id_v1,
        # run params
        dataset_id=tabular_dataset_id,
        replica_count=1,
        model_display_name=MODEL_DISPLAY_NAME,
        region=REGION,
        project_id=PROJECT_ID,
    )
    model_id_v2 = create_custom_training_job_v2.output["model_id"]

    # [START how_to_cloud_vertex_ai_get_model_operator]
    get_model = GetModelOperator(
        task_id="get_model", region=REGION, project_id=PROJECT_ID, model_id=model_id_v1
    )
    # [END how_to_cloud_vertex_ai_get_model_operator]

    # [START how_to_cloud_vertex_ai_list_model_versions_operator]
    list_model_versions = ListModelVersionsOperator(
        task_id="list_model_versions", region=REGION, project_id=PROJECT_ID, model_id=model_id_v1
    )
    # [END how_to_cloud_vertex_ai_list_model_versions_operator]

    # [START how_to_cloud_vertex_ai_set_version_as_default_operator]
    set_default_version = SetDefaultVersionOnModelOperator(
        task_id="set_default_version",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=model_id_v2,
    )
    # [END how_to_cloud_vertex_ai_set_version_as_default_operator]

    # [START how_to_cloud_vertex_ai_add_version_aliases_operator]
    add_version_alias = AddVersionAliasesOnModelOperator(
        task_id="add_version_alias",
        project_id=PROJECT_ID,
        region=REGION,
        version_aliases=["new-version", "beta"],
        model_id=model_id_v2,
    )
    # [END how_to_cloud_vertex_ai_add_version_aliases_operator]

    # [START how_to_cloud_vertex_ai_upload_model_operator]
    upload_model = UploadModelOperator(
        task_id="upload_model",
        region=REGION,
        project_id=PROJECT_ID,
        model=MODEL_OBJ,
    )
    upload_model_v1 = upload_model.output["model_id"]
    # [END how_to_cloud_vertex_ai_upload_model_operator]
    upload_model_with_parent_model = UploadModelOperator(
        task_id="upload_model_with_parent_model",
        region=REGION,
        project_id=PROJECT_ID,
        model=MODEL_OBJ_V2,
        parent_model=upload_model_v1,
    )

    # [START how_to_cloud_vertex_ai_export_model_operator]
    export_model = ExportModelOperator(
        task_id="export_model",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=upload_model.output["model_id"],
        output_config=MODEL_OUTPUT_CONFIG,
    )
    # [END how_to_cloud_vertex_ai_export_model_operator]

    # [START how_to_cloud_vertex_ai_delete_model_operator]
    delete_model = DeleteModelOperator(
        task_id="delete_model",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=upload_model.output["model_id"],
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_model_operator]
    delete_model_with_parent_model = DeleteModelOperator(
        task_id="delete_model_with_parent_model",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=upload_model_with_parent_model.output["model_id"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START how_to_cloud_vertex_ai_list_models_operator]
    list_models = ListModelsOperator(
        task_id="list_models",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_models_operator]

    # [START how_to_cloud_vertex_ai_delete_version_aliases_operator]
    delete_version_alias = DeleteVersionAliasesOnModelOperator(
        task_id="delete_version_alias",
        project_id=PROJECT_ID,
        region=REGION,
        version_aliases=["new-version"],
        model_id=model_id_v2,
    )
    # [END how_to_cloud_vertex_ai_delete_version_aliases_operator]

    # [START how_to_cloud_vertex_ai_delete_version_operator]
    delete_model_version = DeleteModelVersionOperator(
        task_id="delete_model_version",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=model_id_v1,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END how_to_cloud_vertex_ai_delete_version_operator]

    delete_custom_training_job = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job",
        training_pipeline_id="{{ task_instance.xcom_pull(task_ids='custom_task', key='training_id') }}",
        custom_job_id="{{ task_instance.xcom_pull(task_ids='custom_task', key='custom_job_id') }}",
        region=REGION,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_tabular_dataset = DeleteDatasetOperator(
        task_id="delete_tabular_dataset",
        dataset_id=tabular_dataset_id,
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
        >> move_data_files
        >> download_training_script_file
        >> create_tabular_dataset
        >> create_custom_training_job
        >> create_custom_training_job_v2
        # TEST BODY
        >> get_model
        >> list_model_versions
        >> set_default_version
        >> add_version_alias
        >> upload_model
        >> upload_model_with_parent_model
        >> export_model
        >> delete_model
        >> delete_model_with_parent_model
        >> list_models
        # TEST TEARDOWN
        >> delete_version_alias
        >> delete_model_version
        >> delete_custom_training_job
        >> delete_tabular_dataset
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
