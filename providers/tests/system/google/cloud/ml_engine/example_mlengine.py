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
Example Airflow DAG for Google ML Engine service.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.cloud.aiplatform import schema
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import (
    CreateBatchPredictionJobOperator,
    DeleteBatchPredictionJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomPythonPackageTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.dataset import (
    CreateDatasetOperator,
    DeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.model_service import (
    DeleteModelOperator,
    DeleteModelVersionOperator,
    GetModelOperator,
    ListModelVersionsOperator,
    SetDefaultVersionOnModelOperator,
)
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "gcp_mlengine"
REGION = "us-central1"

PACKAGE_DISPLAY_NAME = f"package-{DAG_ID}-{ENV_ID}".replace("_", "-")
MODEL_DISPLAY_NAME = f"model-{DAG_ID}-{ENV_ID}".replace("_", "-")
JOB_DISPLAY_NAME = f"batch_job_{DAG_ID}_{ENV_ID}".replace("-", "_")

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
CUSTOM_PYTHON_GCS_BUCKET_NAME = f"bucket_python_{DAG_ID}_{ENV_ID}".replace("_", "-")

BQ_SOURCE = "bq://bigquery-public-data.ml_datasets.penguins"
TABULAR_DATASET = {
    "display_name": f"tabular-dataset-{ENV_ID}",
    "metadata_schema_uri": schema.dataset.metadata.tabular,
    "metadata": ParseDict(
        {"input_config": {"bigquery_source": {"uri": BQ_SOURCE}}},
        Value(),
    ),
}

REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
TRAINING_FRACTION_SPLIT = 0.7
TEST_FRACTION_SPLIT = 0.15
VALIDATION_FRACTION_SPLIT = 0.15

PYTHON_PACKAGE_GCS_URI = f"gs://{CUSTOM_PYTHON_GCS_BUCKET_NAME}/vertex-ai/penguins_trainer_script-0.1.zip"
PYTHON_MODULE_NAME = "penguins_trainer_script.task"

TRAIN_IMAGE = "us-docker.pkg.dev/vertex-ai/training/tf-cpu.2-8:latest"
DEPLOY_IMAGE = "us-docker.pkg.dev/vertex-ai/prediction/tf2-cpu.2-8:latest"


with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "ml_engine"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=CUSTOM_PYTHON_GCS_BUCKET_NAME,
        storage_class="REGIONAL",
        location=REGION,
    )

    move_data_files = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="vertex-ai/penguins-data",
        destination_bucket=CUSTOM_PYTHON_GCS_BUCKET_NAME,
        destination_object="vertex-ai",
        recursive=True,
    )
    create_tabular_dataset = CreateDatasetOperator(
        task_id="tabular_dataset",
        dataset=TABULAR_DATASET,
        region=REGION,
        project_id=PROJECT_ID,
    )
    tabular_dataset_id = create_tabular_dataset.output["dataset_id"]

    # [START howto_operator_create_custom_python_training_job_v1]
    create_custom_python_package_training_job = CreateCustomPythonPackageTrainingJobOperator(
        task_id="create_custom_python_package_training_job",
        staging_bucket=f"gs://{CUSTOM_PYTHON_GCS_BUCKET_NAME}",
        display_name=PACKAGE_DISPLAY_NAME,
        python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
        python_module_name=PYTHON_MODULE_NAME,
        container_uri=TRAIN_IMAGE,
        model_serving_container_image_uri=DEPLOY_IMAGE,
        bigquery_destination=f"bq://{PROJECT_ID}",
        # run params
        dataset_id=tabular_dataset_id,
        model_display_name=MODEL_DISPLAY_NAME,
        replica_count=REPLICA_COUNT,
        machine_type=MACHINE_TYPE,
        accelerator_type=ACCELERATOR_TYPE,
        accelerator_count=ACCELERATOR_COUNT,
        training_fraction_split=TRAINING_FRACTION_SPLIT,
        validation_fraction_split=VALIDATION_FRACTION_SPLIT,
        test_fraction_split=TEST_FRACTION_SPLIT,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_create_custom_python_training_job_v1]
    model_id_v1 = create_custom_python_package_training_job.output["model_id"]

    # [START howto_operator_gcp_mlengine_get_model]
    get_model = GetModelOperator(
        task_id="get_model", region=REGION, project_id=PROJECT_ID, model_id=model_id_v1
    )
    # [END howto_operator_gcp_mlengine_get_model]

    # [START howto_operator_gcp_mlengine_print_model]
    get_model_result = BashOperator(
        bash_command=f"echo {get_model.output}",
        task_id="get_model_result",
    )
    # [END howto_operator_gcp_mlengine_print_model]

    # [START howto_operator_create_custom_python_training_job_v2]
    create_custom_python_package_training_job_v2 = CreateCustomPythonPackageTrainingJobOperator(
        task_id="create_custom_python_package_training_job_v2",
        staging_bucket=f"gs://{CUSTOM_PYTHON_GCS_BUCKET_NAME}",
        display_name=PACKAGE_DISPLAY_NAME,
        python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
        python_module_name=PYTHON_MODULE_NAME,
        container_uri=TRAIN_IMAGE,
        model_serving_container_image_uri=DEPLOY_IMAGE,
        bigquery_destination=f"bq://{PROJECT_ID}",
        parent_model=model_id_v1,
        # run params
        dataset_id=tabular_dataset_id,
        model_display_name=MODEL_DISPLAY_NAME,
        replica_count=REPLICA_COUNT,
        machine_type=MACHINE_TYPE,
        accelerator_type=ACCELERATOR_TYPE,
        accelerator_count=ACCELERATOR_COUNT,
        training_fraction_split=TRAINING_FRACTION_SPLIT,
        validation_fraction_split=VALIDATION_FRACTION_SPLIT,
        test_fraction_split=TEST_FRACTION_SPLIT,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_create_custom_python_training_job_v2]
    model_id_v2 = create_custom_python_package_training_job_v2.output["model_id"]

    # [START howto_operator_gcp_mlengine_default_version]
    set_default_version = SetDefaultVersionOnModelOperator(
        task_id="set_default_version",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=model_id_v2,
    )
    # [END howto_operator_gcp_mlengine_default_version]

    # [START howto_operator_gcp_mlengine_list_versions]
    list_model_versions = ListModelVersionsOperator(
        task_id="list_model_versions", region=REGION, project_id=PROJECT_ID, model_id=model_id_v2
    )
    # [END howto_operator_gcp_mlengine_list_versions]

    # [START howto_operator_start_batch_prediction]
    create_batch_prediction_job = CreateBatchPredictionJobOperator(
        task_id="create_batch_prediction_job",
        job_display_name=JOB_DISPLAY_NAME,
        model_name=model_id_v2,
        predictions_format="bigquery",
        bigquery_source=BQ_SOURCE,
        bigquery_destination_prefix=f"bq://{PROJECT_ID}",
        region=REGION,
        project_id=PROJECT_ID,
        machine_type=MACHINE_TYPE,
    )
    # [END howto_operator_start_batch_prediction]

    # [START howto_operator_gcp_mlengine_delete_version]
    delete_model_version_1 = DeleteModelVersionOperator(
        task_id="delete_model_version_1",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=model_id_v1,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gcp_mlengine_delete_version]

    # [START howto_operator_gcp_mlengine_delete_model]
    delete_model = DeleteModelOperator(
        task_id="delete_model",
        project_id=PROJECT_ID,
        region=REGION,
        model_id=model_id_v2,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gcp_mlengine_delete_model]

    delete_batch_prediction_job = DeleteBatchPredictionJobOperator(
        task_id="delete_batch_prediction_job",
        batch_prediction_job_id=create_batch_prediction_job.output["batch_prediction_job_id"],
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
        bucket_name=CUSTOM_PYTHON_GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        >> move_data_files
        >> create_tabular_dataset
        # TEST BODY
        >> create_custom_python_package_training_job
        >> create_custom_python_package_training_job_v2
        >> create_batch_prediction_job
        >> get_model
        >> get_model_result
        >> list_model_versions
        >> set_default_version
        # TEST TEARDOWN
        >> delete_model_version_1
        >> delete_model
        >> delete_batch_prediction_job
        >> delete_tabular_dataset
        >> delete_bucket
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
