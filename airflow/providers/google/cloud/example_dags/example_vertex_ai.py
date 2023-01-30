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
Example Airflow DAG that demonstrates operators for the Google Vertex AI service in the Google
Cloud Platform.

This DAG relies on the following OS environment variables:

* GCP_VERTEX_AI_BUCKET - Google Cloud Storage bucket where the model will be saved
  after training process was finished.
* CUSTOM_CONTAINER_URI - path to container with model.
* PYTHON_PACKAGE_GSC_URI - path to test model in archive.
* LOCAL_TRAINING_SCRIPT_PATH - path to local training script.
* DATASET_ID - ID of dataset which will be used in training process.
* MODEL_ID - ID of model which will be used in predict process.
* MODEL_ARTIFACT_URI - The artifact_uri should be the path to a GCS directory containing saved model
  artifacts.
"""
from __future__ import annotations

import os
from datetime import datetime
from uuid import uuid4

from google.cloud import aiplatform
from google.protobuf.struct_pb2 import Value

from airflow import models
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import (
    CreateAutoMLForecastingTrainingJobOperator,
    CreateAutoMLImageTrainingJobOperator,
    CreateAutoMLTabularTrainingJobOperator,
    CreateAutoMLTextTrainingJobOperator,
    CreateAutoMLVideoTrainingJobOperator,
    DeleteAutoMLTrainingJobOperator,
    ListAutoMLTrainingJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import (
    CreateBatchPredictionJobOperator,
    DeleteBatchPredictionJobOperator,
    ListBatchPredictionJobsOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.custom_job import (
    CreateCustomContainerTrainingJobOperator,
    CreateCustomPythonPackageTrainingJobOperator,
    CreateCustomTrainingJobOperator,
    DeleteCustomTrainingJobOperator,
    ListCustomTrainingJobOperator,
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
from airflow.providers.google.cloud.operators.vertex_ai.endpoint_service import (
    CreateEndpointOperator,
    DeleteEndpointOperator,
    DeployModelOperator,
    ListEndpointsOperator,
    UndeployModelOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.hyperparameter_tuning_job import (
    CreateHyperparameterTuningJobOperator,
    DeleteHyperparameterTuningJobOperator,
    GetHyperparameterTuningJobOperator,
    ListHyperparameterTuningJobOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.model_service import (
    DeleteModelOperator,
    ExportModelOperator,
    ListModelsOperator,
    UploadModelOperator,
)

# mypy ignore arg types (for templated fields)
# type: ignore[arg-type]


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "an-id")
REGION = os.environ.get("GCP_LOCATION", "us-central1")
BUCKET = os.environ.get("GCP_VERTEX_AI_BUCKET", "vertex-ai-system-tests")

STAGING_BUCKET = f"gs://{BUCKET}"
DISPLAY_NAME = str(uuid4())  # Create random display name
CONTAINER_URI = "gcr.io/cloud-aiplatform/training/tf-cpu.2-2:latest"
CUSTOM_CONTAINER_URI = os.environ.get("CUSTOM_CONTAINER_URI", "path_to_container_with_model")
MODEL_SERVING_CONTAINER_URI = "gcr.io/cloud-aiplatform/prediction/tf2-cpu.2-2:latest"
REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
TRAINING_FRACTION_SPLIT = 0.7
TEST_FRACTION_SPLIT = 0.15
VALIDATION_FRACTION_SPLIT = 0.15

PYTHON_PACKAGE_GCS_URI = os.environ.get("PYTHON_PACKAGE_GSC_URI", "path_to_test_model_in_arch")
PYTHON_MODULE_NAME = "aiplatform_custom_trainer_script.task"

LOCAL_TRAINING_SCRIPT_PATH = os.environ.get("LOCAL_TRAINING_SCRIPT_PATH", "path_to_training_script")

TRAINING_PIPELINE_ID = "test-training-pipeline-id"
CUSTOM_JOB_ID = "test-custom-job-id"

IMAGE_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/image_1.0.0.yaml",
    "metadata": Value(string_value="test-image-dataset"),
}
TABULAR_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/tabular_1.0.0.yaml",
    "metadata": Value(string_value="test-tabular-dataset"),
}
TEXT_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/text_1.0.0.yaml",
    "metadata": Value(string_value="test-text-dataset"),
}
VIDEO_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/video_1.0.0.yaml",
    "metadata": Value(string_value="test-video-dataset"),
}
TIME_SERIES_DATASET = {
    "display_name": str(uuid4()),
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/time_series_1.0.0.yaml",
    "metadata": Value(string_value="test-video-dataset"),
}
DATASET_ID = os.environ.get("DATASET_ID", "test-dataset-id")
TEST_EXPORT_CONFIG = {"gcs_destination": {"output_uri_prefix": "gs://test-vertex-ai-bucket/exports"}}
TEST_IMPORT_CONFIG = [
    {
        "data_item_labels": {
            "test-labels-name": "test-labels-value",
        },
        "import_schema_uri": (
            "gs://google-cloud-aiplatform/schema/dataset/ioformat/image_bounding_box_io_format_1.0.0.yaml"
        ),
        "gcs_source": {
            "uris": ["gs://ucaip-test-us-central1/dataset/salads_oid_ml_use_public_unassigned.jsonl"]
        },
    },
]
DATASET_TO_UPDATE = {"display_name": "test-name"}
TEST_UPDATE_MASK = {"paths": ["displayName"]}

TEST_TIME_COLUMN = "date"
TEST_TIME_SERIES_IDENTIFIER_COLUMN = "store_name"
TEST_TARGET_COLUMN = "sale_dollars"

COLUMN_SPECS = {
    TEST_TIME_COLUMN: "timestamp",
    TEST_TARGET_COLUMN: "numeric",
    "city": "categorical",
    "zip_code": "categorical",
    "county": "categorical",
}

COLUMN_TRANSFORMATIONS = [
    {"categorical": {"column_name": "Type"}},
    {"numeric": {"column_name": "Age"}},
    {"categorical": {"column_name": "Breed1"}},
    {"categorical": {"column_name": "Color1"}},
    {"categorical": {"column_name": "Color2"}},
    {"categorical": {"column_name": "MaturitySize"}},
    {"categorical": {"column_name": "FurLength"}},
    {"categorical": {"column_name": "Vaccinated"}},
    {"categorical": {"column_name": "Sterilized"}},
    {"categorical": {"column_name": "Health"}},
    {"numeric": {"column_name": "Fee"}},
    {"numeric": {"column_name": "PhotoAmt"}},
]

MODEL_ID = os.environ.get("MODEL_ID", "test-model-id")
MODEL_ARTIFACT_URI = os.environ.get("MODEL_ARTIFACT_URI", "path_to_folder_with_model_artifacts")
MODEL_NAME = f"projects/{PROJECT_ID}/locations/{REGION}/models/{MODEL_ID}"
JOB_DISPLAY_NAME = f"temp_create_batch_prediction_job_test_{uuid4()}"
BIGQUERY_SOURCE = f"bq://{PROJECT_ID}.test_iowa_liquor_sales_forecasting_us.2021_sales_predict"
GCS_DESTINATION_PREFIX = "gs://test-vertex-ai-bucket-us/output"
MODEL_PARAMETERS: dict | None = {}

ENDPOINT_CONF = {
    "display_name": f"endpoint_test_{uuid4()}",
}
DEPLOYED_MODEL = {
    # format: 'projects/{project}/locations/{location}/models/{model}'
    "model": f"projects/{PROJECT_ID}/locations/{REGION}/models/{MODEL_ID}",
    "display_name": f"temp_endpoint_test_{uuid4()}",
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

MODEL_OUTPUT_CONFIG = {
    "artifact_destination": {
        "output_uri_prefix": STAGING_BUCKET,
    },
    "export_format_id": "custom-trained",
}
MODEL_OBJ = {
    "display_name": f"model-{str(uuid4())}",
    "artifact_uri": MODEL_ARTIFACT_URI,
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

with models.DAG(
    "example_gcp_vertex_ai_custom_jobs",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as custom_jobs_dag:
    # [START how_to_cloud_vertex_ai_create_custom_container_training_job_operator]
    create_custom_container_training_job = CreateCustomContainerTrainingJobOperator(
        task_id="custom_container_task",
        staging_bucket=STAGING_BUCKET,
        display_name=f"train-housing-container-{DISPLAY_NAME}",
        container_uri=CUSTOM_CONTAINER_URI,
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=DATASET_ID,
        command=["python3", "task.py"],
        model_display_name=f"container-housing-model-{DISPLAY_NAME}",
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
    # [END how_to_cloud_vertex_ai_create_custom_container_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_custom_python_package_training_job_operator]
    create_custom_python_package_training_job = CreateCustomPythonPackageTrainingJobOperator(
        task_id="python_package_task",
        staging_bucket=STAGING_BUCKET,
        display_name=f"train-housing-py-package-{DISPLAY_NAME}",
        python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
        python_module_name=PYTHON_MODULE_NAME,
        container_uri=CONTAINER_URI,
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=DATASET_ID,
        model_display_name=f"py-package-housing-model-{DISPLAY_NAME}",
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
    # [END how_to_cloud_vertex_ai_create_custom_python_package_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_custom_training_job_operator]
    create_custom_training_job = CreateCustomTrainingJobOperator(
        task_id="custom_task",
        staging_bucket=STAGING_BUCKET,
        display_name=f"train-housing-custom-{DISPLAY_NAME}",
        script_path=LOCAL_TRAINING_SCRIPT_PATH,
        container_uri=CONTAINER_URI,
        requirements=["gcsfs==0.7.1"],
        model_serving_container_image_uri=MODEL_SERVING_CONTAINER_URI,
        # run params
        dataset_id=DATASET_ID,
        replica_count=1,
        model_display_name=f"custom-housing-model-{DISPLAY_NAME}",
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_custom_training_job_operator]

    # [START how_to_cloud_vertex_ai_delete_custom_training_job_operator]
    delete_custom_training_job = DeleteCustomTrainingJobOperator(
        task_id="delete_custom_training_job",
        training_pipeline_id=TRAINING_PIPELINE_ID,
        custom_job_id=CUSTOM_JOB_ID,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_custom_training_job_operator]

    # [START how_to_cloud_vertex_ai_list_custom_training_job_operator]
    list_custom_training_job = ListCustomTrainingJobOperator(
        task_id="list_custom_training_job",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_custom_training_job_operator]

with models.DAG(
    "example_gcp_vertex_ai_dataset",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dataset_dag:
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

    create_time_series_dataset_job
    create_text_dataset_job >> delete_dataset_job
    create_tabular_dataset_job >> get_dataset
    create_image_dataset_job >> import_data_job >> export_data_job
    create_video_dataset_job >> update_dataset_job
    list_dataset_job

with models.DAG(
    "example_gcp_vertex_ai_auto_ml",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as auto_ml_dag:
    # [START how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]
    create_auto_ml_forecasting_training_job = CreateAutoMLForecastingTrainingJobOperator(
        task_id="auto_ml_forecasting_task",
        display_name=f"auto-ml-forecasting-{DISPLAY_NAME}",
        optimization_objective="minimize-rmse",
        column_specs=COLUMN_SPECS,
        # run params
        dataset_id=DATASET_ID,
        target_column=TEST_TARGET_COLUMN,
        time_column=TEST_TIME_COLUMN,
        time_series_identifier_column=TEST_TIME_SERIES_IDENTIFIER_COLUMN,
        available_at_forecast_columns=[TEST_TIME_COLUMN],
        unavailable_at_forecast_columns=[TEST_TARGET_COLUMN],
        time_series_attribute_columns=["city", "zip_code", "county"],
        forecast_horizon=30,
        context_window=30,
        data_granularity_unit="day",
        data_granularity_count=1,
        weight_column=None,
        budget_milli_node_hours=1000,
        model_display_name=f"auto-ml-forecasting-model-{DISPLAY_NAME}",
        predefined_split_column_name=None,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_forecasting_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_auto_ml_image_training_job_operator]
    create_auto_ml_image_training_job = CreateAutoMLImageTrainingJobOperator(
        task_id="auto_ml_image_task",
        display_name=f"auto-ml-image-{DISPLAY_NAME}",
        dataset_id=DATASET_ID,
        prediction_type="classification",
        multi_label=False,
        model_type="CLOUD",
        training_fraction_split=0.6,
        validation_fraction_split=0.2,
        test_fraction_split=0.2,
        budget_milli_node_hours=8000,
        model_display_name=f"auto-ml-image-model-{DISPLAY_NAME}",
        disable_early_stopping=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_image_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]
    create_auto_ml_tabular_training_job = CreateAutoMLTabularTrainingJobOperator(
        task_id="auto_ml_tabular_task",
        display_name=f"auto-ml-tabular-{DISPLAY_NAME}",
        optimization_prediction_type="classification",
        column_transformations=COLUMN_TRANSFORMATIONS,
        dataset_id=DATASET_ID,
        target_column="Adopted",
        training_fraction_split=0.8,
        validation_fraction_split=0.1,
        test_fraction_split=0.1,
        model_display_name="adopted-prediction-model",
        disable_early_stopping=False,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_tabular_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]
    create_auto_ml_text_training_job = CreateAutoMLTextTrainingJobOperator(
        task_id="auto_ml_text_task",
        display_name=f"auto-ml-text-{DISPLAY_NAME}",
        prediction_type="classification",
        multi_label=False,
        dataset_id=DATASET_ID,
        model_display_name=f"auto-ml-text-model-{DISPLAY_NAME}",
        training_fraction_split=0.7,
        validation_fraction_split=0.2,
        test_fraction_split=0.1,
        sync=True,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_text_training_job_operator]

    # [START how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]
    create_auto_ml_video_training_job = CreateAutoMLVideoTrainingJobOperator(
        task_id="auto_ml_video_task",
        display_name=f"auto-ml-video-{DISPLAY_NAME}",
        prediction_type="classification",
        model_type="CLOUD",
        dataset_id=DATASET_ID,
        model_display_name=f"auto-ml-video-model-{DISPLAY_NAME}",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_auto_ml_video_training_job_operator]

    # [START how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]
    delete_auto_ml_training_job = DeleteAutoMLTrainingJobOperator(
        task_id="delete_auto_ml_training_job",
        training_pipeline_id=TRAINING_PIPELINE_ID,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_auto_ml_training_job_operator]

    # [START how_to_cloud_vertex_ai_list_auto_ml_training_job_operator]
    list_auto_ml_training_job = ListAutoMLTrainingJobOperator(
        task_id="list_auto_ml_training_job",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_auto_ml_training_job_operator]

with models.DAG(
    "example_gcp_vertex_ai_batch_prediction_job",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as batch_prediction_job_dag:
    # [START how_to_cloud_vertex_ai_create_batch_prediction_job_operator]
    create_batch_prediction_job = CreateBatchPredictionJobOperator(
        task_id="create_batch_prediction_job",
        job_display_name=JOB_DISPLAY_NAME,
        model_name=MODEL_NAME,
        predictions_format="csv",
        bigquery_source=BIGQUERY_SOURCE,
        gcs_destination_prefix=GCS_DESTINATION_PREFIX,
        model_parameters=MODEL_PARAMETERS,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_create_batch_prediction_job_operator]

    # [START how_to_cloud_vertex_ai_list_batch_prediction_job_operator]
    list_batch_prediction_job = ListBatchPredictionJobsOperator(
        task_id="list_batch_prediction_jobs",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_batch_prediction_job_operator]

    # [START how_to_cloud_vertex_ai_delete_batch_prediction_job_operator]
    delete_batch_prediction_job = DeleteBatchPredictionJobOperator(
        task_id="delete_batch_prediction_job",
        batch_prediction_job_id=create_batch_prediction_job.output["batch_prediction_job_id"],
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_batch_prediction_job_operator]

    create_batch_prediction_job >> delete_batch_prediction_job
    list_batch_prediction_job

with models.DAG(
    "example_gcp_vertex_ai_endpoint",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as endpoint_dag:
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

    create_endpoint >> deploy_model >> undeploy_model >> delete_endpoint
    list_endpoints

with models.DAG(
    "example_gcp_vertex_ai_hyperparameter_tuning_job",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as hyperparameter_tuning_job_dag:
    # [START how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator]
    create_hyperparameter_tuning_job = CreateHyperparameterTuningJobOperator(
        task_id="create_hyperparameter_tuning_job",
        staging_bucket=STAGING_BUCKET,
        display_name=f"horses-humans-hyptertune-{DISPLAY_NAME}",
        worker_pool_specs=[
            {
                "machine_spec": {
                    "machine_type": MACHINE_TYPE,
                    "accelerator_type": ACCELERATOR_TYPE,
                    "accelerator_count": ACCELERATOR_COUNT,
                },
                "replica_count": REPLICA_COUNT,
                "container_spec": {
                    "image_uri": f"gcr.io/{PROJECT_ID}/horse-human:hypertune",
                },
            }
        ],
        sync=False,
        region=REGION,
        project_id=PROJECT_ID,
        parameter_spec={
            "learning_rate": aiplatform.hyperparameter_tuning.DoubleParameterSpec(
                min=0.01, max=1, scale="log"
            ),
            "momentum": aiplatform.hyperparameter_tuning.DoubleParameterSpec(min=0, max=1, scale="linear"),
            "num_neurons": aiplatform.hyperparameter_tuning.DiscreteParameterSpec(
                values=[64, 128, 512], scale="linear"
            ),
        },
        metric_spec={
            "accuracy": "maximize",
        },
        max_trial_count=15,
        parallel_trial_count=3,
    )
    # [END how_to_cloud_vertex_ai_create_hyperparameter_tuning_job_operator]

    # [START how_to_cloud_vertex_ai_get_hyperparameter_tuning_job_operator]
    get_hyperparameter_tuning_job = GetHyperparameterTuningJobOperator(
        task_id="get_hyperparameter_tuning_job",
        project_id=PROJECT_ID,
        region=REGION,
        hyperparameter_tuning_job_id=create_hyperparameter_tuning_job.output["hyperparameter_tuning_job_id"],
    )
    # [END how_to_cloud_vertex_ai_get_hyperparameter_tuning_job_operator]

    # [START how_to_cloud_vertex_ai_delete_hyperparameter_tuning_job_operator]
    delete_hyperparameter_tuning_job = DeleteHyperparameterTuningJobOperator(
        task_id="delete_hyperparameter_tuning_job",
        project_id=PROJECT_ID,
        region=REGION,
        hyperparameter_tuning_job_id=create_hyperparameter_tuning_job.output["hyperparameter_tuning_job_id"],
    )
    # [END how_to_cloud_vertex_ai_delete_hyperparameter_tuning_job_operator]

    # [START how_to_cloud_vertex_ai_list_hyperparameter_tuning_job_operator]
    list_hyperparameter_tuning_job = ListHyperparameterTuningJobOperator(
        task_id="list_hyperparameter_tuning_job",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_hyperparameter_tuning_job_operator]

    create_hyperparameter_tuning_job >> get_hyperparameter_tuning_job >> delete_hyperparameter_tuning_job
    list_hyperparameter_tuning_job

with models.DAG(
    "example_gcp_vertex_ai_model_service",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as model_service_dag:
    # [START how_to_cloud_vertex_ai_upload_model_operator]
    upload_model = UploadModelOperator(
        task_id="upload_model",
        region=REGION,
        project_id=PROJECT_ID,
        model=MODEL_OBJ,
    )
    # [END how_to_cloud_vertex_ai_upload_model_operator]

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
    )
    # [END how_to_cloud_vertex_ai_delete_model_operator]

    # [START how_to_cloud_vertex_ai_list_models_operator]
    list_models = ListModelsOperator(
        task_id="list_models",
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END how_to_cloud_vertex_ai_list_models_operator]

    upload_model >> export_model >> delete_model
    list_models
