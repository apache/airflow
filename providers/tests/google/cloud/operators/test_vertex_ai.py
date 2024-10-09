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

from unittest import mock
from unittest.mock import MagicMock, call

import pytest

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.api_core.gapic_v1.method import DEFAULT
from google.api_core.retry import Retry

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, TaskDeferred
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
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    DeletePipelineJobOperator,
    GetPipelineJobOperator,
    ListPipelineJobOperator,
    RunPipelineJobOperator,
)
from airflow.providers.google.cloud.triggers.vertex_ai import (
    CustomContainerTrainingJobTrigger,
    CustomPythonPackageTrainingJobTrigger,
    CustomTrainingJobTrigger,
    RunPipelineJobTrigger,
)
from airflow.utils import timezone

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"
VERTEX_AI_LINKS_PATH = "airflow.providers.google.cloud.links.vertex_ai.{}"
TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
STAGING_BUCKET = "gs://test-vertex-ai-bucket"
DISPLAY_NAME = "display_name_1"  # Create random display name
DISPLAY_NAME_2 = "display_nmae_2"
ARGS = ["--tfds", "tf_flowers:3.*.*"]
CONTAINER_URI = "gcr.io/cloud-aiplatform/training/tf-cpu.2-2:latest"
REPLICA_COUNT = 1
MACHINE_TYPE = "n1-standard-4"
ACCELERATOR_TYPE = "ACCELERATOR_TYPE_UNSPECIFIED"
ACCELERATOR_COUNT = 0
TRAINING_FRACTION_SPLIT = 0.7
TEST_FRACTION_SPLIT = 0.15
VALIDATION_FRACTION_SPLIT = 0.15
COMMAND_2 = ["echo", "Hello World"]

TEST_API_ENDPOINT: str = "test-api-endpoint"
TEST_PIPELINE_JOB: str = "test-pipeline-job"
TEST_TRAINING_PIPELINE: str = "test-training-pipeline"
TEST_PIPELINE_JOB_ID: str = "test-pipeline-job-id"

PYTHON_PACKAGE = "/files/trainer-0.1.tar.gz"
PYTHON_PACKAGE_CMDARGS = "test-python-cmd"
PYTHON_PACKAGE_GCS_URI = "gs://test-vertex-ai-bucket/trainer-0.1.tar.gz"
PYTHON_MODULE_NAME = "trainer.task"

TRAINING_PIPELINE_ID = "test-training-pipeline-id"
CUSTOM_JOB_ID = "test-custom-job-id"

TEST_DATASET = {
    "display_name": "test-dataset-name",
    "metadata_schema_uri": "gs://google-cloud-aiplatform/schema/dataset/metadata/image_1.0.0.yaml",
    "metadata": "test-image-dataset",
}
TEST_DATASET_ID = "test-dataset-id"
TEST_PARENT_MODEL = "test-parent-model"
VERSIONED_TEST_PARENT_MODEL = f"{TEST_PARENT_MODEL}@1"
TEST_EXPORT_CONFIG = {
    "annotationsFilter": "test-filter",
    "gcs_destination": {"output_uri_prefix": "airflow-system-tests-data"},
}
TEST_IMPORT_CONFIG = [
    {
        "data_item_labels": {
            "test-labels-name": "test-labels-value",
        },
        "import_schema_uri": "test-shema-uri",
        "gcs_source": {"uris": ["test-string"]},
    },
    {},
]
TEST_UPDATE_MASK = "test-update-mask"

TEST_TRAINING_TARGET_COLUMN = "target"
TEST_TRAINING_TIME_COLUMN = "time"
TEST_TRAINING_TIME_SERIES_IDENTIFIER_COLUMN = "time_series_identifier"
TEST_TRAINING_UNAVAILABLE_AT_FORECAST_COLUMNS: list[str] = []
TEST_TRAINING_AVAILABLE_AT_FORECAST_COLUMNS: list[str] = []
TEST_TRAINING_FORECAST_HORIZON = 10
TEST_TRAINING_DATA_GRANULARITY_UNIT = "day"
TEST_TRAINING_DATA_GRANULARITY_COUNT = 1

TEST_MODEL_ID = "test_model_id"
TEST_MODEL_NAME = f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/models/test_model_id"
TEST_MODEL_OBJ: dict = {}
TEST_JOB_DISPLAY_NAME = "temp_create_batch_prediction_job_test"
TEST_BATCH_PREDICTION_JOB_ID = "test_batch_prediction_job_id"

TEST_ENDPOINT = {
    "display_name": "endpoint_test",
}
TEST_ENDPOINT_ID = "1234567890"
TEST_DEPLOYED_MODEL = {
    # format: 'projects/{project}/locations/{location}/models/{model}'
    "model": f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/models/test_model_id",
    "display_name": "temp_endpoint_test",
    "dedicated_resources": {
        "min_replica_count": 1,
        "max_replica_count": 1,
    },
}
TEST_DEPLOYED_MODEL_ID = "test_deployed_model_id"
TEST_HYPERPARAMETER_TUNING_JOB_ID = "test_hyperparameter_tuning_job_id"

TEST_OUTPUT_CONFIG = {
    "artifact_destination": {"output_uri_prefix": "gcs_destination_output_uri_prefix"},
    "export_format_id": "tf-saved-model",
}

TEST_CREATE_REQUEST_TIMEOUT = 100.5
TEST_BATCH_SIZE = 4000
TEST_VERSION_ALIASES = ["new-alias"]

TEST_TEMPLATE_PATH = "test_template_path"

SYNC_DEPRECATION_WARNING = "The 'sync' parameter is deprecated and will be removed after {}."

TEST_TRAINING_PIPELINE_DATA = {
    "model_to_upload": {
        "name": "projects/test-project/locations/us-central1/models/test-model",
        "display_name": "test-model",
    },
    "training_task_metadata": {"backingCustomJob": "prefix/test-custom-job"},
}
TEST_TRAINING_PIPELINE_DATA_NO_MODEL = {
    "training_task_metadata": {"backingCustomJob": "prefix/test-custom-job"}
}


class TestVertexAICreateCustomContainerTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.Dataset"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_custom_container_training_job.return_value = (
            None,
            "training_id",
            "custom_job_id",
        )
        op = CreateCustomContainerTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            parent_model=TEST_PARENT_MODEL,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_dataset.assert_called_once_with(name=TEST_DATASET_ID)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_custom_container_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            dataset=mock_dataset.return_value,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
            model_serving_container_predict_route=None,
            model_serving_container_health_route=None,
            model_serving_container_command=None,
            model_serving_container_args=None,
            model_serving_container_environment_variables=None,
            model_serving_container_ports=None,
            model_description=None,
            model_instance_schema_uri=None,
            model_parameters_schema_uri=None,
            model_prediction_schema_uri=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            # RUN
            annotation_schema_uri=None,
            model_labels=None,
            base_output_dir=None,
            service_account=None,
            network=None,
            bigquery_destination=None,
            environment_variables=None,
            boot_disk_type="pd-ssd",
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch(VERTEX_AI_PATH.format("custom_job.Dataset"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_custom_container_training_job.return_value = (
            None,
            "training_id",
            "custom_job_id",
        )
        op = CreateCustomContainerTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            parent_model=VERSIONED_TEST_PARENT_MODEL,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_custom_container_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            dataset=mock_dataset.return_value,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
            model_serving_container_predict_route=None,
            model_serving_container_health_route=None,
            model_serving_container_command=None,
            model_serving_container_args=None,
            model_serving_container_environment_variables=None,
            model_serving_container_ports=None,
            model_description=None,
            model_instance_schema_uri=None,
            model_parameters_schema_uri=None,
            model_prediction_schema_uri=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            # RUN
            annotation_schema_uri=None,
            model_labels=None,
            base_output_dir=None,
            service_account=None,
            network=None,
            bigquery_destination=None,
            environment_variables=None,
            boot_disk_type="pd-ssd",
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomContainerTrainingJobOperator.hook"))
    def test_execute_enters_deferred_state(self, mock_hook):
        task = CreateCustomContainerTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
            ):
                task.execute(context={"ti": mock.MagicMock()})
        assert isinstance(
            exc.value.trigger, CustomContainerTrainingJobTrigger
        ), "Trigger is not a CustomContainerTrainingJobTrigger"

    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIModelLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomContainerTrainingJobOperator.xcom_push"))
    @mock.patch(
        VERTEX_AI_PATH.format("custom_job.CreateCustomContainerTrainingJobOperator.hook.extract_model_id")
    )
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomContainerTrainingJobOperator.hook"))
    def test_execute_complete_success(
        self, mock_hook, mock_hook_extract_model_id, mock_xcom_push, mock_link_persist
    ):
        task = CreateCustomContainerTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        expected_result = TEST_TRAINING_PIPELINE_DATA["model_to_upload"]
        mock_hook_extract_model_id.return_value = "test-model"
        actual_result = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": "",
                "job": TEST_TRAINING_PIPELINE_DATA,
            },
        )
        mock_xcom_push.assert_called_with(None, key="model_id", value="test-model")
        mock_link_persist.assert_called_once_with(context=None, task_instance=task, model_id="test-model")
        assert actual_result == expected_result

    def test_execute_complete_error_status_raises_exception(self):
        task = CreateCustomContainerTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event={"status": "error", "message": "test message"})

    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIModelLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomContainerTrainingJobOperator.xcom_push"))
    @mock.patch(
        VERTEX_AI_PATH.format("custom_job.CreateCustomContainerTrainingJobOperator.hook.extract_model_id")
    )
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomContainerTrainingJobOperator.hook"))
    def test_execute_complete_no_model_produced(
        self,
        mock_hook,
        hook_extract_model_id,
        mock_xcom_push,
        mock_link_persist,
    ):
        task = CreateCustomContainerTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            command=COMMAND_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        expected_result = None
        hook_extract_model_id.return_value = None
        actual_result = task.execute_complete(
            context=None,
            event={"status": "success", "message": "", "job": TEST_TRAINING_PIPELINE_DATA_NO_MODEL},
        )
        mock_xcom_push.assert_called_once()
        mock_link_persist.assert_not_called()
        assert actual_result == expected_result


class TestVertexAICreateCustomPythonPackageTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.Dataset"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_custom_python_package_training_job.return_value = (
            None,
            "training_id",
            "custom_job_id",
        )
        op = CreateCustomPythonPackageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            container_uri=CONTAINER_URI,
            args=ARGS,
            model_serving_container_image_uri=CONTAINER_URI,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            parent_model=TEST_PARENT_MODEL,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_dataset.assert_called_once_with(name=TEST_DATASET_ID)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_custom_python_package_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            dataset=mock_dataset.return_value,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
            model_serving_container_predict_route=None,
            model_serving_container_health_route=None,
            model_serving_container_command=None,
            model_serving_container_args=None,
            model_serving_container_environment_variables=None,
            model_serving_container_ports=None,
            model_description=None,
            model_instance_schema_uri=None,
            model_parameters_schema_uri=None,
            model_prediction_schema_uri=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            # RUN
            annotation_schema_uri=None,
            model_labels=None,
            base_output_dir=None,
            service_account=None,
            network=None,
            bigquery_destination=None,
            environment_variables=None,
            boot_disk_type="pd-ssd",
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
        )

    @mock.patch(VERTEX_AI_PATH.format("custom_job.Dataset"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_custom_python_package_training_job.return_value = (
            None,
            "training_id",
            "custom_job_id",
        )
        op = CreateCustomPythonPackageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            container_uri=CONTAINER_URI,
            args=ARGS,
            model_serving_container_image_uri=CONTAINER_URI,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            parent_model=VERSIONED_TEST_PARENT_MODEL,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_custom_python_package_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            dataset=mock_dataset.return_value,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
            model_serving_container_predict_route=None,
            model_serving_container_health_route=None,
            model_serving_container_command=None,
            model_serving_container_args=None,
            model_serving_container_environment_variables=None,
            model_serving_container_ports=None,
            model_description=None,
            model_instance_schema_uri=None,
            model_parameters_schema_uri=None,
            model_prediction_schema_uri=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            # RUN
            annotation_schema_uri=None,
            model_labels=None,
            base_output_dir=None,
            service_account=None,
            network=None,
            bigquery_destination=None,
            environment_variables=None,
            boot_disk_type="pd-ssd",
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
        )

    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomPythonPackageTrainingJobOperator.hook"))
    def test_execute_enters_deferred_state(self, mock_hook):
        task = CreateCustomPythonPackageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            container_uri=CONTAINER_URI,
            args=ARGS,
            model_serving_container_image_uri=CONTAINER_URI,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
            ):
                task.execute(context={"ti": mock.MagicMock()})
        assert isinstance(
            exc.value.trigger, CustomPythonPackageTrainingJobTrigger
        ), "Trigger is not a CustomPythonPackageTrainingJobTrigger"

    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIModelLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomPythonPackageTrainingJobOperator.xcom_push"))
    @mock.patch(
        VERTEX_AI_PATH.format("custom_job.CreateCustomPythonPackageTrainingJobOperator.hook.extract_model_id")
    )
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomPythonPackageTrainingJobOperator.hook"))
    def test_execute_complete_success(
        self,
        mock_hook,
        hook_extract_model_id,
        mock_xcom_push,
        mock_link_persist,
    ):
        task = CreateCustomPythonPackageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            container_uri=CONTAINER_URI,
            args=ARGS,
            model_serving_container_image_uri=CONTAINER_URI,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        expected_result = TEST_TRAINING_PIPELINE_DATA["model_to_upload"]
        hook_extract_model_id.return_value = "test-model"
        actual_result = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": "",
                "job": TEST_TRAINING_PIPELINE_DATA,
            },
        )
        mock_xcom_push.assert_called_with(None, key="model_id", value="test-model")
        mock_link_persist.assert_called_once_with(context=None, task_instance=task, model_id="test-model")
        assert actual_result == expected_result

    def test_execute_complete_error_status_raises_exception(self):
        task = CreateCustomPythonPackageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            container_uri=CONTAINER_URI,
            args=ARGS,
            model_serving_container_image_uri=CONTAINER_URI,
            model_display_name=DISPLAY_NAME_2,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event={"status": "error", "message": "test message"})

    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIModelLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomPythonPackageTrainingJobOperator.xcom_push"))
    @mock.patch(
        VERTEX_AI_PATH.format("custom_job.CreateCustomPythonPackageTrainingJobOperator.hook.extract_model_id")
    )
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomPythonPackageTrainingJobOperator.hook"))
    def test_execute_complete_no_model_produced(
        self,
        mock_hook,
        hook_extract_model_id,
        mock_xcom_push,
        mock_link_persist,
    ):
        task = CreateCustomPythonPackageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            container_uri=CONTAINER_URI,
            args=ARGS,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=TRAINING_FRACTION_SPLIT,
            validation_fraction_split=VALIDATION_FRACTION_SPLIT,
            test_fraction_split=TEST_FRACTION_SPLIT,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        expected_result = None
        actual_result = task.execute_complete(
            context=None,
            event={"status": "success", "message": "", "job": TEST_TRAINING_PIPELINE_DATA_NO_MODEL},
        )
        mock_xcom_push.assert_called_once()
        mock_link_persist.assert_not_called()
        assert actual_result == expected_result


class TestVertexAICreateCustomTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.Dataset"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_custom_training_job.return_value = (
            None,
            "training_id",
            "custom_job_id",
        )
        op = CreateCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            script_path=PYTHON_PACKAGE,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            requirements=[],
            replica_count=1,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            parent_model=TEST_PARENT_MODEL,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_dataset.assert_called_once_with(name=TEST_DATASET_ID)
        mock_hook.return_value.create_custom_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            script_path=PYTHON_PACKAGE,
            requirements=[],
            dataset=mock_dataset.return_value,
            model_display_name=None,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            parent_model=TEST_PARENT_MODEL,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_serving_container_predict_route=None,
            model_serving_container_health_route=None,
            model_serving_container_command=None,
            model_serving_container_args=None,
            model_serving_container_environment_variables=None,
            model_serving_container_ports=None,
            model_description=None,
            model_instance_schema_uri=None,
            model_parameters_schema_uri=None,
            model_prediction_schema_uri=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            # RUN
            annotation_schema_uri=None,
            model_labels=None,
            base_output_dir=None,
            service_account=None,
            network=None,
            bigquery_destination=None,
            environment_variables=None,
            boot_disk_type="pd-ssd",
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch(VERTEX_AI_PATH.format("custom_job.Dataset"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_custom_training_job.return_value = (
            None,
            "training_id",
            "custom_job_id",
        )
        op = CreateCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            script_path=PYTHON_PACKAGE,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            requirements=[],
            replica_count=1,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            parent_model=VERSIONED_TEST_PARENT_MODEL,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_custom_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            script_path=PYTHON_PACKAGE,
            requirements=[],
            dataset=mock_dataset.return_value,
            model_display_name=None,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            parent_model=TEST_PARENT_MODEL,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_serving_container_predict_route=None,
            model_serving_container_health_route=None,
            model_serving_container_command=None,
            model_serving_container_args=None,
            model_serving_container_environment_variables=None,
            model_serving_container_ports=None,
            model_description=None,
            model_instance_schema_uri=None,
            model_parameters_schema_uri=None,
            model_prediction_schema_uri=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            # RUN
            annotation_schema_uri=None,
            model_labels=None,
            base_output_dir=None,
            service_account=None,
            network=None,
            bigquery_destination=None,
            environment_variables=None,
            boot_disk_type="pd-ssd",
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomTrainingJobOperator.hook"))
    def test_execute_enters_deferred_state(self, mock_hook):
        task = CreateCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            script_path=PYTHON_PACKAGE,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            requirements=[],
            replica_count=1,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            with pytest.warns(
                AirflowProviderDeprecationWarning, match=SYNC_DEPRECATION_WARNING.format("01.10.2024")
            ):
                task.execute(context={"ti": mock.MagicMock()})
        assert isinstance(
            exc.value.trigger, CustomTrainingJobTrigger
        ), "Trigger is not a CustomTrainingJobTrigger"

    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIModelLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomTrainingJobOperator.xcom_push"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomTrainingJobOperator.hook.extract_model_id"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomTrainingJobOperator.hook"))
    def test_execute_complete_success(
        self,
        mock_hook,
        hook_extract_model_id,
        mock_xcom_push,
        mock_link_persist,
    ):
        task = CreateCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            script_path=PYTHON_PACKAGE,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            requirements=[],
            replica_count=1,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        expected_result = TEST_TRAINING_PIPELINE_DATA["model_to_upload"]
        hook_extract_model_id.return_value = "test-model"
        actual_result = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": "",
                "job": TEST_TRAINING_PIPELINE_DATA,
            },
        )
        mock_xcom_push.assert_called_with(None, key="model_id", value="test-model")
        mock_link_persist.assert_called_once_with(context=None, task_instance=task, model_id="test-model")
        assert actual_result == expected_result

    def test_execute_complete_error_status_raises_exception(self):
        task = CreateCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            script_path=PYTHON_PACKAGE,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            requirements=[],
            replica_count=1,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event={"status": "error", "message": "test message"})

    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIModelLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomTrainingJobOperator.xcom_push"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomTrainingJobOperator.hook.extract_model_id"))
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CreateCustomTrainingJobOperator.hook"))
    def test_execute_complete_no_model_produced(
        self,
        mock_hook,
        hook_extract_model_id,
        mock_xcom_push,
        mock_link_persist,
    ):
        task = CreateCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            script_path=PYTHON_PACKAGE,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            requirements=[],
            replica_count=1,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            deferrable=True,
        )
        expected_result = None
        hook_extract_model_id.return_value = None
        actual_result = task.execute_complete(
            context=None, event={"status": "success", "message": "", "job": {}}
        )
        mock_xcom_push.assert_called_once()
        mock_link_persist.assert_not_called()
        assert actual_result == expected_result


class TestVertexAIDeleteCustomTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook):
        op = DeleteCustomTrainingJobOperator(
            task_id=TASK_ID,
            training_pipeline_id=TRAINING_PIPELINE_ID,
            custom_job_id=CUSTOM_JOB_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_training_pipeline.assert_called_once_with(
            training_pipeline=TRAINING_PIPELINE_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_hook.return_value.delete_custom_job.assert_called_once_with(
            custom_job=CUSTOM_JOB_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
        ti = create_task_instance_of_operator(
            DeleteCustomTrainingJobOperator,
            # Templated fields
            training_pipeline_id="{{ 'training-pipeline-id' }}",
            custom_job_id="{{ 'custom_job_id' }}",
            region="{{ 'region' }}",
            project_id="{{ 'project_id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: DeleteCustomTrainingJobOperator = ti.task
        assert task.training_pipeline_id == "training-pipeline-id"
        assert task.custom_job_id == "custom_job_id"
        assert task.region == "region"
        assert task.project_id == "project_id"
        assert task.impersonation_chain == "impersonation-chain"

        # Deprecated aliases
        with pytest.warns(AirflowProviderDeprecationWarning):
            assert task.training_pipeline == "training-pipeline-id"
        with pytest.warns(AirflowProviderDeprecationWarning):
            assert task.custom_job == "custom_job_id"


class TestVertexAIListCustomTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        read_mask = "read_mask"

        op = ListCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_training_pipelines.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAICreateDatasetOperator:
    @mock.patch(VERTEX_AI_PATH.format("dataset.Dataset.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("dataset.DatasetHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = CreateDatasetOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset=TEST_DATASET,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_dataset.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset=TEST_DATASET,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIDeleteDatasetOperator:
    @mock.patch(VERTEX_AI_PATH.format("dataset.Dataset.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("dataset.DatasetHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = DeleteDatasetOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_dataset.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset=TEST_DATASET_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIExportDataOperator:
    @mock.patch(VERTEX_AI_PATH.format("dataset.Dataset.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("dataset.DatasetHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ExportDataOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            export_config=TEST_EXPORT_CONFIG,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.export_data.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset=TEST_DATASET_ID,
            export_config=TEST_EXPORT_CONFIG,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIImportDataOperator:
    @mock.patch(VERTEX_AI_PATH.format("dataset.Dataset.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("dataset.DatasetHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ImportDataOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            import_configs=TEST_IMPORT_CONFIG,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.import_data.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset=TEST_DATASET_ID,
            import_configs=TEST_IMPORT_CONFIG,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIListDatasetsOperator:
    @mock.patch(VERTEX_AI_PATH.format("dataset.Dataset.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("dataset.DatasetHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        read_mask = "read_mask"
        order_by = "order_by"

        op = ListDatasetsOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            filter=filter,
            page_size=page_size,
            page_token=page_token,
            read_mask=read_mask,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_datasets.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            filter=filter,
            page_size=page_size,
            page_token=page_token,
            read_mask=read_mask,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIUpdateDatasetOperator:
    @mock.patch(VERTEX_AI_PATH.format("dataset.Dataset.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("dataset.DatasetHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = UpdateDatasetOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            dataset_id=TEST_DATASET_ID,
            dataset=TEST_DATASET,
            update_mask=TEST_UPDATE_MASK,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_dataset.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            dataset_id=TEST_DATASET_ID,
            dataset=TEST_DATASET,
            update_mask=TEST_UPDATE_MASK,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAICreateAutoMLForecastingTrainingJobOperator:
    @mock.patch("google.cloud.aiplatform.datasets.TimeSeriesDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_forecasting_training_job.return_value = (None, "training_id")
        op = CreateAutoMLForecastingTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            target_column=TEST_TRAINING_TARGET_COLUMN,
            time_column=TEST_TRAINING_TIME_COLUMN,
            time_series_identifier_column=TEST_TRAINING_TIME_SERIES_IDENTIFIER_COLUMN,
            unavailable_at_forecast_columns=TEST_TRAINING_UNAVAILABLE_AT_FORECAST_COLUMNS,
            available_at_forecast_columns=TEST_TRAINING_AVAILABLE_AT_FORECAST_COLUMNS,
            forecast_horizon=TEST_TRAINING_FORECAST_HORIZON,
            data_granularity_unit=TEST_TRAINING_DATA_GRANULARITY_UNIT,
            data_granularity_count=TEST_TRAINING_DATA_GRANULARITY_COUNT,
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_dataset.assert_called_once_with(dataset_name=TEST_DATASET_ID)
        mock_hook.return_value.create_auto_ml_forecasting_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            target_column=TEST_TRAINING_TARGET_COLUMN,
            time_column=TEST_TRAINING_TIME_COLUMN,
            time_series_identifier_column=TEST_TRAINING_TIME_SERIES_IDENTIFIER_COLUMN,
            unavailable_at_forecast_columns=TEST_TRAINING_UNAVAILABLE_AT_FORECAST_COLUMNS,
            available_at_forecast_columns=TEST_TRAINING_AVAILABLE_AT_FORECAST_COLUMNS,
            forecast_horizon=TEST_TRAINING_FORECAST_HORIZON,
            data_granularity_unit=TEST_TRAINING_DATA_GRANULARITY_UNIT,
            data_granularity_count=TEST_TRAINING_DATA_GRANULARITY_COUNT,
            parent_model=TEST_PARENT_MODEL,
            optimization_objective=None,
            column_specs=None,
            column_transformations=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            predefined_split_column_name=None,
            weight_column=None,
            time_series_attribute_columns=None,
            context_window=None,
            export_evaluated_data_items=False,
            export_evaluated_data_items_bigquery_destination_uri=None,
            export_evaluated_data_items_override_destination=False,
            quantiles=None,
            validation_options=None,
            budget_milli_node_hours=1000,
            model_display_name=None,
            model_labels=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
            window_stride_length=None,
            window_max_count=None,
        )

    @mock.patch("google.cloud.aiplatform.datasets.TimeSeriesDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_forecasting_training_job.return_value = (None, "training_id")
        op = CreateAutoMLForecastingTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            target_column=TEST_TRAINING_TARGET_COLUMN,
            time_column=TEST_TRAINING_TIME_COLUMN,
            time_series_identifier_column=TEST_TRAINING_TIME_SERIES_IDENTIFIER_COLUMN,
            unavailable_at_forecast_columns=TEST_TRAINING_UNAVAILABLE_AT_FORECAST_COLUMNS,
            available_at_forecast_columns=TEST_TRAINING_AVAILABLE_AT_FORECAST_COLUMNS,
            forecast_horizon=TEST_TRAINING_FORECAST_HORIZON,
            data_granularity_unit=TEST_TRAINING_DATA_GRANULARITY_UNIT,
            data_granularity_count=TEST_TRAINING_DATA_GRANULARITY_COUNT,
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=VERSIONED_TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_auto_ml_forecasting_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            target_column=TEST_TRAINING_TARGET_COLUMN,
            time_column=TEST_TRAINING_TIME_COLUMN,
            time_series_identifier_column=TEST_TRAINING_TIME_SERIES_IDENTIFIER_COLUMN,
            unavailable_at_forecast_columns=TEST_TRAINING_UNAVAILABLE_AT_FORECAST_COLUMNS,
            available_at_forecast_columns=TEST_TRAINING_AVAILABLE_AT_FORECAST_COLUMNS,
            forecast_horizon=TEST_TRAINING_FORECAST_HORIZON,
            data_granularity_unit=TEST_TRAINING_DATA_GRANULARITY_UNIT,
            data_granularity_count=TEST_TRAINING_DATA_GRANULARITY_COUNT,
            parent_model=TEST_PARENT_MODEL,
            optimization_objective=None,
            column_specs=None,
            column_transformations=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            predefined_split_column_name=None,
            weight_column=None,
            time_series_attribute_columns=None,
            context_window=None,
            export_evaluated_data_items=False,
            export_evaluated_data_items_bigquery_destination_uri=None,
            export_evaluated_data_items_override_destination=False,
            quantiles=None,
            validation_options=None,
            budget_milli_node_hours=1000,
            model_display_name=None,
            model_labels=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
            window_stride_length=None,
            window_max_count=None,
        )


class TestVertexAICreateAutoMLImageTrainingJobOperator:
    @mock.patch("google.cloud.aiplatform.datasets.ImageDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_image_training_job.return_value = (None, "training_id")
        op = CreateAutoMLImageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            prediction_type="classification",
            multi_label=False,
            model_type="CLOUD",
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_dataset.assert_called_once_with(dataset_name=TEST_DATASET_ID)
        mock_hook.return_value.create_auto_ml_image_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            prediction_type="classification",
            parent_model=TEST_PARENT_MODEL,
            multi_label=False,
            model_type="CLOUD",
            base_model=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            budget_milli_node_hours=None,
            model_display_name=None,
            model_labels=None,
            disable_early_stopping=False,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch("google.cloud.aiplatform.datasets.ImageDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_image_training_job.return_value = (None, "training_id")
        op = CreateAutoMLImageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            prediction_type="classification",
            multi_label=False,
            model_type="CLOUD",
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=VERSIONED_TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_auto_ml_image_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            prediction_type="classification",
            parent_model=TEST_PARENT_MODEL,
            multi_label=False,
            model_type="CLOUD",
            base_model=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            budget_milli_node_hours=None,
            model_display_name=None,
            model_labels=None,
            disable_early_stopping=False,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )


class TestVertexAICreateAutoMLTabularTrainingJobOperator:
    @mock.patch("google.cloud.aiplatform.datasets.TabularDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value = MagicMock(
            **{
                "create_auto_ml_tabular_training_job.return_value": (None, "training_id"),
                "get_credentials_and_project_id.return_value": ("creds", "project_id"),
            }
        )
        op = CreateAutoMLTabularTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            target_column=None,
            optimization_prediction_type=None,
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_dataset.assert_called_once_with(
            dataset_name=TEST_DATASET_ID, project=GCP_PROJECT, credentials="creds"
        )
        mock_hook.return_value.create_auto_ml_tabular_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            parent_model=TEST_PARENT_MODEL,
            target_column=None,
            optimization_prediction_type=None,
            optimization_objective=None,
            column_specs=None,
            column_transformations=None,
            optimization_objective_recall_value=None,
            optimization_objective_precision_value=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            weight_column=None,
            budget_milli_node_hours=1000,
            model_display_name=None,
            model_labels=None,
            disable_early_stopping=False,
            export_evaluated_data_items=False,
            export_evaluated_data_items_bigquery_destination_uri=None,
            export_evaluated_data_items_override_destination=False,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch("google.cloud.aiplatform.datasets.TabularDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value = MagicMock(
            **{
                "create_auto_ml_tabular_training_job.return_value": (None, "training_id"),
                "get_credentials_and_project_id.return_value": ("creds", "project_id"),
            }
        )
        op = CreateAutoMLTabularTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            target_column=None,
            optimization_prediction_type=None,
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=VERSIONED_TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_auto_ml_tabular_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            parent_model=TEST_PARENT_MODEL,
            target_column=None,
            optimization_prediction_type=None,
            optimization_objective=None,
            column_specs=None,
            column_transformations=None,
            optimization_objective_recall_value=None,
            optimization_objective_precision_value=None,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            weight_column=None,
            budget_milli_node_hours=1000,
            model_display_name=None,
            model_labels=None,
            disable_early_stopping=False,
            export_evaluated_data_items=False,
            export_evaluated_data_items_bigquery_destination_uri=None,
            export_evaluated_data_items_override_destination=False,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )


class TestVertexAICreateAutoMLTextTrainingJobOperator:
    @mock.patch("google.cloud.aiplatform.datasets.TextDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_text_training_job.return_value = (None, "training_id")
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = CreateAutoMLTextTrainingJobOperator(
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                display_name=DISPLAY_NAME,
                dataset_id=TEST_DATASET_ID,
                prediction_type=None,
                multi_label=False,
                sentiment_max=10,
                sync=True,
                region=GCP_LOCATION,
                project_id=GCP_PROJECT,
                parent_model=TEST_PARENT_MODEL,
            )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_dataset.assert_called_once_with(dataset_name=TEST_DATASET_ID)
        mock_hook.return_value.create_auto_ml_text_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            parent_model=TEST_PARENT_MODEL,
            prediction_type=None,
            multi_label=False,
            sentiment_max=10,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            model_display_name=None,
            model_labels=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch("google.cloud.aiplatform.datasets.TextDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_text_training_job.return_value = (None, "training_id")
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = CreateAutoMLTextTrainingJobOperator(
                task_id=TASK_ID,
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                display_name=DISPLAY_NAME,
                dataset_id=TEST_DATASET_ID,
                prediction_type=None,
                multi_label=False,
                sentiment_max=10,
                sync=True,
                region=GCP_LOCATION,
                project_id=GCP_PROJECT,
                parent_model=VERSIONED_TEST_PARENT_MODEL,
            )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_auto_ml_text_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            parent_model=TEST_PARENT_MODEL,
            prediction_type=None,
            multi_label=False,
            sentiment_max=10,
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            model_display_name=None,
            model_labels=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )


class TestVertexAICreateAutoMLVideoTrainingJobOperator:
    @mock.patch("google.cloud.aiplatform.datasets.VideoDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_video_training_job.return_value = (None, "training_id")
        op = CreateAutoMLVideoTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            prediction_type="classification",
            model_type="CLOUD",
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_dataset.assert_called_once_with(dataset_name=TEST_DATASET_ID)
        mock_hook.return_value.create_auto_ml_video_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            parent_model=TEST_PARENT_MODEL,
            prediction_type="classification",
            model_type="CLOUD",
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            test_fraction_split=None,
            training_filter_split=None,
            test_filter_split=None,
            model_display_name=None,
            model_labels=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )

    @mock.patch("google.cloud.aiplatform.datasets.VideoDataset")
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute__parent_model_version_index_is_removed(self, mock_hook, mock_dataset):
        mock_hook.return_value.create_auto_ml_video_training_job.return_value = (None, "training_id")
        op = CreateAutoMLVideoTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            display_name=DISPLAY_NAME,
            dataset_id=TEST_DATASET_ID,
            prediction_type="classification",
            model_type="CLOUD",
            sync=True,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            parent_model=VERSIONED_TEST_PARENT_MODEL,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.return_value.create_auto_ml_video_training_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            dataset=mock_dataset.return_value,
            parent_model=TEST_PARENT_MODEL,
            prediction_type="classification",
            model_type="CLOUD",
            labels=None,
            training_encryption_spec_key_name=None,
            model_encryption_spec_key_name=None,
            training_fraction_split=None,
            test_fraction_split=None,
            training_filter_split=None,
            test_filter_split=None,
            model_display_name=None,
            model_labels=None,
            sync=True,
            is_default_version=None,
            model_version_aliases=None,
            model_version_description=None,
        )


class TestVertexAIDeleteAutoMLTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute(self, mock_hook):
        op = DeleteAutoMLTrainingJobOperator(
            task_id=TASK_ID,
            training_pipeline_id=TRAINING_PIPELINE_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_training_pipeline.assert_called_once_with(
            training_pipeline=TRAINING_PIPELINE_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
        ti = create_task_instance_of_operator(
            DeleteAutoMLTrainingJobOperator,
            # Templated fields
            training_pipeline_id="{{ 'training-pipeline-id' }}",
            region="{{ 'region' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: DeleteAutoMLTrainingJobOperator = ti.task
        assert task.training_pipeline_id == "training-pipeline-id"
        assert task.region == "region"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"

        with pytest.warns(AirflowProviderDeprecationWarning):
            assert task.training_pipeline == "training-pipeline-id"


class TestVertexAIListAutoMLTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("auto_ml.AutoMLHook"))
    def test_execute(self, mock_hook):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        read_mask = "read_mask"

        op = ListAutoMLTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_training_pipelines.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAICreateBatchPredictionJobOperator:
    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIBatchPredictionJobLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("batch_prediction_job.BatchPredictionJobHook"))
    def test_execute(self, mock_hook, mock_link_persist):
        mock_job = mock_hook.return_value.submit_batch_prediction_job.return_value
        mock_job.name = TEST_BATCH_PREDICTION_JOB_ID

        op = CreateBatchPredictionJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job_display_name=TEST_JOB_DISPLAY_NAME,
            model_name=TEST_MODEL_NAME,
            instances_format="jsonl",
            predictions_format="jsonl",
            create_request_timeout=TEST_CREATE_REQUEST_TIMEOUT,
            batch_size=TEST_BATCH_SIZE,
        )
        context = {"ti": mock.MagicMock()}
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match=SYNC_DEPRECATION_WARNING.format("28.08.2024"),
        ):
            op.execute(context=context)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.submit_batch_prediction_job.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job_display_name=TEST_JOB_DISPLAY_NAME,
            model_name=TEST_MODEL_NAME,
            instances_format="jsonl",
            predictions_format="jsonl",
            gcs_source=None,
            bigquery_source=None,
            gcs_destination_prefix=None,
            bigquery_destination_prefix=None,
            model_parameters=None,
            machine_type=None,
            accelerator_type=None,
            accelerator_count=None,
            starting_replica_count=None,
            max_replica_count=None,
            generate_explanation=False,
            explanation_metadata=None,
            explanation_parameters=None,
            labels=None,
            encryption_spec_key_name=None,
            create_request_timeout=TEST_CREATE_REQUEST_TIMEOUT,
            batch_size=TEST_BATCH_SIZE,
        )
        mock_job.wait_for_completion.assert_called_once()
        mock_job.to_dict.assert_called_once()
        mock_link_persist.assert_called_once_with(
            context=context, task_instance=op, batch_prediction_job_id=TEST_BATCH_PREDICTION_JOB_ID
        )

    @mock.patch(VERTEX_AI_LINKS_PATH.format("VertexAIBatchPredictionJobLink.persist"))
    @mock.patch(VERTEX_AI_PATH.format("batch_prediction_job.BatchPredictionJobHook"))
    def test_execute_deferrable(self, mock_hook, mock_link_persist):
        mock_job = mock_hook.return_value.submit_batch_prediction_job.return_value
        mock_job.name = TEST_BATCH_PREDICTION_JOB_ID
        op = CreateBatchPredictionJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job_display_name=TEST_JOB_DISPLAY_NAME,
            model_name=TEST_MODEL_NAME,
            instances_format="jsonl",
            predictions_format="jsonl",
            create_request_timeout=TEST_CREATE_REQUEST_TIMEOUT,
            batch_size=TEST_BATCH_SIZE,
            deferrable=True,
        )
        context = {"ti": mock.MagicMock()}
        with (
            pytest.raises(TaskDeferred) as exception_info,
            pytest.warns(
                AirflowProviderDeprecationWarning,
                match=SYNC_DEPRECATION_WARNING.format("28.08.2024"),
            ),
        ):
            op.execute(context=context)

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.submit_batch_prediction_job.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job_display_name=TEST_JOB_DISPLAY_NAME,
            model_name=TEST_MODEL_NAME,
            instances_format="jsonl",
            predictions_format="jsonl",
            gcs_source=None,
            bigquery_source=None,
            gcs_destination_prefix=None,
            bigquery_destination_prefix=None,
            model_parameters=None,
            machine_type=None,
            accelerator_type=None,
            accelerator_count=None,
            starting_replica_count=None,
            max_replica_count=None,
            generate_explanation=False,
            explanation_metadata=None,
            explanation_parameters=None,
            labels=None,
            encryption_spec_key_name=None,
            create_request_timeout=TEST_CREATE_REQUEST_TIMEOUT,
            batch_size=TEST_BATCH_SIZE,
        )
        mock_job.wait_for_completion.assert_not_called()
        mock_job.to_dict.assert_not_called()
        mock_link_persist.assert_called_once_with(
            batch_prediction_job_id=TEST_BATCH_PREDICTION_JOB_ID,
            context=context,
            task_instance=op,
        )
        assert hasattr(exception_info.value, "trigger")
        assert exception_info.value.trigger.conn_id == GCP_CONN_ID
        assert exception_info.value.trigger.project_id == GCP_PROJECT
        assert exception_info.value.trigger.location == GCP_LOCATION
        assert exception_info.value.trigger.job_id == TEST_BATCH_PREDICTION_JOB_ID
        assert exception_info.value.trigger.poll_interval == 10
        assert exception_info.value.trigger.impersonation_chain == IMPERSONATION_CHAIN

    @mock.patch(VERTEX_AI_PATH.format("batch_prediction_job.CreateBatchPredictionJobOperator.xcom_push"))
    @mock.patch(VERTEX_AI_PATH.format("batch_prediction_job.BatchPredictionJobHook"))
    def test_execute_complete(self, mock_hook, mock_xcom_push):
        context = mock.MagicMock()
        mock_job = {"name": TEST_JOB_DISPLAY_NAME}
        event = {
            "status": "success",
            "job": mock_job,
        }
        mock_hook.return_value.extract_batch_prediction_job_id.return_value = TEST_BATCH_PREDICTION_JOB_ID

        op = CreateBatchPredictionJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            job_display_name=TEST_JOB_DISPLAY_NAME,
            model_name=TEST_MODEL_NAME,
            instances_format="jsonl",
            predictions_format="jsonl",
            create_request_timeout=TEST_CREATE_REQUEST_TIMEOUT,
            batch_size=TEST_BATCH_SIZE,
        )
        execute_complete_result = op.execute_complete(context=context, event=event)

        mock_hook.return_value.extract_batch_prediction_job_id.assert_called_once_with(mock_job)
        mock_xcom_push.assert_has_calls(
            [
                call(context, key="batch_prediction_job_id", value=TEST_BATCH_PREDICTION_JOB_ID),
                call(
                    context,
                    key="training_conf",
                    value={
                        "training_conf_id": TEST_BATCH_PREDICTION_JOB_ID,
                        "region": GCP_LOCATION,
                        "project_id": GCP_PROJECT,
                    },
                ),
            ]
        )
        assert execute_complete_result == mock_job


class TestVertexAIDeleteBatchPredictionJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("batch_prediction_job.BatchPredictionJobHook"))
    def test_execute(self, mock_hook):
        op = DeleteBatchPredictionJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            batch_prediction_job_id=TEST_BATCH_PREDICTION_JOB_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_batch_prediction_job.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            batch_prediction_job=TEST_BATCH_PREDICTION_JOB_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIListBatchPredictionJobsOperator:
    @mock.patch(VERTEX_AI_PATH.format("batch_prediction_job.BatchPredictionJobHook"))
    def test_execute(self, mock_hook):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        read_mask = "read_mask"

        op = ListBatchPredictionJobsOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_batch_prediction_jobs.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAICreateEndpointOperator:
    @mock.patch(VERTEX_AI_PATH.format("endpoint_service.Endpoint.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("endpoint_service.EndpointServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = CreateEndpointOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint=TEST_ENDPOINT,
            endpoint_id=TEST_ENDPOINT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_endpoint.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint=TEST_ENDPOINT,
            endpoint_id=TEST_ENDPOINT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIDeleteEndpointOperator:
    @mock.patch(VERTEX_AI_PATH.format("endpoint_service.EndpointServiceHook"))
    def test_execute(self, mock_hook):
        op = DeleteEndpointOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint_id=TEST_ENDPOINT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_endpoint.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint=TEST_ENDPOINT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIDeployModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("endpoint_service.endpoint_service.DeployModelResponse.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("endpoint_service.EndpointServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = DeployModelOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint_id=TEST_ENDPOINT_ID,
            deployed_model=TEST_DEPLOYED_MODEL,
            traffic_split=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.deploy_model.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint=TEST_ENDPOINT_ID,
            deployed_model=TEST_DEPLOYED_MODEL,
            traffic_split=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIListEndpointsOperator:
    @mock.patch(VERTEX_AI_PATH.format("endpoint_service.EndpointServiceHook"))
    def test_execute(self, mock_hook):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        read_mask = "read_mask"
        order_by = "order_by"

        op = ListEndpointsOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_endpoints.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIUndeployModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("endpoint_service.EndpointServiceHook"))
    def test_execute(self, mock_hook):
        op = UndeployModelOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint_id=TEST_ENDPOINT_ID,
            deployed_model_id=TEST_DEPLOYED_MODEL_ID,
            traffic_split=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.undeploy_model.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            endpoint=TEST_ENDPOINT_ID,
            deployed_model_id=TEST_DEPLOYED_MODEL_ID,
            traffic_split=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAICreateHyperparameterTuningJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.types.HyperparameterTuningJob.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.HyperparameterTuningJobHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = CreateHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            worker_pool_specs=[],
            sync=False,
            parameter_spec={},
            metric_spec={},
            max_trial_count=15,
            parallel_trial_count=3,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match=SYNC_DEPRECATION_WARNING.format("01.09.2024"),
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_hyperparameter_tuning_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            metric_spec={},
            parameter_spec={},
            max_trial_count=15,
            parallel_trial_count=3,
            worker_pool_specs=[],
            base_output_dir=None,
            custom_job_labels=None,
            custom_job_encryption_spec_key_name=None,
            staging_bucket=STAGING_BUCKET,
            max_failed_trial_count=0,
            search_algorithm=None,
            measurement_selection="best",
            hyperparameter_tuning_job_labels=None,
            hyperparameter_tuning_job_encryption_spec_key_name=None,
            service_account=None,
            network=None,
            timeout=None,
            restart_job_on_worker_restart=False,
            enable_web_access=False,
            tensorboard=None,
            sync=False,
            wait_job_completed=False,
        )

    @mock.patch(
        VERTEX_AI_PATH.format("hyperparameter_tuning_job.CreateHyperparameterTuningJobOperator.defer")
    )
    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.HyperparameterTuningJobHook"))
    def test_deferrable(self, mock_hook, mock_defer):
        op = CreateHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            worker_pool_specs=[],
            sync=False,
            parameter_spec={},
            metric_spec={},
            max_trial_count=15,
            parallel_trial_count=3,
            deferrable=True,
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match=SYNC_DEPRECATION_WARNING.format("01.09.2024"),
        ):
            op.execute(context={"ti": mock.MagicMock()})
        mock_defer.assert_called_once()

    @pytest.mark.db_test
    def test_deferrable_sync_error(self):
        op = CreateHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            worker_pool_specs=[],
            sync=True,
            parameter_spec={},
            metric_spec={},
            max_trial_count=15,
            parallel_trial_count=3,
            deferrable=True,
        )
        with (
            pytest.raises(AirflowException),
            pytest.warns(
                AirflowProviderDeprecationWarning,
                match=SYNC_DEPRECATION_WARNING.format("01.09.2024"),
            ),
        ):
            op.execute(context={"ti": mock.MagicMock()})

    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.HyperparameterTuningJobHook"))
    def test_execute_complete(self, mock_hook):
        test_job_id = "test_job_id"
        test_job = {"name": f"test/{test_job_id}"}
        event = {
            "status": "success",
            "message": "test message",
            "job": test_job,
        }
        mock_hook.return_value.extract_hyperparameter_tuning_job_id.return_value = test_job_id
        mock_context = mock.MagicMock()

        op = CreateHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            worker_pool_specs=[],
            sync=False,
            parameter_spec={},
            metric_spec={},
            max_trial_count=15,
            parallel_trial_count=3,
        )

        result = op.execute_complete(context=mock_context, event=event)

        assert result == test_job

    def test_execute_complete_error(self):
        event = {
            "status": "error",
            "message": "test error message",
        }

        op = CreateHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            worker_pool_specs=[],
            sync=False,
            parameter_spec={},
            metric_spec={},
            max_trial_count=15,
            parallel_trial_count=3,
        )

        with pytest.raises(AirflowException):
            op.execute_complete(context=mock.MagicMock(), event=event)


class TestVertexAIGetHyperparameterTuningJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.types.HyperparameterTuningJob.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.HyperparameterTuningJobHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = GetHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            hyperparameter_tuning_job_id=TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_hyperparameter_tuning_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            hyperparameter_tuning_job=TEST_HYPERPARAMETER_TUNING_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestVertexAIDeleteHyperparameterTuningJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.HyperparameterTuningJobHook"))
    def test_execute(self, mock_hook):
        op = DeleteHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            hyperparameter_tuning_job_id=TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_hyperparameter_tuning_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            hyperparameter_tuning_job=TEST_HYPERPARAMETER_TUNING_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestVertexAIListHyperparameterTuningJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("hyperparameter_tuning_job.HyperparameterTuningJobHook"))
    def test_execute(self, mock_hook):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        read_mask = "read_mask"

        op = ListHyperparameterTuningJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_hyperparameter_tuning_jobs.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            read_mask=read_mask,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIExportModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook):
        op = ExportModelOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_ID,
            output_config=TEST_OUTPUT_CONFIG,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.export_model.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model=TEST_MODEL_ID,
            output_config=TEST_OUTPUT_CONFIG,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIDeleteModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook):
        op = DeleteModelOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_model.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model=TEST_MODEL_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIListModelsOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.Model.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        read_mask = "read_mask"
        order_by = "order_by"

        op = ListModelsOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            filter=filter,
            page_size=page_size,
            page_token=page_token,
            read_mask=read_mask,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_models.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            filter=filter,
            page_size=page_size,
            page_token=page_token,
            read_mask=read_mask,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIUploadModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.model_service.UploadModelResponse.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = UploadModelOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model=TEST_MODEL_OBJ,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.upload_model.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model=TEST_MODEL_OBJ,
            parent_model=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(VERTEX_AI_PATH.format("model_service.model_service.UploadModelResponse.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute_with_parent_model(self, mock_hook, to_dict_mock):
        op = UploadModelOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model=TEST_MODEL_OBJ,
            parent_model=TEST_PARENT_MODEL,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.upload_model.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model=TEST_MODEL_OBJ,
            parent_model=TEST_PARENT_MODEL,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIGetModelsOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.Model.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = GetModelOperator(
            task_id=TASK_ID,
            model_id=TEST_MODEL_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_model.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_NAME,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIListModelVersionsOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.Model.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ListModelVersionsOperator(
            task_id=TASK_ID,
            model_id=TEST_MODEL_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_model_versions.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_NAME,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAISetDefaultVersionOnModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.Model.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = SetDefaultVersionOnModelOperator(
            task_id=TASK_ID,
            model_id=TEST_MODEL_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.set_version_as_default.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_NAME,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIAddVersionAliasesOnModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.Model.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = AddVersionAliasesOnModelOperator(
            task_id=TASK_ID,
            model_id=TEST_MODEL_NAME,
            version_aliases=TEST_VERSION_ALIASES,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.add_version_aliases.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_NAME,
            version_aliases=TEST_VERSION_ALIASES,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIDeleteVersionAliasesOnModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.Model.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = DeleteVersionAliasesOnModelOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_ID,
            version_aliases=TEST_VERSION_ALIASES,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_version_aliases.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_ID,
            version_aliases=TEST_VERSION_ALIASES,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIDeleteModelVersionOperator:
    @mock.patch(VERTEX_AI_PATH.format("model_service.Model.to_dict"))
    @mock.patch(VERTEX_AI_PATH.format("model_service.ModelServiceHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = DeleteModelVersionOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_model_version.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            model_id=TEST_MODEL_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestVertexAIRunPipelineJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("pipeline_job.PipelineJobHook"))
    @mock.patch("google.cloud.aiplatform_v1.types.PipelineJob.to_dict")
    def test_execute(self, to_dict_mock, mock_hook):
        op = RunPipelineJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            display_name=DISPLAY_NAME,
            template_path=TEST_TEMPLATE_PATH,
            job_id=TEST_PIPELINE_JOB_ID,
            pipeline_root="",
            parameter_values={},
            input_artifacts={},
            enable_caching=False,
            encryption_spec_key_name="",
            labels={},
            failure_policy="",
            service_account="",
            network="",
            create_request_timeout=None,
            experiment=None,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.submit_pipeline_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            display_name=DISPLAY_NAME,
            template_path=TEST_TEMPLATE_PATH,
            job_id=TEST_PIPELINE_JOB_ID,
            pipeline_root="",
            parameter_values={},
            input_artifacts={},
            enable_caching=False,
            encryption_spec_key_name="",
            labels={},
            failure_policy="",
            service_account="",
            network="",
            create_request_timeout=None,
            experiment=None,
        )

    @mock.patch(VERTEX_AI_PATH.format("pipeline_job.PipelineJobHook"))
    def test_execute_enters_deferred_state(self, mock_hook):
        task = RunPipelineJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            display_name=DISPLAY_NAME,
            template_path=TEST_TEMPLATE_PATH,
            job_id=TEST_PIPELINE_JOB_ID,
            deferrable=True,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context={"ti": mock.MagicMock()})
        assert isinstance(exc.value.trigger, RunPipelineJobTrigger), "Trigger is not a RunPipelineJobTrigger"

    @mock.patch(VERTEX_AI_PATH.format("pipeline_job.RunPipelineJobOperator.xcom_push"))
    @mock.patch(VERTEX_AI_PATH.format("pipeline_job.PipelineJobHook"))
    def test_execute_complete_success(self, mock_hook, mock_xcom_push):
        task = RunPipelineJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            display_name=DISPLAY_NAME,
            template_path=TEST_TEMPLATE_PATH,
            job_id=TEST_PIPELINE_JOB_ID,
            deferrable=True,
        )
        expected_pipeline_job = expected_result = {
            "name": f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/pipelineJobs/{TEST_PIPELINE_JOB_ID}",
        }
        mock_hook.return_value.exists.return_value = False
        mock_xcom_push.return_value = None
        actual_result = task.execute_complete(
            context=None, event={"status": "success", "message": "", "job": expected_pipeline_job}
        )
        assert actual_result == expected_result

    def test_execute_complete_error_status_raises_exception(self):
        task = RunPipelineJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            display_name=DISPLAY_NAME,
            template_path=TEST_TEMPLATE_PATH,
            job_id=TEST_PIPELINE_JOB_ID,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context=None, event={"status": "error", "message": "test message", "job": None}
            )


class TestVertexAIGetPipelineJobOperator:
    @mock.patch("google.cloud.aiplatform_v1.types.PipelineJob.to_dict")
    @mock.patch(VERTEX_AI_PATH.format("pipeline_job.PipelineJobHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = GetPipelineJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_pipeline_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestVertexAIDeletePipelineJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("pipeline_job.PipelineJobHook"))
    def test_execute(self, mock_hook):
        op = DeletePipelineJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_pipeline_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            region=GCP_LOCATION,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestVertexAIListPipelineJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("pipeline_job.PipelineJobHook"))
    def test_execute(self, mock_hook):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        order_by = "order_by"

        op = ListPipelineJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_pipeline_jobs.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
