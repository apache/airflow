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

from unittest import mock

from google.api_core.retry import Retry

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

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"
TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
DELEGATE_TO = "test-delegate-to"
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
COMMAND_2 = ['echo', 'Hello World']

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
        "gcs_source": {"uris": ['test-string']},
    },
    {},
]
TEST_UPDATE_MASK = "test-update-mask"


class TestVertexAICreateCustomContainerTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook):
        op = CreateCustomContainerTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
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
        )
        op.execute(context={'ti': mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
        mock_hook.return_value.create_custom_container_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            command=COMMAND_2,
            dataset=None,
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
            boot_disk_type='pd-ssd',
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
        )


class TestVertexAICreateCustomPythonPackageTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook):
        op = CreateCustomPythonPackageTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
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
        )
        op.execute(context={'ti': mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
        mock_hook.return_value.create_custom_python_package_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=ARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            python_package_gcs_uri=PYTHON_PACKAGE_GCS_URI,
            python_module_name=PYTHON_MODULE_NAME,
            dataset=None,
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
            boot_disk_type='pd-ssd',
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
        )


class TestVertexAICreateCustomTrainingJobOperator:
    @mock.patch(VERTEX_AI_PATH.format("custom_job.CustomJobHook"))
    def test_execute(self, mock_hook):
        op = CreateCustomTrainingJobOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
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
        )
        op.execute(context={'ti': mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
        mock_hook.return_value.create_custom_training_job.assert_called_once_with(
            staging_bucket=STAGING_BUCKET,
            display_name=DISPLAY_NAME,
            args=PYTHON_PACKAGE_CMDARGS,
            container_uri=CONTAINER_URI,
            model_serving_container_image_uri=CONTAINER_URI,
            script_path=PYTHON_PACKAGE,
            requirements=[],
            dataset=None,
            model_display_name=None,
            replica_count=REPLICA_COUNT,
            machine_type=MACHINE_TYPE,
            accelerator_type=ACCELERATOR_TYPE,
            accelerator_count=ACCELERATOR_COUNT,
            training_fraction_split=None,
            validation_fraction_split=None,
            test_fraction_split=None,
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
            boot_disk_type='pd-ssd',
            boot_disk_size_gb=100,
            training_filter_split=None,
            validation_filter_split=None,
            test_filter_split=None,
            predefined_split_column_name=None,
            timestamp_split_column_name=None,
            tensorboard=None,
            sync=True,
        )


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
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
            delegate_to=DELEGATE_TO,
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
        op.execute(context={'ti': mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset=TEST_DATASET,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={'ti': mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataset_id=TEST_DATASET_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
            delegate_to=DELEGATE_TO,
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
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
            delegate_to=DELEGATE_TO,
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
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
            delegate_to=DELEGATE_TO,
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
        op.execute(context={'ti': mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
            delegate_to=DELEGATE_TO,
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
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, delegate_to=DELEGATE_TO, impersonation_chain=IMPERSONATION_CHAIN
        )
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
