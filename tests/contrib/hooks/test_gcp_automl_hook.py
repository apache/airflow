# -*- coding: utf-8 -*-
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
#
import unittest

from google.cloud.automl_v1beta1 import AutoMlClient, PredictionServiceClient

from airflow import version
from airflow.contrib.hooks.gcp_automl_hook import AutoMLHook
from tests.compat import mock

CREDENTIALS = "test-creds"
TASK_ID = "test-automl-hook"
GCP_PROJECT_ID = "test-project"
GCP_LOCATION = "test-location"
MODEL_NAME = "test_model"
MODEL_ID = "projects/198907790164/locations/us-central1/models/TBL9195602771183665152"
DATASET_ID = "TBL123456789"
MODEL = {
    "display_name": MODEL_NAME,
    "dataset_id": DATASET_ID,
    "tables_model_metadata": {"train_budget_milli_node_hours": 1000},
}

LOCATION_PATH = AutoMlClient.location_path(GCP_PROJECT_ID, GCP_LOCATION)
MODEL_PATH = PredictionServiceClient.model_path(GCP_PROJECT_ID, GCP_LOCATION, MODEL_ID)
DATASET_PATH = AutoMlClient.dataset_path(GCP_PROJECT_ID, GCP_LOCATION, DATASET_ID)

INPUT_CONFIG = {"input": "value"}
OUTPUT_CONFIG = {"output": "value"}
PAYLOAD = {"test": "payload"}
DATASET = {"dataset_id": "data"}
MASK = {"field": "mask"}


class TestAuoMLHook(unittest.TestCase):
    def setUp(self) -> None:
        self.hook = AutoMLHook()
        self.hook._get_credentials = mock.MagicMock(  # type: ignore
            return_value=CREDENTIALS
        )

    def test_version_information(self):
        expected_version = "airflow_v" + version.version
        self.assertEqual(
            expected_version, self.hook._client_info.client_library_version
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.PredictionServiceClient")
    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient")
    def test_get_conn(self, mock_automl_client, mock_prediction_client):
        self.hook.get_conn()
        mock_automl_client.assert_called_with(
            credentials=CREDENTIALS, client_info=self.hook._client_info
        )

        client = self.hook.prediction_client  # pylint:disable=unused-variable  # noqa
        mock_prediction_client.assert_called_with(
            credentials=CREDENTIALS, client_info=self.hook._client_info
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.create_model")
    def test_create_model(self, service_mock):
        self.hook.create_model(
            model=MODEL, location=GCP_LOCATION, project_id=GCP_PROJECT_ID
        )

        service_mock.assert_called_with(
            parent=LOCATION_PATH, model=MODEL, retry=None, timeout=None, metadata=None
        )

    @mock.patch(
        "airflow.contrib.hooks.gcp_automl_hook.PredictionServiceClient.batch_predict"
    )
    def test_batch_predict(self, service_mock):
        self.hook.batch_predict(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            input_config=INPUT_CONFIG,
            output_config=OUTPUT_CONFIG,
        )

        service_mock.assert_called_with(
            name=MODEL_PATH,
            input_config=INPUT_CONFIG,
            output_config=OUTPUT_CONFIG,
            params=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.PredictionServiceClient.predict")
    def test_predict(self, service_mock):
        self.hook.predict(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            payload=PAYLOAD,
        )

        service_mock.assert_called_with(
            name=MODEL_PATH,
            payload=PAYLOAD,
            params=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.create_dataset")
    def test_create_dataset(self, service_mock):
        self.hook.create_dataset(
            dataset=DATASET, location=GCP_LOCATION, project_id=GCP_PROJECT_ID
        )

        service_mock.assert_called_with(
            parent=LOCATION_PATH,
            dataset=DATASET,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.import_data")
    def test_import_dataset(self, service_mock):
        self.hook.import_data(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            input_config=INPUT_CONFIG,
        )

        service_mock.assert_called_with(
            name=DATASET_PATH,
            input_config=INPUT_CONFIG,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.list_column_specs")
    def test_list_column_specs(self, service_mock):
        table_spec = "table_spec_id"
        filter_ = "filter"
        page_size = 42

        self.hook.list_column_specs(
            dataset_id=DATASET_ID,
            table_spec_id=table_spec,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            field_mask=MASK,
            filter_=filter_,
            page_size=page_size,
        )

        parent = AutoMlClient.table_spec_path(
            GCP_PROJECT_ID, GCP_LOCATION, DATASET_ID, table_spec
        )
        service_mock.assert_called_with(
            parent=parent,
            field_mask=MASK,
            filter_=filter_,
            page_size=page_size,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.get_model")
    def test_get_model(self, service_mock):
        self.hook.get_model(
            model_id=MODEL_ID, location=GCP_LOCATION, project_id=GCP_PROJECT_ID
        )

        service_mock.assert_called_with(
            name=MODEL_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.delete_model")
    def test_delete_model(self, service_mock):
        self.hook.delete_model(
            model_id=MODEL_ID, location=GCP_LOCATION, project_id=GCP_PROJECT_ID
        )

        service_mock.assert_called_with(
            name=MODEL_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.update_dataset")
    def test_update_dataset(self, service_mock):
        self.hook.update_dataset(
            dataset=DATASET, update_mask=MASK, project_id=GCP_PROJECT_ID
        )

        service_mock.assert_called_with(
            dataset=DATASET, update_mask=MASK, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.deploy_model")
    def test_deploy_model(self, service_mock):
        image_detection_metadata = {}

        self.hook.deploy_model(
            model_id=MODEL_ID,
            image_detection_metadata=image_detection_metadata,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
        )

        service_mock.assert_called_with(
            name=MODEL_PATH,
            retry=None,
            timeout=None,
            metadata=None,
            image_object_detection_model_deployment_metadata=image_detection_metadata,
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.list_table_specs")
    def test_list_table_specs(self, service_mock):
        filter_ = "filter"
        page_size = 42

        self.hook.list_table_specs(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            filter_=filter_,
            page_size=page_size,
        )

        service_mock.assert_called_with(
            parent=DATASET_PATH,
            filter_=filter_,
            page_size=page_size,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.list_datasets")
    def test_list_datasets(self, service_mock):
        self.hook.list_datasets(location=GCP_LOCATION, project_id=GCP_PROJECT_ID)

        service_mock.assert_called_with(
            parent=LOCATION_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch("airflow.contrib.hooks.gcp_automl_hook.AutoMlClient.delete_dataset")
    def test_delete_dataset(self, service_mock):
        self.hook.delete_dataset(
            dataset_id=DATASET_ID, location=GCP_LOCATION, project_id=GCP_PROJECT_ID
        )

        service_mock.assert_called_with(
            name=DATASET_PATH, retry=None, timeout=None, metadata=None
        )
