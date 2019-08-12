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
import copy

from google.cloud.automl_v1beta1 import AutoMlClient, PredictionServiceClient

from airflow.gcp.operators.automl import (
    AutoMLTrainModelOperator,
    AutoMLPredictOperator,
    AutoMLBatchPredictOperator,
    AutoMLCreateDatasetOperator,
    AutoMLImportDataOperator,
    AutoMLGetModelOperator,
    AutoMLTablesListColumnSpecsOperator,
    AutoMLDeployModelOperator,
    AutoMLTablesUpdateDatasetOperator,
    AutoMLTablesListTableSpecsOperator,
    AutoMLListDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLDeleteModelOperator,
)
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

INPUT_CONFIG = {"input": "value"}
OUTPUT_CONFIG = {"output": "value"}
PAYLOAD = {"test": "payload"}
DATASET = {"dataset_id": "data"}
MASK = {"field": "mask"}


class TestAutoMLTrainModelOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.operators.automl.AutoMLTrainModelOperator.xcom_push"
    )
    @mock.patch("airflow.gcp.hooks.automl.CloudAutoMLHook.create_model")
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.extract_object_id",
        return_value=MODEL_ID,
    )
    def test_execute(self, mock_extract_object_id, mock_create_model, mock_xcom):
        op = AutoMLTrainModelOperator(
            model=MODEL,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_create_model.assert_called_once_with(
            model=MODEL,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
            metadata=None,
        )
        mock_xcom.assert_called_once_with(None, key="model_id", value=MODEL_ID)


class TestAutoMLBatchPredictOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.batch_predict"
    )
    def test_execute(self, mock_batch_predict):
        op = AutoMLBatchPredictOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            input_config=INPUT_CONFIG,
            output_config=OUTPUT_CONFIG,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_batch_predict.assert_called_once_with(
            input_config=INPUT_CONFIG,
            location=GCP_LOCATION,
            metadata=None,
            model_id=MODEL_ID,
            output_config=OUTPUT_CONFIG,
            params={},
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )


class TestAutoMLPredictOperator(unittest.TestCase):
    @mock.patch("airflow.gcp.hooks.automl.CloudAutoMLHook.predict")
    def test_execute(self, mock_predict):
        op = AutoMLPredictOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            payload=PAYLOAD,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_predict.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=None,
            model_id=MODEL_ID,
            params={},
            payload=PAYLOAD,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )


class TestAutoMLCreateImportOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.operators.automl.AutoMLCreateDatasetOperator.xcom_push"
    )
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.create_dataset"
    )
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.extract_object_id",
        return_value=DATASET_ID,
    )
    def test_execute(self, mock_extract_object_id, mock_create_dataset, mock_xcom):
        op = AutoMLCreateDatasetOperator(
            dataset=DATASET,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_create_dataset.assert_called_once_with(
            dataset=DATASET,
            location=GCP_LOCATION,
            metadata=None,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )
        mock_xcom.assert_called_once_with(None, key="dataset_id", value=DATASET_ID)


class TestAutoMLListColumnsSpecsOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.list_column_specs"
    )
    def test_execute(self, mock_list_column_specs):
        table_spec = "table_spec_id"
        filter_ = "filter"
        page_size = 42

        op = AutoMLTablesListColumnSpecsOperator(
            dataset_id=DATASET_ID,
            table_spec_id=table_spec,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            field_mask=MASK,
            filter_=filter_,
            page_size=page_size,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_list_column_specs.assert_called_once_with(
            dataset_id=DATASET_ID,
            field_mask=MASK,
            filter_=filter_,
            location=GCP_LOCATION,
            metadata=None,
            page_size=page_size,
            project_id=GCP_PROJECT_ID,
            retry=None,
            table_spec_id=table_spec,
            timeout=None,
        )


class TestAutoMLUpdateDatasetOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.update_dataset"
    )
    def test_execute(self, mock_update_dataset):
        dataset = copy.deepcopy(DATASET)
        dataset["name"] = DATASET_ID

        op = AutoMLTablesUpdateDatasetOperator(
            dataset=dataset,
            update_mask=MASK,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_update_dataset.assert_called_once_with(
            dataset=dataset,
            metadata=None,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
            update_mask=MASK,
        )


class TestAutoMLGetModelOperator(unittest.TestCase):
    @mock.patch("airflow.gcp.hooks.automl.CloudAutoMLHook.get_model")
    def test_execute(self, mock_get_model):
        op = AutoMLGetModelOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_get_model.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=None,
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )


class TestAutoMLDeleteModelOperator(unittest.TestCase):
    @mock.patch("airflow.gcp.hooks.automl.CloudAutoMLHook.delete_model")
    def test_execute(self, mock_delete_model):
        op = AutoMLDeleteModelOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_delete_model.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=None,
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )


class TestAutoMLDeployModelOperator(unittest.TestCase):
    @mock.patch("airflow.gcp.hooks.automl.CloudAutoMLHook.deploy_model")
    def test_execute(self, mock_deploy_model):
        image_detection_metadata = {}
        op = AutoMLDeployModelOperator(
            model_id=MODEL_ID,
            image_detection_metadata=image_detection_metadata,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_deploy_model.assert_called_once_with(
            image_detection_metadata={},
            location=GCP_LOCATION,
            metadata=None,
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )


class TestAutoMLDatasetImportOperator(unittest.TestCase):
    @mock.patch("airflow.gcp.hooks.automl.CloudAutoMLHook.import_data")
    def test_execute(self, mock_import_data):
        op = AutoMLImportDataOperator(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            input_config=INPUT_CONFIG,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_import_data.assert_called_once_with(
            input_config=INPUT_CONFIG,
            location=GCP_LOCATION,
            metadata=None,
            dataset_id=DATASET_ID,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )


class TestAutoMLTablesListTableSpecsOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.list_table_specs"
    )
    def test_execute(self, mock_list_table_specs):
        filter_ = "filter"
        page_size = 42

        op = AutoMLTablesListTableSpecsOperator(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            filter_=filter_,
            page_size=page_size,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_list_table_specs.assert_called_once_with(
            dataset_id=DATASET_ID,
            filter_=filter_,
            location=GCP_LOCATION,
            metadata=None,
            page_size=page_size,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )


class TestAutoMLDatasetListOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.operators.automl.AutoMLListDatasetOperator.xcom_push"
    )
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.list_datasets"
    )
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.extract_object_id"
    )
    def test_execute(self, mock_extract_object_id, mock_list_databases, mock_xcom):
        op = AutoMLListDatasetOperator(
            location=GCP_LOCATION, project_id=GCP_PROJECT_ID, task_id=TASK_ID
        )
        op.execute(context=None)
        mock_list_databases.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=None,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )
        mock_xcom.assert_called_once_with(None, key="dataset_id_list", value=[])


class TestAutoMLDatasetDeleteOperator(unittest.TestCase):
    @mock.patch(
        "airflow.gcp.hooks.automl.CloudAutoMLHook.delete_dataset"
    )
    def test_execute(self, mock_delete_dataset):
        op = AutoMLDeleteDatasetOperator(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_delete_dataset.assert_called_once_with(
            location=GCP_LOCATION,
            dataset_id=DATASET_ID,
            metadata=None,
            project_id=GCP_PROJECT_ID,
            retry=None,
            timeout=None,
        )
