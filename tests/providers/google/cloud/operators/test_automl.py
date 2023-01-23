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
from __future__ import annotations

import copy
import unittest
from unittest import mock

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.automl_v1beta1 import BatchPredictResult, Dataset, Model, PredictResponse

from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.operators.automl import (
    AutoMLBatchPredictOperator,
    AutoMLCreateDatasetOperator,
    AutoMLDeleteDatasetOperator,
    AutoMLDeleteModelOperator,
    AutoMLDeployModelOperator,
    AutoMLGetModelOperator,
    AutoMLImportDataOperator,
    AutoMLListDatasetOperator,
    AutoMLPredictOperator,
    AutoMLTablesListColumnSpecsOperator,
    AutoMLTablesListTableSpecsOperator,
    AutoMLTablesUpdateDatasetOperator,
    AutoMLTrainModelOperator,
)

CREDENTIALS = "test-creds"
TASK_ID = "test-automl-hook"
GCP_PROJECT_ID = "test-project"
GCP_LOCATION = "test-location"
MODEL_NAME = "test_model"
MODEL_ID = "TBL9195602771183665152"
DATASET_ID = "TBL123456789"
MODEL = {
    "display_name": MODEL_NAME,
    "dataset_id": DATASET_ID,
    "tables_model_metadata": {"train_budget_milli_node_hours": 1000},
}

LOCATION_PATH = f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}"
MODEL_PATH = f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/models/{MODEL_ID}"
DATASET_PATH = f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/datasets/{DATASET_ID}"

INPUT_CONFIG = {"input": "value"}
OUTPUT_CONFIG = {"output": "value"}
PAYLOAD = {"test": "payload"}
DATASET = {"dataset_id": "data"}
MASK = {"field": "mask"}

extract_object_id = CloudAutoMLHook.extract_object_id


class TestAutoMLTrainModelOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.create_model.return_value.result.return_value = Model(name=MODEL_PATH)
        mock_hook.return_value.extract_object_id = extract_object_id
        op = AutoMLTrainModelOperator(
            model=MODEL,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.create_model.assert_called_once_with(
            model=MODEL,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestAutoMLBatchPredictOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.batch_predict.return_value.result.return_value = BatchPredictResult()
        mock_hook.return_value.extract_object_id = extract_object_id

        op = AutoMLBatchPredictOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            input_config=INPUT_CONFIG,
            output_config=OUTPUT_CONFIG,
            task_id=TASK_ID,
            prediction_params={},
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.batch_predict.assert_called_once_with(
            input_config=INPUT_CONFIG,
            location=GCP_LOCATION,
            metadata=(),
            model_id=MODEL_ID,
            output_config=OUTPUT_CONFIG,
            params={},
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLPredictOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.predict.return_value = PredictResponse()

        op = AutoMLPredictOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            payload=PAYLOAD,
            task_id=TASK_ID,
            operation_params={"TEST_KEY": "TEST_VALUE"},
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.predict.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=(),
            model_id=MODEL_ID,
            params={"TEST_KEY": "TEST_VALUE"},
            payload=PAYLOAD,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLCreateImportOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.create_dataset.return_value = Dataset(name=DATASET_PATH)
        mock_hook.return_value.extract_object_id = extract_object_id

        op = AutoMLCreateDatasetOperator(
            dataset=DATASET,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.create_dataset.assert_called_once_with(
            dataset=DATASET,
            location=GCP_LOCATION,
            metadata=(),
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLListColumnsSpecsOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
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
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.list_column_specs.assert_called_once_with(
            dataset_id=DATASET_ID,
            field_mask=MASK,
            filter_=filter_,
            location=GCP_LOCATION,
            metadata=(),
            page_size=page_size,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            table_spec_id=table_spec,
            timeout=None,
        )


class TestAutoMLUpdateDatasetOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.update_dataset.return_value = Dataset(name=DATASET_PATH)

        dataset = copy.deepcopy(DATASET)
        dataset["name"] = DATASET_ID

        op = AutoMLTablesUpdateDatasetOperator(
            dataset=dataset,
            update_mask=MASK,
            location=GCP_LOCATION,
            task_id=TASK_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.update_dataset.assert_called_once_with(
            dataset=dataset,
            metadata=(),
            retry=DEFAULT,
            timeout=None,
            update_mask=MASK,
        )


class TestAutoMLGetModelOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.get_model.return_value = Model(name=MODEL_PATH)
        mock_hook.return_value.extract_object_id = extract_object_id

        op = AutoMLGetModelOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_model.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=(),
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLDeleteModelOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        op = AutoMLDeleteModelOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_hook.return_value.delete_model.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=(),
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLDeployModelOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        image_detection_metadata = {}
        op = AutoMLDeployModelOperator(
            model_id=MODEL_ID,
            image_detection_metadata=image_detection_metadata,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_hook.return_value.deploy_model.assert_called_once_with(
            image_detection_metadata={},
            location=GCP_LOCATION,
            metadata=(),
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLDatasetImportOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        op = AutoMLImportDataOperator(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            input_config=INPUT_CONFIG,
            task_id=TASK_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.import_data.assert_called_once_with(
            input_config=INPUT_CONFIG,
            location=GCP_LOCATION,
            metadata=(),
            dataset_id=DATASET_ID,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLTablesListTableSpecsOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
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
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.list_table_specs.assert_called_once_with(
            dataset_id=DATASET_ID,
            filter_=filter_,
            location=GCP_LOCATION,
            metadata=(),
            page_size=page_size,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLDatasetListOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        op = AutoMLListDatasetOperator(location=GCP_LOCATION, project_id=GCP_PROJECT_ID, task_id=TASK_ID)
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.list_datasets.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=(),
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestAutoMLDatasetDeleteOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        op = AutoMLDeleteDatasetOperator(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        op.execute(context=None)
        mock_hook.return_value.delete_dataset.assert_called_once_with(
            location=GCP_LOCATION,
            dataset_id=DATASET_ID,
            metadata=(),
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )
