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
from unittest import mock

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.automl_v1beta1 import BatchPredictResult, Dataset, Model, PredictResponse

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
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
    AutoMLUpdateDatasetOperator,
)
from airflow.utils import timezone

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
    "translation_model_metadata": {"base_model": "some_base_model"},
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


class TestAutoMLTrainModelOperator:
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.wait_for_operation.return_value = Model()
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

    def test_execute_deprecated_model_types(self):
        invalid_model = MODEL.copy()
        invalid_model.pop("translation_model_metadata")
        op = AutoMLTrainModelOperator(
            model=invalid_model,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        with pytest.raises(AirflowException):
            op.execute(context=mock.MagicMock())

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLTrainModelOperator,
            # Templated fields
            model="{{ 'model' }}",
            location="{{ 'location' }}",
            impersonation_chain="{{ 'impersonation_chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLTrainModelOperator = ti.task
        assert task.model == "model"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation_chain"


class TestAutoMLBatchPredictOperator:
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.batch_predict.return_value.result.return_value = BatchPredictResult()
        mock_hook.return_value.extract_object_id = extract_object_id
        mock_hook.return_value.wait_for_operation.return_value = BatchPredictResult()

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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLBatchPredictOperator,
            # Templated fields
            model_id="{{ 'model' }}",
            input_config="{{ 'input-config' }}",
            output_config="{{ 'output-config' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLBatchPredictOperator = ti.task
        assert task.model_id == "model"
        assert task.input_config == "input-config"
        assert task.output_config == "output-config"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLPredictOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLPredictOperator,
            # Templated fields
            model_id="{{ 'model-id' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
            payload={},
        )
        ti.render_templates()
        task: AutoMLPredictOperator = ti.task
        assert task.model_id == "model-id"
        assert task.project_id == "project-id"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLCreateImportOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLCreateDatasetOperator,
            # Templated fields
            dataset="{{ 'dataset' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLCreateDatasetOperator = ti.task
        assert task.dataset == "dataset"
        assert task.project_id == "project-id"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLListColumnsSpecsOperator:
    def test_deprecation(self):
        with pytest.raises(AirflowException):
            AutoMLTablesListColumnSpecsOperator(
                dataset_id=DATASET_ID,
                table_spec_id="table_spec",
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                field_mask=MASK,
                filter_="filter_",
                page_size=10,
                task_id=TASK_ID,
            )


class TestAutoMLUpdateDatasetOperator:
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.update_dataset.return_value = Dataset(name=DATASET_PATH)

        dataset = copy.deepcopy(DATASET)
        dataset["name"] = DATASET_ID

        op = AutoMLUpdateDatasetOperator(
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLUpdateDatasetOperator,
            # Templated fields
            dataset="{{ 'dataset' }}",
            update_mask="{{ 'update-mask' }}",
            location="{{ 'location' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLUpdateDatasetOperator = ti.task
        assert task.dataset == "dataset"
        assert task.update_mask == "update-mask"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLTablesUpdateDatasetOperator:
    def test_deprecation(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            AutoMLTablesUpdateDatasetOperator(
                dataset={},
                update_mask=MASK,
                location=GCP_LOCATION,
                task_id=TASK_ID,
            )

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.update_dataset.return_value = Dataset(name=DATASET_PATH)

        dataset = copy.deepcopy(DATASET)
        dataset["name"] = DATASET_ID

        op = AutoMLUpdateDatasetOperator(
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLUpdateDatasetOperator,
            # Templated fields
            dataset="{{ 'dataset' }}",
            update_mask="{{ 'update-mask' }}",
            location="{{ 'location' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLUpdateDatasetOperator = ti.task
        assert task.dataset == "dataset"
        assert task.update_mask == "update-mask"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLGetModelOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLGetModelOperator,
            # Templated fields
            model_id="{{ 'model-id' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLGetModelOperator = ti.task
        assert task.model_id == "model-id"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLDeleteModelOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLDeleteModelOperator,
            # Templated fields
            model_id="{{ 'model-id' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLDeleteModelOperator = ti.task
        assert task.model_id == "model-id"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLDeployModelOperator:
    def test_deprecation(self):
        with pytest.raises(AirflowException):
            AutoMLDeployModelOperator(
                model_id=MODEL_ID,
                image_detection_metadata={},
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                task_id=TASK_ID,
            )


class TestAutoMLDatasetImportOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLImportDataOperator,
            # Templated fields
            dataset_id="{{ 'dataset-id' }}",
            input_config="{{ 'input-config' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLImportDataOperator = ti.task
        assert task.dataset_id == "dataset-id"
        assert task.input_config == "input-config"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLTablesListTableSpecsOperator:
    def test_deprecation(self):
        with pytest.raises(AirflowException):
            AutoMLTablesListTableSpecsOperator(
                dataset_id=DATASET_ID,
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                filter_="filter",
                page_size=10,
                task_id=TASK_ID,
            )


class TestAutoMLDatasetListOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLListDatasetOperator,
            # Templated fields
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLListDatasetOperator = ti.task
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLDatasetDeleteOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLDeleteDatasetOperator,
            # Templated fields
            dataset_id="{{ 'dataset-id' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLDeleteDatasetOperator = ti.task
        assert task.dataset_id == "dataset-id"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"
