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


class TestAutoMLTrainModelOperator:
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.create_model.return_value.result.return_value = Model(name=MODEL_PATH)
        mock_hook.return_value.extract_object_id = extract_object_id
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLTablesListColumnSpecsOperator,
            # Templated fields
            dataset_id="{{ 'dataset-id' }}",
            table_spec_id="{{ 'table-spec-id' }}",
            field_mask="{{ 'field-mask' }}",
            filter_="{{ 'filter-' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLTablesListColumnSpecsOperator = ti.task
        assert task.dataset_id == "dataset-id"
        assert task.table_spec_id == "table-spec-id"
        assert task.field_mask == "field-mask"
        assert task.filter_ == "filter-"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLUpdateDatasetOperator:
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLTablesUpdateDatasetOperator,
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
        task: AutoMLTablesUpdateDatasetOperator = ti.task
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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLDeployModelOperator,
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
        task: AutoMLDeployModelOperator = ti.task
        assert task.model_id == "model-id"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


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

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            AutoMLTablesListTableSpecsOperator,
            # Templated fields
            dataset_id="{{ 'dataset-id' }}",
            filter_="{{ 'filter-' }}",
            location="{{ 'location' }}",
            project_id="{{ 'project-id' }}",
            impersonation_chain="{{ 'impersonation-chain' }}",
            # Other parameters
            dag_id="test_template_body_templating_dag",
            task_id="test_template_body_templating_task",
            execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        ti.render_templates()
        task: AutoMLTablesListTableSpecsOperator = ti.task
        assert task.dataset_id == "dataset-id"
        assert task.filter_ == "filter-"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


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
