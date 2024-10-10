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

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.automl_v1beta1 import BatchPredictResult, Dataset, Model, PredictResponse

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.automl import CloudAutoMLHook
from airflow.providers.google.cloud.hooks.vertex_ai.prediction_service import PredictionServiceHook
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
    "translation_model_metadata": {"train_budget_milli_node_hours": 1000},
}
MODEL_DEPRECATED = {
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
DATASET = {"dataset_id": "data", "translation_dataset_metadata": "data"}
DATASET_DEPRECATED = {"tables_model_metadata": "data"}
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

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        op = AutoMLTrainModelOperator(
            model=MODEL_DEPRECATED,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        expected_exception_str = (
            "AutoMLTrainModelOperator for text, image, and video prediction has been "
            "deprecated and no longer available"
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
            op.execute(context=mock.MagicMock())
        mock_hook.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: AutoMLTrainModelOperator = ti.task
        assert task.model == "model"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation_chain"


class TestAutoMLBatchPredictOperator:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslationLegacyModelPredictLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook, mock_link_persist):
        mock_hook.return_value.batch_predict.return_value.result.return_value = BatchPredictResult()
        mock_hook.return_value.extract_object_id = extract_object_id
        mock_hook.return_value.wait_for_operation.return_value = BatchPredictResult()
        mock_context = {"ti": mock.MagicMock()}
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = AutoMLBatchPredictOperator(
                model_id=MODEL_ID,
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                input_config=INPUT_CONFIG,
                output_config=OUTPUT_CONFIG,
                task_id=TASK_ID,
                prediction_params={},
            )
        op.execute(context=mock_context)
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
        mock_link_persist.assert_called_once_with(
            context=mock_context,
            task_instance=op,
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        returned_model = mock.MagicMock()
        del returned_model.translation_model_metadata
        mock_hook.return_value.get_model.return_value = returned_model
        mock_hook.return_value.extract_object_id = extract_object_id
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = AutoMLBatchPredictOperator(
                model_id=MODEL_ID,
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                input_config=INPUT_CONFIG,
                output_config=OUTPUT_CONFIG,
                task_id=TASK_ID,
                prediction_params={},
            )
        expected_exception_str = (
            "AutoMLBatchPredictOperator for text, image, and video prediction has been "
            "deprecated and no longer available"
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
            op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_model.assert_called_once_with(
            location=GCP_LOCATION,
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        mock_hook.return_value.batch_predict.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
        with pytest.warns(AirflowProviderDeprecationWarning):
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
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: AutoMLBatchPredictOperator = ti.task
        assert task.model_id == "model"
        assert task.input_config == "input-config"
        assert task.output_config == "output-config"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLPredictOperator:
    @mock.patch("airflow.providers.google.cloud.links.translate.TranslationLegacyModelPredictLink.persist")
    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook, mock_link_persist):
        mock_hook.return_value.predict.return_value = PredictResponse()
        mock_context = {"ti": mock.MagicMock()}
        op = AutoMLPredictOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            payload=PAYLOAD,
            task_id=TASK_ID,
            operation_params={"TEST_KEY": "TEST_VALUE"},
        )
        op.execute(context=mock_context)
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
        mock_link_persist.assert_called_once_with(
            context=mock_context,
            task_instance=op,
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
        )

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: AutoMLPredictOperator = ti.task
        assert task.model_id == "model-id"
        assert task.project_id == "project-id"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation-chain"

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecation(self, mock_hook):
        returned_model = mock.MagicMock(**MODEL_DEPRECATED)
        del returned_model.translation_model_metadata
        mock_hook.return_value.get_model.return_value = returned_model

        mock_hook.return_value.predict.return_value = PredictResponse()

        op = AutoMLPredictOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            payload=PAYLOAD,
            task_id=TASK_ID,
            operation_params={"TEST_KEY": "TEST_VALUE"},
        )
        expected_exception_str = (
            "AutoMLPredictOperator for text, image, and video prediction has been "
            "deprecated. Please use endpoint_id param instead of model_id param."
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
            op.execute(context=mock.MagicMock())
        mock_hook.return_value.predict.assert_not_called()

    @pytest.mark.db_test
    def test_hook_type(self):
        op = AutoMLPredictOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            payload=PAYLOAD,
            task_id=TASK_ID,
            operation_params={"TEST_KEY": "TEST_VALUE"},
        )
        assert isinstance(op.hook, CloudAutoMLHook)

        op = AutoMLPredictOperator(
            endpoint_id="endpoint_id",
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            payload=PAYLOAD,
            task_id=TASK_ID,
            operation_params={"TEST_KEY": "TEST_VALUE"},
        )
        assert isinstance(op.hook, PredictionServiceHook)


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

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        op = AutoMLCreateDatasetOperator(
            dataset=DATASET_DEPRECATED,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        expected_exception_str = (
            "AutoMLCreateDatasetOperator for text, image, and video prediction has been "
            "deprecated and no longer available"
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
            op.execute(context=mock.MagicMock())
        mock_hook.return_value.create_dataset.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: AutoMLCreateDatasetOperator = ti.task
        assert task.dataset == "dataset"
        assert task.project_id == "project-id"
        assert task.location == "location"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLTablesListColumnsSpecsOperator:
    expected_exception_string = (
        "Operator AutoMLTablesListColumnSpecsOperator has been deprecated due to shutdown of "
        "a legacy version of AutoML Tables on March 31, 2024. "
        "For additional information see: https://cloud.google.com/automl-tables/docs/deprecations."
    )

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        table_spec = "table_spec_id"
        filter_ = "filter"
        page_size = 42

        with pytest.raises(AirflowException, match=self.expected_exception_string):
            _ = AutoMLTablesListColumnSpecsOperator(
                dataset_id=DATASET_ID,
                table_spec_id=table_spec,
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                field_mask=MASK,
                filter_=filter_,
                page_size=page_size,
                task_id=TASK_ID,
            )
        mock_hook.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        with pytest.raises(AirflowException, match=self.expected_exception_string):
            _ = create_task_instance_of_operator(
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


class TestAutoMLTablesUpdateDatasetOperator:
    expected_exception_string = (
        "Operator AutoMLTablesUpdateDatasetOperator has been deprecated due to shutdown of "
        "a legacy version of AutoML Tables on March 31, 2024. "
        "For additional information see: https://cloud.google.com/automl-tables/docs/deprecations. "
        "Please use UpdateDatasetOperator from Vertex AI instead."
    )

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        mock_hook.return_value.update_dataset.return_value = Dataset(name=DATASET_PATH)

        dataset = copy.deepcopy(DATASET)
        dataset["name"] = DATASET_ID

        with pytest.raises(AirflowException, match=self.expected_exception_string):
            AutoMLTablesUpdateDatasetOperator(
                dataset=dataset,
                update_mask=MASK,
                location=GCP_LOCATION,
                task_id=TASK_ID,
            )
        mock_hook.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        with pytest.raises(AirflowException, match=self.expected_exception_string):
            create_task_instance_of_operator(
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

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        returned_model = mock.MagicMock(**MODEL_DEPRECATED)
        del returned_model.translation_model_metadata
        mock_hook.return_value.get_model.return_value = returned_model

        op = AutoMLGetModelOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        expected_exception_str = (
            "AutoMLGetModelOperator for text, image, and video prediction has been "
            "deprecated and no longer available"
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
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
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
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

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        returned_model = mock.MagicMock(**MODEL_DEPRECATED)
        del returned_model.translation_model_metadata
        mock_hook.return_value.get_model.return_value = returned_model

        op = AutoMLDeleteModelOperator(
            model_id=MODEL_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        expected_exception_str = (
            "AutoMLDeleteModelOperator for text, image, and video prediction has been "
            "deprecated and no longer available"
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
            op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_model.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=(),
            model_id=MODEL_ID,
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )
        mock_hook.return_value.delete_model.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: AutoMLDeleteModelOperator = ti.task
        assert task.model_id == "model-id"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLDeployModelOperator:
    expected_exception_string = (
        "Operator AutoMLDeployModelOperator has been deprecated due to shutdown of "
        "a legacy version of AutoML AutoML Natural Language, Vision, Video Intelligence "
        "on March 31, 2024. "
        "For additional information see: https://cloud.google.com/vision/automl/docs/deprecations. "
        "Please use DeployModelOperator from Vertex AI instead."
    )

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        image_detection_metadata = {}

        with pytest.raises(AirflowException, match=self.expected_exception_string):
            AutoMLDeployModelOperator(
                model_id=MODEL_ID,
                image_detection_metadata=image_detection_metadata,
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                task_id=TASK_ID,
            )

        mock_hook.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        with pytest.raises(AirflowException, match=self.expected_exception_string):
            create_task_instance_of_operator(
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

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        returned_dataset = mock.MagicMock()
        del returned_dataset.translation_dataset_metadata
        mock_hook.return_value.get_dataset.return_value = returned_dataset

        op = AutoMLImportDataOperator(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            input_config=INPUT_CONFIG,
            task_id=TASK_ID,
        )
        expected_exception_str = (
            "AutoMLImportDataOperator for text, image, and video prediction has been "
            "deprecated and no longer available"
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
            op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_dataset.assert_called_once_with(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_hook.return_value.import_data.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: AutoMLImportDataOperator = ti.task
        assert task.dataset_id == "dataset-id"
        assert task.input_config == "input-config"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"


class TestAutoMLTablesListTableSpecsOperator:
    expected_exception_string = (
        "Operator AutoMLTablesListTableSpecsOperator has been deprecated due to shutdown of "
        "a legacy version of AutoML Tables on March 31, 2024. "
        "For additional information see: https://cloud.google.com/automl-tables/docs/deprecations. "
    )

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute(self, mock_hook):
        filter_ = "filter"
        page_size = 42

        with pytest.raises(AirflowException, match=self.expected_exception_string):
            _ = AutoMLTablesListTableSpecsOperator(
                dataset_id=DATASET_ID,
                location=GCP_LOCATION,
                project_id=GCP_PROJECT_ID,
                filter_=filter_,
                page_size=page_size,
                task_id=TASK_ID,
            )
        mock_hook.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator):
        with pytest.raises(AirflowException, match=self.expected_exception_string):
            _ = create_task_instance_of_operator(
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

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        not_valid_dataset = mock.MagicMock()
        del not_valid_dataset.translation_dataset_metadata
        mock_hook.return_value.list_datasets.return_value = [DATASET, not_valid_dataset]
        op = AutoMLListDatasetOperator(location=GCP_LOCATION, project_id=GCP_PROJECT_ID, task_id=TASK_ID)
        expected_warning_str = (
            "Class `AutoMLListDatasetOperator` has been deprecated and no longer available. "
            "Please use `ListDatasetsOperator` instead"
        )
        with pytest.warns(UserWarning, match=expected_warning_str):
            op.execute(context=mock.MagicMock())

        mock_hook.return_value.list_datasets.assert_called_once_with(
            location=GCP_LOCATION,
            metadata=(),
            project_id=GCP_PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
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

    @mock.patch("airflow.providers.google.cloud.operators.automl.CloudAutoMLHook")
    def test_execute_deprecated(self, mock_hook):
        returned_dataset = mock.MagicMock()
        del returned_dataset.translation_dataset_metadata
        mock_hook.return_value.get_dataset.return_value = returned_dataset

        op = AutoMLDeleteDatasetOperator(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            task_id=TASK_ID,
        )
        expected_exception_str = (
            "AutoMLDeleteDatasetOperator for text, image, and video prediction has been "
            "deprecated and no longer available"
        )
        with pytest.raises(AirflowException, match=expected_exception_str):
            op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_dataset.assert_called_once_with(
            dataset_id=DATASET_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_hook.return_value.delete_dataset.assert_not_called()

    @pytest.mark.db_test
    def test_templating(self, create_task_instance_of_operator, session):
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
        session.add(ti)
        session.commit()
        ti.render_templates()
        task: AutoMLDeleteDatasetOperator = ti.task
        assert task.dataset_id == "dataset-id"
        assert task.location == "location"
        assert task.project_id == "project-id"
        assert task.impersonation_chain == "impersonation-chain"
