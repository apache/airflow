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

import pytest

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.cloud.automl_v1beta1 import Model

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links.translate import (
    TRANSLATION_BASE_LINK,
    TranslationDatasetListLink,
    TranslationLegacyDatasetLink,
    TranslationLegacyModelLink,
    TranslationLegacyModelPredictLink,
    TranslationLegacyModelTrainLink,
)
from airflow.providers.google.cloud.operators.automl import (
    AutoMLBatchPredictOperator,
    AutoMLCreateDatasetOperator,
    AutoMLListDatasetOperator,
    AutoMLTrainModelOperator,
)

GCP_LOCATION = "test-location"
GCP_PROJECT_ID = "test-project"
DATASET = "test-dataset"
MODEL = "test-model"


class TestTranslationLegacyDatasetLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
        expected_url = f"{TRANSLATION_BASE_LINK}/locations/{GCP_LOCATION}/datasets/{DATASET}/sentences?project={GCP_PROJECT_ID}"
        link = TranslationLegacyDatasetLink()
        ti = create_task_instance_of_operator(
            AutoMLCreateDatasetOperator,
            dag_id="test_legacy_dataset_link_dag",
            task_id="test_legacy_dataset_link_task",
            dataset=DATASET,
            location=GCP_LOCATION,
        )
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task, dataset_id=DATASET, project_id=GCP_PROJECT_ID)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestTranslationDatasetListLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
        expected_url = f"{TRANSLATION_BASE_LINK}/datasets?project={GCP_PROJECT_ID}"
        link = TranslationDatasetListLink()
        ti = create_task_instance_of_operator(
            AutoMLListDatasetOperator,
            dag_id="test_dataset_list_link_dag",
            task_id="test_dataset_list_link_task",
            location=GCP_LOCATION,
        )
        session.add(ti)
        session.commit()
        link.persist(context={"ti": ti}, task_instance=ti.task, project_id=GCP_PROJECT_ID)
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestTranslationLegacyModelLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
        expected_url = (
            f"{TRANSLATION_BASE_LINK}/locations/{GCP_LOCATION}/datasets/{DATASET}/"
            f"evaluate;modelId={MODEL}?project={GCP_PROJECT_ID}"
        )
        link = TranslationLegacyModelLink()
        ti = create_task_instance_of_operator(
            AutoMLTrainModelOperator,
            dag_id="test_legacy_model_link_dag",
            task_id="test_legacy_model_link_task",
            model=MODEL,
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
        )
        session.add(ti)
        session.commit()
        link.persist(
            context={"ti": ti},
            task_instance=ti.task,
            dataset_id=DATASET,
            model_id=MODEL,
            project_id=GCP_PROJECT_ID,
        )
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestTranslationLegacyModelTrainLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
        expected_url = (
            f"{TRANSLATION_BASE_LINK}/locations/{GCP_LOCATION}/datasets/{DATASET}/"
            f"train?project={GCP_PROJECT_ID}"
        )
        link = TranslationLegacyModelTrainLink()
        ti = create_task_instance_of_operator(
            AutoMLTrainModelOperator,
            dag_id="test_legacy_model_train_link_dag",
            task_id="test_legacy_model_train_link_task",
            model={"dataset_id": DATASET},
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
        )
        session.add(ti)
        session.commit()
        link.persist(
            context={"ti": ti},
            task_instance=ti.task,
            project_id=GCP_PROJECT_ID,
        )
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestTranslationLegacyModelPredictLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session):
        expected_url = (
            f"{TRANSLATION_BASE_LINK}/locations/{GCP_LOCATION}/datasets/{DATASET}/"
            f"predict;modelId={MODEL}?project={GCP_PROJECT_ID}"
        )
        link = TranslationLegacyModelPredictLink()
        with pytest.warns(AirflowProviderDeprecationWarning):
            ti = create_task_instance_of_operator(
                AutoMLBatchPredictOperator,
                dag_id="test_legacy_model_predict_link_dag",
                task_id="test_legacy_model_predict_link_task",
                model_id=MODEL,
                project_id=GCP_PROJECT_ID,
                location=GCP_LOCATION,
                input_config="input_config",
                output_config="input_config",
            )
        ti.task.model = Model(dataset_id=DATASET, display_name=MODEL)
        session.add(ti)
        session.commit()
        link.persist(
            context={"ti": ti},
            task_instance=ti.task,
            model_id=MODEL,
            project_id=GCP_PROJECT_ID,
            dataset_id=DATASET,
        )
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url
