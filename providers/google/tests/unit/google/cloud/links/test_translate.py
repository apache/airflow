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

from unittest import mock

import pytest

from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from airflow.providers.google.cloud.links.translate import (
    TRANSLATION_BASE_LINK,
    TranslationDatasetListLink,
    TranslationLegacyDatasetLink,
    TranslationLegacyModelLink,
    TranslationLegacyModelTrainLink,
)
from airflow.providers.google.cloud.operators.automl import (
    AutoMLCreateDatasetOperator,
    AutoMLListDatasetOperator,
    AutoMLTrainModelOperator,
)

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

GCP_LOCATION = "test-location"
GCP_PROJECT_ID = "test-project"
DATASET = "test-dataset"
MODEL = "test-model"


class TestTranslationLegacyDatasetLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
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

        link.persist(
            context={"ti": ti, "task": ti.task},
            dataset_id=DATASET,
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
        )

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "location": ti.task.location,
                    "dataset_id": DATASET,
                    "project_id": GCP_PROJECT_ID,
                },
            )
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestTranslationDatasetListLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
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
        link.persist(context={"ti": ti, "task": ti.task}, project_id=GCP_PROJECT_ID)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "project_id": GCP_PROJECT_ID,
                },
            )
        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestTranslationLegacyModelLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
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
        task = mock.MagicMock()
        task.extra_links_params = {
            "dataset_id": DATASET,
            "model_id": MODEL,
            "project_id": GCP_PROJECT_ID,
            "location": GCP_LOCATION,
        }
        link.persist(
            context={"ti": ti, "task": task},
            dataset_id=DATASET,
            model_id=MODEL,
            project_id=GCP_PROJECT_ID,
        )
        if mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "location": ti.task.location,
                    "dataset_id": DATASET,
                    "model_id": MODEL,
                    "project_id": GCP_PROJECT_ID,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url


class TestTranslationLegacyModelTrainLink:
    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
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
        task = mock.MagicMock()
        task.extra_links_params = {
            "dataset_id": DATASET,
            "project_id": GCP_PROJECT_ID,
            "location": GCP_LOCATION,
        }
        link.persist(
            context={"ti": ti, "task": task},
            dataset_id=DATASET,
            project_id=GCP_PROJECT_ID,
            location=GCP_LOCATION,
        )

        if mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "location": ti.task.location,
                    "dataset_id": ti.task.model["dataset_id"],
                    "project_id": GCP_PROJECT_ID,
                },
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == expected_url
