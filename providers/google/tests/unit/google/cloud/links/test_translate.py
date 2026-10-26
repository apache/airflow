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
"""Tests for Google Cloud Translate links.

Expected URLs are fully hard-coded: the path and query parts are never derived from the
constants under test, so a broken or hand-edited ``format_str`` always fails the test. The
``key`` values are asserted as literals too, they are stored in XComs and serialized DAGs,
so values that merely mirror the module constants would not catch a rename.
"""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.translate import (
    TranslateResultByOutputConfigLink,
    TranslateTextBatchLink,
    TranslationDatasetListLink,
    TranslationDatasetsListLink,
    TranslationGlossariesListLink,
    TranslationLegacyDatasetLink,
    TranslationLegacyModelLink,
    TranslationLegacyModelPredictLink,
    TranslationLegacyModelTrainLink,
    TranslationModelLink,
    TranslationModelsListLink,
    TranslationNativeDatasetLink,
)

TEST_PROJECT_ID = "test-project"
TEST_DATASET_ID = "test-dataset"
TEST_MODEL_ID = "test-model"
TEST_LOCATION = "europe-west1"
TEST_OUTPUT_URI_PREFIX = "test-bucket/test-results/run-1"


class TestTranslationLegacyDatasetLink:
    def test_class_attributes(self):
        assert TranslationLegacyDatasetLink.key == "translation_legacy_dataset"
        assert TranslationLegacyDatasetLink.name == "Translation Legacy Dataset"

    def test_get_link(self):
        result = TranslationLegacyDatasetLink()._format_link(
            location=TEST_LOCATION, dataset_id=TEST_DATASET_ID, project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK
            + "/translation/locations/europe-west1/datasets/test-dataset/sentences?project=test-project"
        )


class TestTranslationDatasetListLink:
    def test_class_attributes(self):
        assert TranslationDatasetListLink.key == "translation_dataset_list"
        assert TranslationDatasetListLink.name == "Translation Dataset List"

    def test_get_link(self):
        result = TranslationDatasetListLink()._format_link(project_id=TEST_PROJECT_ID)
        assert result == BASE_LINK + "/translation/datasets?project=test-project"


class TestTranslationLegacyModelLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelLink.key == "translation_legacy_model"
        assert TranslationLegacyModelLink.name == "Translation Legacy Model"

    def test_get_link(self):
        result = TranslationLegacyModelLink()._format_link(
            location=TEST_LOCATION,
            dataset_id=TEST_DATASET_ID,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )
        assert result == (
            BASE_LINK
            + "/translation/locations/europe-west1/datasets/test-dataset/"
            + "evaluate;modelId=test-model?project=test-project"
        )


class TestTranslationLegacyModelTrainLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelTrainLink.key == "translation_legacy_model_train"
        assert TranslationLegacyModelTrainLink.name == "Translation Legacy Model Train"

    def test_get_link(self):
        result = TranslationLegacyModelTrainLink()._format_link(
            location=TEST_LOCATION, dataset_id=TEST_DATASET_ID, project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK + "/translation/locations/europe-west1/datasets/test-dataset/train?project=test-project"
        )


class TestTranslationLegacyModelPredictLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelPredictLink.key == "translation_legacy_model_predict"
        assert TranslationLegacyModelPredictLink.name == "Translation Legacy Model Predict"

    def test_get_link(self):
        result = TranslationLegacyModelPredictLink()._format_link(
            location=TEST_LOCATION,
            dataset_id=TEST_DATASET_ID,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )
        assert result == (
            BASE_LINK
            + "/translation/locations/europe-west1/datasets/test-dataset/"
            + "predict;modelId=test-model?project=test-project"
        )


class TestTranslationNativeDatasetLink:
    def test_class_attributes(self):
        assert TranslationNativeDatasetLink.key == "translation_native_dataset"
        assert TranslationNativeDatasetLink.name == "Translation Native Dataset"

    def test_get_link(self):
        result = TranslationNativeDatasetLink()._format_link(
            location=TEST_LOCATION, dataset_id=TEST_DATASET_ID, project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK
            + "/translation/locations/europe-west1/datasets/test-dataset/sentences?project=test-project"
        )


class TestTranslationDatasetsListLink:
    def test_class_attributes(self):
        assert TranslationDatasetsListLink.key == "translation_dataset_list"
        assert TranslationDatasetsListLink.name == "Translation Dataset List"

    def test_get_link(self):
        result = TranslationDatasetsListLink()._format_link(project_id=TEST_PROJECT_ID)
        assert result == BASE_LINK + "/translation/datasets?project=test-project"


class TestTranslationModelLink:
    def test_class_attributes(self):
        assert TranslationModelLink.key == "translation_model"
        assert TranslationModelLink.name == "Translation Model"

    def test_get_link(self):
        result = TranslationModelLink()._format_link(
            location=TEST_LOCATION,
            dataset_id=TEST_DATASET_ID,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )
        assert result == (
            BASE_LINK
            + "/translation/locations/europe-west1/datasets/test-dataset/"
            + "evaluate;modelId=test-model?project=test-project"
        )


class TestTranslationModelsListLink:
    def test_class_attributes(self):
        assert TranslationModelsListLink.key == "translation_models_list"
        assert TranslationModelsListLink.name == "Translation Models List"

    def test_get_link(self):
        result = TranslationModelsListLink()._format_link(project_id=TEST_PROJECT_ID)
        assert result == BASE_LINK + "/translation/models/list?project=test-project"


class TestTranslationGlossariesListLink:
    def test_class_attributes(self):
        assert TranslationGlossariesListLink.key == "translation_glossaries_list"
        assert TranslationGlossariesListLink.name == "Translation Glossaries List"

    def test_get_link(self):
        result = TranslationGlossariesListLink()._format_link(project_id=TEST_PROJECT_ID)
        assert result == BASE_LINK + "/translation/hub/resources?project=test-project"


class TestTranslateTextBatchLink:
    def test_class_attributes(self):
        assert TranslateTextBatchLink.key == "translate_text_batch"
        assert TranslateTextBatchLink.name == "Text Translate Batch"

    def test_extract_output_uri_prefix_strips_gs_scheme(self):
        output_config = {"gcs_destination": {"output_uri_prefix": "gs://" + TEST_OUTPUT_URI_PREFIX}}
        assert TranslateTextBatchLink.extract_output_uri_prefix(output_config) == TEST_OUTPUT_URI_PREFIX

    def test_extract_output_uri_prefix_keeps_relative_prefix(self):
        long_prefix = {"gcs_destination": {"output_uri_prefix": TEST_OUTPUT_URI_PREFIX}}
        assert TranslateTextBatchLink.extract_output_uri_prefix(long_prefix) == TEST_OUTPUT_URI_PREFIX

    def test_get_link(self):
        result = TranslateTextBatchLink()._format_link(
            project_id=TEST_PROJECT_ID, output_uri_prefix=TEST_OUTPUT_URI_PREFIX
        )
        assert result == BASE_LINK + "/storage/browser/test-bucket/test-results/run-1?project=test-project"

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslateTextBatchLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            output_config={"gcs_destination": {"output_uri_prefix": "gs://" + TEST_OUTPUT_URI_PREFIX}},
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translate_text_batch",
            value={"project_id": TEST_PROJECT_ID, "output_uri_prefix": TEST_OUTPUT_URI_PREFIX},
        )


class TestTranslateResultByOutputConfigLink:
    def test_class_attributes(self):
        assert TranslateResultByOutputConfigLink.key == "translate_results_by_output_config"
        assert TranslateResultByOutputConfigLink.name == "Translate Results By Output Config"

    def test_extract_output_uri_prefix(self):
        output_config = {"gcs_destination": {"output_uri_prefix": "gs://" + TEST_OUTPUT_URI_PREFIX}}
        assert (
            TranslateResultByOutputConfigLink.extract_output_uri_prefix(output_config)
            == TEST_OUTPUT_URI_PREFIX
        )

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslateResultByOutputConfigLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            output_config={"gcs_destination": {"output_uri_prefix": "gs://" + TEST_OUTPUT_URI_PREFIX}},
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translate_results_by_output_config",
            value={"project_id": TEST_PROJECT_ID, "output_uri_prefix": TEST_OUTPUT_URI_PREFIX},
        )

    def test_get_link(self):
        result = TranslateResultByOutputConfigLink()._format_link(
            project_id=TEST_PROJECT_ID, output_uri_prefix=TEST_OUTPUT_URI_PREFIX
        )
        assert result == BASE_LINK + "/storage/browser/test-bucket/test-results/run-1?project=test-project"
