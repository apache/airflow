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

"""Tests for Translate links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.translate import (
    TRANSLATION_DATASET_LIST_LINK,
    TRANSLATION_HUB_RESOURCES_LIST_LINK,
    TRANSLATION_LEGACY_DATASET_LINK,
    TRANSLATION_LEGACY_MODEL_LINK,
    TRANSLATION_LEGACY_MODEL_PREDICT_LINK,
    TRANSLATION_LEGACY_MODEL_TRAIN_LINK,
    TRANSLATION_MODELS_LIST_LINK,
    TRANSLATION_NATIVE_DATASET_LINK,
    TRANSLATION_NATIVE_MODEL_LINK,
    TRANSLATION_TRANSLATE_TEXT_BATCH,
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

TEST_DATASET_ID = "test-dataset-id"
TEST_LOCATION = "test-location"
TEST_MODEL_ID = "test-model-id"
TEST_OUTPUT_URI_PREFIX = "test-output-uri-prefix"
TEST_PROJECT_ID = "test-project-id"


class TestTranslationLegacyDatasetLink:
    def test_class_attributes(self):
        assert TranslationLegacyDatasetLink.key == "translation_legacy_dataset"
        assert TranslationLegacyDatasetLink.name == "Translation Legacy Dataset"
        assert TranslationLegacyDatasetLink.format_str == TRANSLATION_LEGACY_DATASET_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationLegacyDatasetLink.persist(
            context=mock_context,
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_legacy_dataset",
            value={"dataset_id": TEST_DATASET_ID, "location": TEST_LOCATION, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = TranslationLegacyDatasetLink()

        result = link._format_link(
            dataset_id=TEST_DATASET_ID, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )

        # ``TRANSLATION_LEGACY_DATASET_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_LEGACY_DATASET_LINK.format(
            dataset_id=TEST_DATASET_ID, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )


class TestTranslationDatasetListLink:
    def test_class_attributes(self):
        assert TranslationDatasetListLink.key == "translation_dataset_list"
        assert TranslationDatasetListLink.name == "Translation Dataset List"
        assert TranslationDatasetListLink.format_str == TRANSLATION_DATASET_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationDatasetListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_dataset_list",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = TranslationDatasetListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``TRANSLATION_DATASET_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_DATASET_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestTranslationLegacyModelLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelLink.key == "translation_legacy_model"
        assert TranslationLegacyModelLink.name == "Translation Legacy Model"
        assert TranslationLegacyModelLink.format_str == TRANSLATION_LEGACY_MODEL_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationLegacyModelLink.persist(
            context=mock_context,
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_legacy_model",
            value={
                "dataset_id": TEST_DATASET_ID,
                "location": TEST_LOCATION,
                "model_id": TEST_MODEL_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = TranslationLegacyModelLink()

        result = link._format_link(
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )

        # ``TRANSLATION_LEGACY_MODEL_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_LEGACY_MODEL_LINK.format(
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )


class TestTranslationLegacyModelTrainLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelTrainLink.key == "translation_legacy_model_train"
        assert TranslationLegacyModelTrainLink.name == "Translation Legacy Model Train"
        assert TranslationLegacyModelTrainLink.format_str == TRANSLATION_LEGACY_MODEL_TRAIN_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationLegacyModelTrainLink.persist(
            context=mock_context,
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_legacy_model_train",
            value={"dataset_id": TEST_DATASET_ID, "location": TEST_LOCATION, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = TranslationLegacyModelTrainLink()

        result = link._format_link(
            dataset_id=TEST_DATASET_ID, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )

        # ``TRANSLATION_LEGACY_MODEL_TRAIN_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_LEGACY_MODEL_TRAIN_LINK.format(
            dataset_id=TEST_DATASET_ID, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )


class TestTranslationLegacyModelPredictLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelPredictLink.key == "translation_legacy_model_predict"
        assert TranslationLegacyModelPredictLink.name == "Translation Legacy Model Predict"
        assert TranslationLegacyModelPredictLink.format_str == TRANSLATION_LEGACY_MODEL_PREDICT_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationLegacyModelPredictLink.persist(
            context=mock_context,
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_legacy_model_predict",
            value={
                "dataset_id": TEST_DATASET_ID,
                "location": TEST_LOCATION,
                "model_id": TEST_MODEL_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = TranslationLegacyModelPredictLink()

        result = link._format_link(
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )

        # ``TRANSLATION_LEGACY_MODEL_PREDICT_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_LEGACY_MODEL_PREDICT_LINK.format(
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )


class TestTranslateTextBatchLink:
    def test_class_attributes(self):
        assert TranslateTextBatchLink.key == "translate_text_batch"
        assert TranslateTextBatchLink.name == "Text Translate Batch"
        assert TranslateTextBatchLink.format_str == TRANSLATION_TRANSLATE_TEXT_BATCH

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslateTextBatchLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            output_config={"gcs_destination": {"output_uri_prefix": f"gs://{TEST_OUTPUT_URI_PREFIX}"}},
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translate_text_batch",
            value={"project_id": TEST_PROJECT_ID, "output_uri_prefix": TEST_OUTPUT_URI_PREFIX},
        )

    def test_extract_output_uri_prefix_strips_the_gcs_scheme(self):
        output_config = {"gcs_destination": {"output_uri_prefix": f"gs://{TEST_OUTPUT_URI_PREFIX}"}}

        assert TranslateTextBatchLink.extract_output_uri_prefix(output_config) == TEST_OUTPUT_URI_PREFIX

    def test_format_link(self):
        link = TranslateTextBatchLink()

        result = link._format_link(output_uri_prefix=TEST_OUTPUT_URI_PREFIX, project_id=TEST_PROJECT_ID)

        # ``TRANSLATION_TRANSLATE_TEXT_BATCH`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_TRANSLATE_TEXT_BATCH.format(
            output_uri_prefix=TEST_OUTPUT_URI_PREFIX, project_id=TEST_PROJECT_ID
        )


class TestTranslationNativeDatasetLink:
    def test_class_attributes(self):
        assert TranslationNativeDatasetLink.key == "translation_native_dataset"
        assert TranslationNativeDatasetLink.name == "Translation Native Dataset"
        assert TranslationNativeDatasetLink.format_str == TRANSLATION_NATIVE_DATASET_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationNativeDatasetLink.persist(
            context=mock_context,
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_native_dataset",
            value={"dataset_id": TEST_DATASET_ID, "location": TEST_LOCATION, "project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = TranslationNativeDatasetLink()

        result = link._format_link(
            dataset_id=TEST_DATASET_ID, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )

        # ``TRANSLATION_NATIVE_DATASET_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_NATIVE_DATASET_LINK.format(
            dataset_id=TEST_DATASET_ID, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )


class TestTranslationDatasetsListLink:
    def test_class_attributes(self):
        assert TranslationDatasetsListLink.key == "translation_dataset_list"
        assert TranslationDatasetsListLink.name == "Translation Dataset List"
        assert TranslationDatasetsListLink.format_str == TRANSLATION_DATASET_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationDatasetsListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_dataset_list",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = TranslationDatasetsListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``TRANSLATION_DATASET_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_DATASET_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestTranslationModelLink:
    def test_class_attributes(self):
        assert TranslationModelLink.key == "translation_model"
        assert TranslationModelLink.name == "Translation Model"
        assert TranslationModelLink.format_str == TRANSLATION_NATIVE_MODEL_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationModelLink.persist(
            context=mock_context,
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_model",
            value={
                "dataset_id": TEST_DATASET_ID,
                "location": TEST_LOCATION,
                "model_id": TEST_MODEL_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = TranslationModelLink()

        result = link._format_link(
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )

        # ``TRANSLATION_NATIVE_MODEL_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_NATIVE_MODEL_LINK.format(
            dataset_id=TEST_DATASET_ID,
            location=TEST_LOCATION,
            model_id=TEST_MODEL_ID,
            project_id=TEST_PROJECT_ID,
        )


class TestTranslationModelsListLink:
    def test_class_attributes(self):
        assert TranslationModelsListLink.key == "translation_models_list"
        assert TranslationModelsListLink.name == "Translation Models List"
        assert TranslationModelsListLink.format_str == TRANSLATION_MODELS_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationModelsListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_models_list",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = TranslationModelsListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``TRANSLATION_MODELS_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_MODELS_LIST_LINK.format(project_id=TEST_PROJECT_ID)


class TestTranslateResultByOutputConfigLink:
    def test_class_attributes(self):
        assert TranslateResultByOutputConfigLink.key == "translate_results_by_output_config"
        assert TranslateResultByOutputConfigLink.name == "Translate Results By Output Config"
        assert TranslateResultByOutputConfigLink.format_str == TRANSLATION_TRANSLATE_TEXT_BATCH

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslateResultByOutputConfigLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            output_config={"gcs_destination": {"output_uri_prefix": f"gs://{TEST_OUTPUT_URI_PREFIX}"}},
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translate_results_by_output_config",
            value={"project_id": TEST_PROJECT_ID, "output_uri_prefix": TEST_OUTPUT_URI_PREFIX},
        )

    def test_extract_output_uri_prefix_strips_the_gcs_scheme(self):
        output_config = {"gcs_destination": {"output_uri_prefix": f"gs://{TEST_OUTPUT_URI_PREFIX}"}}

        assert (
            TranslateResultByOutputConfigLink.extract_output_uri_prefix(output_config)
            == TEST_OUTPUT_URI_PREFIX
        )

    def test_format_link(self):
        link = TranslateResultByOutputConfigLink()

        result = link._format_link(output_uri_prefix=TEST_OUTPUT_URI_PREFIX, project_id=TEST_PROJECT_ID)

        # ``TRANSLATION_TRANSLATE_TEXT_BATCH`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_TRANSLATE_TEXT_BATCH.format(
            output_uri_prefix=TEST_OUTPUT_URI_PREFIX, project_id=TEST_PROJECT_ID
        )


class TestTranslationGlossariesListLink:
    def test_class_attributes(self):
        assert TranslationGlossariesListLink.key == "translation_glossaries_list"
        assert TranslationGlossariesListLink.name == "Translation Glossaries List"
        assert TranslationGlossariesListLink.format_str == TRANSLATION_HUB_RESOURCES_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        TranslationGlossariesListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="translation_glossaries_list",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = TranslationGlossariesListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``TRANSLATION_HUB_RESOURCES_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == TRANSLATION_HUB_RESOURCES_LIST_LINK.format(project_id=TEST_PROJECT_ID)
