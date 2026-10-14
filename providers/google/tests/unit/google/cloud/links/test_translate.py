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

EXPECTED_TRANSLATION_LEGACY_DATASET_LINK_NAME = "Translation Legacy Dataset"
EXPECTED_TRANSLATION_LEGACY_DATASET_LINK_KEY = "translation_legacy_dataset"
EXPECTED_TRANSLATION_LEGACY_DATASET_LINK_FORMAT_STR = (
    BASE_LINK + "/translation/locations/{location}/datasets/{dataset_id}/sentences?project={project_id}"
)
EXPECTED_TRANSLATION_DATASET_LIST_LINK_NAME = "Translation Dataset List"
EXPECTED_TRANSLATION_DATASET_LIST_LINK_KEY = "translation_dataset_list"
EXPECTED_TRANSLATION_DATASET_LIST_LINK_FORMAT_STR = BASE_LINK + "/translation/datasets?project={project_id}"
EXPECTED_TRANSLATION_LEGACY_MODEL_LINK_NAME = "Translation Legacy Model"
EXPECTED_TRANSLATION_LEGACY_MODEL_LINK_KEY = "translation_legacy_model"
EXPECTED_TRANSLATION_LEGACY_MODEL_LINK_FORMAT_STR = (
    BASE_LINK
    + "/translation/locations/{location}/datasets/{dataset_id}/evaluate;modelId={model_id}?project={project_id}"
)
EXPECTED_TRANSLATION_LEGACY_MODEL_TRAIN_LINK_NAME = "Translation Legacy Model Train"
EXPECTED_TRANSLATION_LEGACY_MODEL_TRAIN_LINK_KEY = "translation_legacy_model_train"
EXPECTED_TRANSLATION_LEGACY_MODEL_TRAIN_LINK_FORMAT_STR = (
    BASE_LINK + "/translation/locations/{location}/datasets/{dataset_id}/train?project={project_id}"
)
EXPECTED_TRANSLATION_LEGACY_MODEL_PREDICT_LINK_NAME = "Translation Legacy Model Predict"
EXPECTED_TRANSLATION_LEGACY_MODEL_PREDICT_LINK_KEY = "translation_legacy_model_predict"
EXPECTED_TRANSLATION_LEGACY_MODEL_PREDICT_LINK_FORMAT_STR = (
    BASE_LINK
    + "/translation/locations/{location}/datasets/{dataset_id}/predict;modelId={model_id}?project={project_id}"
)
EXPECTED_TRANSLATE_TEXT_BATCH_LINK_NAME = "Text Translate Batch"
EXPECTED_TRANSLATE_TEXT_BATCH_LINK_KEY = "translate_text_batch"
EXPECTED_TRANSLATE_TEXT_BATCH_LINK_FORMAT_STR = (
    BASE_LINK + "/storage/browser/{output_uri_prefix}?project={project_id}"
)
EXPECTED_TRANSLATION_NATIVE_DATASET_LINK_NAME = "Translation Native Dataset"
EXPECTED_TRANSLATION_NATIVE_DATASET_LINK_KEY = "translation_native_dataset"
EXPECTED_TRANSLATION_NATIVE_DATASET_LINK_FORMAT_STR = (
    BASE_LINK + "/translation/locations/{location}/datasets/{dataset_id}/sentences?project={project_id}"
)
EXPECTED_TRANSLATION_DATASETS_LIST_LINK_NAME = "Translation Dataset List"
EXPECTED_TRANSLATION_DATASETS_LIST_LINK_KEY = "translation_dataset_list"
EXPECTED_TRANSLATION_DATASETS_LIST_LINK_FORMAT_STR = BASE_LINK + "/translation/datasets?project={project_id}"
EXPECTED_TRANSLATION_MODEL_LINK_NAME = "Translation Model"
EXPECTED_TRANSLATION_MODEL_LINK_KEY = "translation_model"
EXPECTED_TRANSLATION_MODEL_LINK_FORMAT_STR = (
    BASE_LINK
    + "/translation/locations/{location}/datasets/{dataset_id}/evaluate;modelId={model_id}?project={project_id}"
)
EXPECTED_TRANSLATION_MODELS_LIST_LINK_NAME = "Translation Models List"
EXPECTED_TRANSLATION_MODELS_LIST_LINK_KEY = "translation_models_list"
EXPECTED_TRANSLATION_MODELS_LIST_LINK_FORMAT_STR = BASE_LINK + "/translation/models/list?project={project_id}"
EXPECTED_TRANSLATE_RESULT_BY_OUTPUT_CONFIG_LINK_NAME = "Translate Results By Output Config"
EXPECTED_TRANSLATE_RESULT_BY_OUTPUT_CONFIG_LINK_KEY = "translate_results_by_output_config"
EXPECTED_TRANSLATE_RESULT_BY_OUTPUT_CONFIG_LINK_FORMAT_STR = (
    BASE_LINK + "/storage/browser/{output_uri_prefix}?project={project_id}"
)
EXPECTED_TRANSLATION_GLOSSARIES_LIST_LINK_NAME = "Translation Glossaries List"
EXPECTED_TRANSLATION_GLOSSARIES_LIST_LINK_KEY = "translation_glossaries_list"
EXPECTED_TRANSLATION_GLOSSARIES_LIST_LINK_FORMAT_STR = (
    BASE_LINK + "/translation/hub/resources?project={project_id}"
)


class TestTranslationLegacyDatasetLink:
    def test_class_attributes(self):
        assert TranslationLegacyDatasetLink.name == EXPECTED_TRANSLATION_LEGACY_DATASET_LINK_NAME
        assert TranslationLegacyDatasetLink.key == EXPECTED_TRANSLATION_LEGACY_DATASET_LINK_KEY
        assert TranslationLegacyDatasetLink.format_str == EXPECTED_TRANSLATION_LEGACY_DATASET_LINK_FORMAT_STR


class TestTranslationDatasetListLink:
    def test_class_attributes(self):
        assert TranslationDatasetListLink.name == EXPECTED_TRANSLATION_DATASET_LIST_LINK_NAME
        assert TranslationDatasetListLink.key == EXPECTED_TRANSLATION_DATASET_LIST_LINK_KEY
        assert TranslationDatasetListLink.format_str == EXPECTED_TRANSLATION_DATASET_LIST_LINK_FORMAT_STR


class TestTranslationLegacyModelLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelLink.name == EXPECTED_TRANSLATION_LEGACY_MODEL_LINK_NAME
        assert TranslationLegacyModelLink.key == EXPECTED_TRANSLATION_LEGACY_MODEL_LINK_KEY
        assert TranslationLegacyModelLink.format_str == EXPECTED_TRANSLATION_LEGACY_MODEL_LINK_FORMAT_STR


class TestTranslationLegacyModelTrainLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelTrainLink.name == EXPECTED_TRANSLATION_LEGACY_MODEL_TRAIN_LINK_NAME
        assert TranslationLegacyModelTrainLink.key == EXPECTED_TRANSLATION_LEGACY_MODEL_TRAIN_LINK_KEY
        assert (
            TranslationLegacyModelTrainLink.format_str
            == EXPECTED_TRANSLATION_LEGACY_MODEL_TRAIN_LINK_FORMAT_STR
        )


class TestTranslationLegacyModelPredictLink:
    def test_class_attributes(self):
        assert TranslationLegacyModelPredictLink.name == EXPECTED_TRANSLATION_LEGACY_MODEL_PREDICT_LINK_NAME
        assert TranslationLegacyModelPredictLink.key == EXPECTED_TRANSLATION_LEGACY_MODEL_PREDICT_LINK_KEY
        assert (
            TranslationLegacyModelPredictLink.format_str
            == EXPECTED_TRANSLATION_LEGACY_MODEL_PREDICT_LINK_FORMAT_STR
        )


class TestTranslateTextBatchLink:
    def test_class_attributes(self):
        assert TranslateTextBatchLink.name == EXPECTED_TRANSLATE_TEXT_BATCH_LINK_NAME
        assert TranslateTextBatchLink.key == EXPECTED_TRANSLATE_TEXT_BATCH_LINK_KEY
        assert TranslateTextBatchLink.format_str == EXPECTED_TRANSLATE_TEXT_BATCH_LINK_FORMAT_STR


class TestTranslationNativeDatasetLink:
    def test_class_attributes(self):
        assert TranslationNativeDatasetLink.name == EXPECTED_TRANSLATION_NATIVE_DATASET_LINK_NAME
        assert TranslationNativeDatasetLink.key == EXPECTED_TRANSLATION_NATIVE_DATASET_LINK_KEY
        assert TranslationNativeDatasetLink.format_str == EXPECTED_TRANSLATION_NATIVE_DATASET_LINK_FORMAT_STR


class TestTranslationDatasetsListLink:
    def test_class_attributes(self):
        assert TranslationDatasetsListLink.name == EXPECTED_TRANSLATION_DATASETS_LIST_LINK_NAME
        assert TranslationDatasetsListLink.key == EXPECTED_TRANSLATION_DATASETS_LIST_LINK_KEY
        assert TranslationDatasetsListLink.format_str == EXPECTED_TRANSLATION_DATASETS_LIST_LINK_FORMAT_STR


class TestTranslationModelLink:
    def test_class_attributes(self):
        assert TranslationModelLink.name == EXPECTED_TRANSLATION_MODEL_LINK_NAME
        assert TranslationModelLink.key == EXPECTED_TRANSLATION_MODEL_LINK_KEY
        assert TranslationModelLink.format_str == EXPECTED_TRANSLATION_MODEL_LINK_FORMAT_STR


class TestTranslationModelsListLink:
    def test_class_attributes(self):
        assert TranslationModelsListLink.name == EXPECTED_TRANSLATION_MODELS_LIST_LINK_NAME
        assert TranslationModelsListLink.key == EXPECTED_TRANSLATION_MODELS_LIST_LINK_KEY
        assert TranslationModelsListLink.format_str == EXPECTED_TRANSLATION_MODELS_LIST_LINK_FORMAT_STR


class TestTranslateResultByOutputConfigLink:
    def test_class_attributes(self):
        assert TranslateResultByOutputConfigLink.name == EXPECTED_TRANSLATE_RESULT_BY_OUTPUT_CONFIG_LINK_NAME
        assert TranslateResultByOutputConfigLink.key == EXPECTED_TRANSLATE_RESULT_BY_OUTPUT_CONFIG_LINK_KEY
        assert (
            TranslateResultByOutputConfigLink.format_str
            == EXPECTED_TRANSLATE_RESULT_BY_OUTPUT_CONFIG_LINK_FORMAT_STR
        )


class TestTranslationGlossariesListLink:
    def test_class_attributes(self):
        assert TranslationGlossariesListLink.name == EXPECTED_TRANSLATION_GLOSSARIES_LIST_LINK_NAME
        assert TranslationGlossariesListLink.key == EXPECTED_TRANSLATION_GLOSSARIES_LIST_LINK_KEY
        assert (
            TranslationGlossariesListLink.format_str == EXPECTED_TRANSLATION_GLOSSARIES_LIST_LINK_FORMAT_STR
        )
