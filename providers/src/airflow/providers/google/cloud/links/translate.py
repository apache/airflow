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
"""This module contains Google Translate links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BASE_LINK, BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


TRANSLATION_BASE_LINK = BASE_LINK + "/translation"
TRANSLATION_LEGACY_DATASET_LINK = (
    TRANSLATION_BASE_LINK + "/locations/{location}/datasets/{dataset_id}/sentences?project={project_id}"
)
TRANSLATION_DATASET_LIST_LINK = TRANSLATION_BASE_LINK + "/datasets?project={project_id}"
TRANSLATION_LEGACY_MODEL_LINK = (
    TRANSLATION_BASE_LINK
    + "/locations/{location}/datasets/{dataset_id}/evaluate;modelId={model_id}?project={project_id}"
)
TRANSLATION_LEGACY_MODEL_TRAIN_LINK = (
    TRANSLATION_BASE_LINK + "/locations/{location}/datasets/{dataset_id}/train?project={project_id}"
)
TRANSLATION_LEGACY_MODEL_PREDICT_LINK = (
    TRANSLATION_BASE_LINK
    + "/locations/{location}/datasets/{dataset_id}/predict;modelId={model_id}?project={project_id}"
)

TRANSLATION_TRANSLATE_TEXT_BATCH = BASE_LINK + "/storage/browser/{output_uri_prefix}?project={project_id}"

TRANSLATION_NATIVE_DATASET_LINK = (
    TRANSLATION_BASE_LINK + "/locations/{location}/datasets/{dataset_id}/sentences?project={project_id}"
)
TRANSLATION_NATIVE_LIST_LINK = TRANSLATION_BASE_LINK + "/datasets?project={project_id}"

TRANSLATION_NATIVE_MODEL_LINK = (
    TRANSLATION_BASE_LINK
    + "/locations/{location}/datasets/{dataset_id}/evaluate;modelId={model_id}?project={project_id}"
)
TRANSLATION_MODELS_LIST_LINK = TRANSLATION_BASE_LINK + "/models/list?project={project_id}"


class TranslationLegacyDatasetLink(BaseGoogleLink):
    """
    Helper class for constructing Legacy Translation Dataset link.

    Legacy Datasets are created and managed by AutoML API.
    """

    name = "Translation Legacy Dataset"
    key = "translation_legacy_dataset"
    format_str = TRANSLATION_LEGACY_DATASET_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        dataset_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationLegacyDatasetLink.key,
            value={"location": task_instance.location, "dataset_id": dataset_id, "project_id": project_id},
        )


class TranslationDatasetListLink(BaseGoogleLink):
    """Helper class for constructing Translation Dataset List link."""

    name = "Translation Dataset List"
    key = "translation_dataset_list"
    format_str = TRANSLATION_DATASET_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationDatasetListLink.key,
            value={
                "project_id": project_id,
            },
        )


class TranslationLegacyModelLink(BaseGoogleLink):
    """
    Helper class for constructing Translation Legacy Model link.

    Legacy Models are created and managed by AutoML API.
    """

    name = "Translation Legacy Model"
    key = "translation_legacy_model"
    format_str = TRANSLATION_LEGACY_MODEL_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        dataset_id: str,
        model_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationLegacyModelLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": dataset_id,
                "model_id": model_id,
                "project_id": project_id,
            },
        )


class TranslationLegacyModelTrainLink(BaseGoogleLink):
    """
    Helper class for constructing Translation Legacy Model Train link.

    Legacy Models are created and managed by AutoML API.
    """

    name = "Translation Legacy Model Train"
    key = "translation_legacy_model_train"
    format_str = TRANSLATION_LEGACY_MODEL_TRAIN_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationLegacyModelTrainLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": task_instance.model["dataset_id"],
                "project_id": project_id,
            },
        )


class TranslationLegacyModelPredictLink(BaseGoogleLink):
    """
    Helper class for constructing Translation Legacy Model Predict link.

    Legacy Models are created and managed by AutoML API.
    """

    name = "Translation Legacy Model Predict"
    key = "translation_legacy_model_predict"
    format_str = TRANSLATION_LEGACY_MODEL_PREDICT_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        model_id: str,
        project_id: str,
        dataset_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationLegacyModelPredictLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": dataset_id,
                "model_id": model_id,
                "project_id": project_id,
            },
        )


class TranslateTextBatchLink(BaseGoogleLink):
    """
    Helper class for constructing Translation results for the text batch translate.

    Provides link to output results.

    """

    name = "Text Translate Batch"
    key = "translate_text_batch"
    format_str = TRANSLATION_TRANSLATE_TEXT_BATCH

    @staticmethod
    def extract_output_uri_prefix(output_config):
        return output_config["gcs_destination"]["output_uri_prefix"].rpartition("gs://")[-1]

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
        output_config: dict,
    ):
        task_instance.xcom_push(
            context,
            key=TranslateTextBatchLink.key,
            value={
                "project_id": project_id,
                "output_uri_prefix": TranslateTextBatchLink.extract_output_uri_prefix(output_config),
            },
        )


class TranslationNativeDatasetLink(BaseGoogleLink):
    """
    Helper class for constructing Legacy Translation Dataset link.

    Legacy Datasets are created and managed by AutoML API.
    """

    name = "Translation Native Dataset"
    key = "translation_naive_dataset"
    format_str = TRANSLATION_NATIVE_DATASET_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        dataset_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationNativeDatasetLink.key,
            value={"location": task_instance.location, "dataset_id": dataset_id, "project_id": project_id},
        )


class TranslationDatasetsListLink(BaseGoogleLink):
    """
    Helper class for constructing Translation Datasets List link.

    Both legacy and native datasets are available under this link.
    """

    name = "Translation Dataset List"
    key = "translation_dataset_list"
    format_str = TRANSLATION_DATASET_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationDatasetsListLink.key,
            value={
                "project_id": project_id,
            },
        )


class TranslationModelLink(BaseGoogleLink):
    """
    Helper class for constructing Translation Model link.

    Link for legacy and native models.
    """

    name = "Translation Model"
    key = "translation_model"
    format_str = TRANSLATION_NATIVE_MODEL_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        dataset_id: str,
        model_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationLegacyModelLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": dataset_id,
                "model_id": model_id,
                "project_id": project_id,
            },
        )


class TranslationModelsListLink(BaseGoogleLink):
    """
    Helper class for constructing Translation Models List link.

    Both legacy and native models are available under this link.
    """

    name = "Translation Models List"
    key = "translation_models_list"
    format_str = TRANSLATION_MODELS_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=TranslationModelsListLink.key,
            value={
                "project_id": project_id,
            },
        )


class TranslateResultByOutputConfigLink(BaseGoogleLink):
    """
    Helper class for constructing Translation results Link.

    Provides link to gcs destination output translation results, by provided output_config
    with gcs destination specified.
    """

    name = "Translate Results By Output Config"
    key = "translate_results_by_output_config"
    format_str = TRANSLATION_TRANSLATE_TEXT_BATCH

    @staticmethod
    def extract_output_uri_prefix(output_config):
        return output_config["gcs_destination"]["output_uri_prefix"].rpartition("gs://")[-1]

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
        output_config: dict,
    ):
        task_instance.xcom_push(
            context,
            key=TranslateResultByOutputConfigLink.key,
            value={
                "project_id": project_id,
                "output_uri_prefix": TranslateResultByOutputConfigLink.extract_output_uri_prefix(
                    output_config
                ),
            },
        )
