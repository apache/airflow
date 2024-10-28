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
"""This module contains Google AutoML links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links.base import BaseGoogleLink
from airflow.providers.google.common.deprecated import deprecated

if TYPE_CHECKING:
    from airflow.utils.context import Context

AUTOML_BASE_LINK = "https://console.cloud.google.com/automl-tables"
AUTOML_DATASET_LINK = (
    AUTOML_BASE_LINK
    + "/locations/{location}/datasets/{dataset_id}/schemav2?project={project_id}"
)
AUTOML_DATASET_LIST_LINK = AUTOML_BASE_LINK + "/datasets?project={project_id}"
AUTOML_MODEL_LINK = (
    AUTOML_BASE_LINK
    + "/locations/{location}/datasets/{dataset_id};modelId={model_id}/evaluate?project={project_id}"
)
AUTOML_MODEL_TRAIN_LINK = (
    AUTOML_BASE_LINK
    + "/locations/{location}/datasets/{dataset_id}/train?project={project_id}"
)
AUTOML_MODEL_PREDICT_LINK = (
    AUTOML_BASE_LINK
    + "/locations/{location}/datasets/{dataset_id};modelId={model_id}/predict?project={project_id}"
)


@deprecated(
    planned_removal_date="December 31, 2024",
    use_instead="TranslationLegacyDatasetLink class from airflow/providers/google/cloud/links/translate.py",
    category=AirflowProviderDeprecationWarning,
)
class AutoMLDatasetLink(BaseGoogleLink):
    """Helper class for constructing AutoML Dataset link."""

    name = "AutoML Dataset"
    key = "automl_dataset"
    format_str = AUTOML_DATASET_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        dataset_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=AutoMLDatasetLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": dataset_id,
                "project_id": project_id,
            },
        )


@deprecated(
    planned_removal_date="December 31, 2024",
    use_instead="TranslationDatasetListLink class from airflow/providers/google/cloud/links/translate.py",
    category=AirflowProviderDeprecationWarning,
)
class AutoMLDatasetListLink(BaseGoogleLink):
    """Helper class for constructing AutoML Dataset List link."""

    name = "AutoML Dataset List"
    key = "automl_dataset_list"
    format_str = AUTOML_DATASET_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=AutoMLDatasetListLink.key,
            value={
                "project_id": project_id,
            },
        )


@deprecated(
    planned_removal_date="December 31, 2024",
    use_instead="TranslationLegacyModelLink class from airflow/providers/google/cloud/links/translate.py",
    category=AirflowProviderDeprecationWarning,
)
class AutoMLModelLink(BaseGoogleLink):
    """Helper class for constructing AutoML Model link."""

    name = "AutoML Model"
    key = "automl_model"
    format_str = AUTOML_MODEL_LINK

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
            key=AutoMLModelLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": dataset_id,
                "model_id": model_id,
                "project_id": project_id,
            },
        )


@deprecated(
    planned_removal_date="December 31, 2024",
    use_instead="TranslationLegacyModelTrainLink class from "
    "airflow/providers/google/cloud/links/translate.py",
    category=AirflowProviderDeprecationWarning,
)
class AutoMLModelTrainLink(BaseGoogleLink):
    """Helper class for constructing AutoML Model Train link."""

    name = "AutoML Model Train"
    key = "automl_model_train"
    format_str = AUTOML_MODEL_TRAIN_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=AutoMLModelTrainLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": task_instance.model["dataset_id"],
                "project_id": project_id,
            },
        )


@deprecated(
    planned_removal_date="December 31, 2024",
    use_instead="TranslationLegacyModelPredictLink class from "
    "airflow/providers/google/cloud/links/translate.py",
    category=AirflowProviderDeprecationWarning,
)
class AutoMLModelPredictLink(BaseGoogleLink):
    """Helper class for constructing AutoML Model Predict link."""

    name = "AutoML Model Predict"
    key = "automl_model_predict"
    format_str = AUTOML_MODEL_PREDICT_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        model_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=AutoMLModelPredictLink.key,
            value={
                "location": task_instance.location,
                "dataset_id": "-",
                "model_id": model_id,
                "project_id": project_id,
            },
        )
