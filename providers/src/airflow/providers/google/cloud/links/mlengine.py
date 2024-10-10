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
"""This module contains Google ML Engine links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


MLENGINE_BASE_LINK = "https://console.cloud.google.com/ai-platform"
MLENGINE_MODEL_DETAILS_LINK = MLENGINE_BASE_LINK + "/models/{model_id}/versions?project={project_id}"
MLENGINE_MODEL_VERSION_DETAILS_LINK = (
    MLENGINE_BASE_LINK + "/models/{model_id}/versions/{version_id}/performance?project={project_id}"
)
MLENGINE_MODELS_LIST_LINK = MLENGINE_BASE_LINK + "/models/?project={project_id}"
MLENGINE_JOB_DETAILS_LINK = MLENGINE_BASE_LINK + "/jobs/{job_id}?project={project_id}"
MLENGINE_JOBS_LIST_LINK = MLENGINE_BASE_LINK + "/jobs?project={project_id}"


class MLEngineModelLink(BaseGoogleLink):
    """Helper class for constructing ML Engine link."""

    name = "MLEngine Model"
    key = "ml_engine_model"
    format_str = MLENGINE_MODEL_DETAILS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        model_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=MLEngineModelLink.key,
            value={"model_id": model_id, "project_id": project_id},
        )


class MLEngineModelsListLink(BaseGoogleLink):
    """Helper class for constructing ML Engine link."""

    name = "MLEngine Models List"
    key = "ml_engine_models_list"
    format_str = MLENGINE_MODELS_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=MLEngineModelsListLink.key,
            value={"project_id": project_id},
        )


class MLEngineJobDetailsLink(BaseGoogleLink):
    """Helper class for constructing ML Engine link."""

    name = "MLEngine Job Details"
    key = "ml_engine_job_details"
    format_str = MLENGINE_JOB_DETAILS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        job_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=MLEngineJobDetailsLink.key,
            value={"job_id": job_id, "project_id": project_id},
        )


class MLEngineModelVersionDetailsLink(BaseGoogleLink):
    """Helper class for constructing ML Engine link."""

    name = "MLEngine Version Details"
    key = "ml_engine_version_details"
    format_str = MLENGINE_MODEL_VERSION_DETAILS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        model_id: str,
        project_id: str,
        version_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=MLEngineModelVersionDetailsLink.key,
            value={"model_id": model_id, "project_id": project_id, "version_id": version_id},
        )


class MLEngineJobSListLink(BaseGoogleLink):
    """Helper class for constructing ML Engine link."""

    name = "MLEngine Jobs List"
    key = "ml_engine_jobs_list"
    format_str = MLENGINE_JOBS_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=MLEngineJobSListLink.key,
            value={"project_id": project_id},
        )
