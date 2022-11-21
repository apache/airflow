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

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

VERTEX_AI_BASE_LINK = "/vertex-ai"
VERTEX_AI_MODEL_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/models/{model_id}/deploy?project={project_id}"
)
VERTEX_AI_MODEL_LIST_LINK = VERTEX_AI_BASE_LINK + "/models?project={project_id}"
VERTEX_AI_MODEL_EXPORT_LINK = "/storage/browser/{bucket_name}/model-{model_id}?project={project_id}"
VERTEX_AI_TRAINING_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/training/{training_id}/cpu?project={project_id}"
)
VERTEX_AI_TRAINING_PIPELINES_LINK = VERTEX_AI_BASE_LINK + "/training/training-pipelines?project={project_id}"
VERTEX_AI_DATASET_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/datasets/{dataset_id}/analyze?project={project_id}"
)
VERTEX_AI_DATASET_LIST_LINK = VERTEX_AI_BASE_LINK + "/datasets?project={project_id}"
VERTEX_AI_HYPERPARAMETER_TUNING_JOB_LIST_LINK = (
    VERTEX_AI_BASE_LINK + "/training/hyperparameter-tuning-jobs?project={project_id}"
)
VERTEX_AI_BATCH_PREDICTION_JOB_LINK = (
    VERTEX_AI_BASE_LINK
    + "/locations/{region}/batch-predictions/{batch_prediction_job_id}?project={project_id}"
)
VERTEX_AI_BATCH_PREDICTION_JOB_LIST_LINK = VERTEX_AI_BASE_LINK + "/batch-predictions?project={project_id}"
VERTEX_AI_ENDPOINT_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/endpoints/{endpoint_id}?project={project_id}"
)
VERTEX_AI_ENDPOINT_LIST_LINK = VERTEX_AI_BASE_LINK + "/endpoints?project={project_id}"


class VertexAIModelLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Model link"""

    name = "Vertex AI Model"
    key = "model_conf"
    format_str = VERTEX_AI_MODEL_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        model_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIModelLink.key,
            value={
                "model_id": model_id,
                "region": task_instance.region,
                "project_id": task_instance.project_id,
            },
        )


class VertexAIModelListLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Models Link"""

    name = "Model List"
    key = "models_conf"
    format_str = VERTEX_AI_MODEL_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIModelListLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )


class VertexAIModelExportLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Model Export Link"""

    name = "Export Model"
    key = "export_conf"
    format_str = VERTEX_AI_MODEL_EXPORT_LINK

    @staticmethod
    def extract_bucket_name(config):
        """Returns bucket name from output configuration."""
        return config["artifact_destination"]["output_uri_prefix"].rpartition("gs://")[-1]

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIModelExportLink.key,
            value={
                "project_id": task_instance.project_id,
                "model_id": task_instance.model_id,
                "bucket_name": VertexAIModelExportLink.extract_bucket_name(task_instance.output_config),
            },
        )


class VertexAITrainingLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Training link"""

    name = "Vertex AI Training"
    key = "training_conf"
    format_str = VERTEX_AI_TRAINING_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        training_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAITrainingLink.key,
            value={
                "training_id": training_id,
                "region": task_instance.region,
                "project_id": task_instance.project_id,
            },
        )


class VertexAITrainingPipelinesLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Training Pipelines link"""

    name = "Vertex AI Training Pipelines"
    key = "pipelines_conf"
    format_str = VERTEX_AI_TRAINING_PIPELINES_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAITrainingPipelinesLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )


class VertexAIDatasetLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Dataset link"""

    name = "Dataset"
    key = "dataset_conf"
    format_str = VERTEX_AI_DATASET_LINK

    @staticmethod
    def persist(context: Context, task_instance, dataset_id: str):
        task_instance.xcom_push(
            context=context,
            key=VertexAIDatasetLink.key,
            value={
                "dataset_id": dataset_id,
                "region": task_instance.region,
                "project_id": task_instance.project_id,
            },
        )


class VertexAIDatasetListLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Datasets Link"""

    name = "Dataset List"
    key = "datasets_conf"
    format_str = VERTEX_AI_DATASET_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIDatasetListLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )


class VertexAIHyperparameterTuningJobListLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI HyperparameterTuningJobs Link"""

    name = "Hyperparameter Tuning Job List"
    key = "hyperparameter_tuning_jobs_conf"
    format_str = VERTEX_AI_HYPERPARAMETER_TUNING_JOB_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIHyperparameterTuningJobListLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )


class VertexAIBatchPredictionJobLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI BatchPredictionJob link"""

    name = "Batch Prediction Job"
    key = "batch_prediction_job_conf"
    format_str = VERTEX_AI_BATCH_PREDICTION_JOB_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        batch_prediction_job_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIBatchPredictionJobLink.key,
            value={
                "batch_prediction_job_id": batch_prediction_job_id,
                "region": task_instance.region,
                "project_id": task_instance.project_id,
            },
        )


class VertexAIBatchPredictionJobListLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI BatchPredictionJobList link"""

    name = "Batch Prediction Job List"
    key = "batch_prediction_jobs_conf"
    format_str = VERTEX_AI_BATCH_PREDICTION_JOB_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIBatchPredictionJobListLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )


class VertexAIEndpointLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI Endpoint link"""

    name = "Endpoint"
    key = "endpoint_conf"
    format_str = VERTEX_AI_ENDPOINT_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
        endpoint_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIEndpointLink.key,
            value={
                "endpoint_id": endpoint_id,
                "region": task_instance.region,
                "project_id": task_instance.project_id,
            },
        )


class VertexAIEndpointListLink(BaseGoogleLink):
    """Helper class for constructing Vertex AI EndpointList link"""

    name = "Endpoint List"
    key = "endpoints_conf"
    format_str = VERTEX_AI_ENDPOINT_LIST_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIEndpointListLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )
