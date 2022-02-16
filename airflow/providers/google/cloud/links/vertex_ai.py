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
"""This module contains a links for Vertex AI assets."""

from datetime import datetime
from typing import TYPE_CHECKING

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.xcom import XCom

if TYPE_CHECKING:
    from airflow.utils.context import Context

VERTEX_AI_BASE_LINK = "https://console.cloud.google.com/vertex-ai"
VERTEX_AI_MODEL_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/models/{model_id}/deploy?project={project_id}"
)
VERTEX_AI_TRAINING_PIPELINES_LINK = VERTEX_AI_BASE_LINK + "/training/training-pipelines?project={project_id}"
VERTEX_AI_DATASET_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/datasets/{dataset_id}/analyze?project={project_id}"
)
VERTEX_AI_DATASET_LIST_LINK = VERTEX_AI_BASE_LINK + "/datasets?project={project_id}"


class VertexAIModelLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Model link"""

    name = "Vertex AI Model"
    key = "model_conf"

    @staticmethod
    def persist(
        context: "Context",
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

    def get_link(self, operator: BaseOperator, dttm: datetime):
        model_conf = XCom.get_one(
            key=VertexAIModelLink.key,
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )
        return (
            VERTEX_AI_MODEL_LINK.format(
                region=model_conf["region"],
                model_id=model_conf["model_id"],
                project_id=model_conf["project_id"],
            )
            if model_conf
            else ""
        )


class VertexAITrainingPipelinesLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Training Pipelines link"""

    name = "Vertex AI Training Pipelines"
    key = "pipelines_conf"

    @staticmethod
    def persist(
        context: "Context",
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAITrainingPipelinesLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )

    def get_link(self, operator: BaseOperator, dttm: datetime):
        pipelines_conf = XCom.get_one(
            key=VertexAITrainingPipelinesLink.key,
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )
        return (
            VERTEX_AI_TRAINING_PIPELINES_LINK.format(
                project_id=pipelines_conf["project_id"],
            )
            if pipelines_conf
            else ""
        )


class VertexAIDatasetLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Dataset link"""

    name = "Dataset"
    key = "dataset_conf"

    @staticmethod
    def persist(context: "Context", task_instance, dataset_id: str):
        task_instance.xcom_push(
            context=context,
            key=VertexAIDatasetLink.key,
            value={
                "dataset_id": dataset_id,
                "region": task_instance.region,
                "project_id": task_instance.project_id,
            },
        )

    def get_link(self, operator: BaseOperator, dttm: datetime):
        dataset_conf = XCom.get_one(
            key=VertexAIDatasetLink.key,
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )
        return (
            VERTEX_AI_DATASET_LINK.format(
                region=dataset_conf["region"],
                dataset_id=dataset_conf["dataset_id"],
                project_id=dataset_conf["project_id"],
            )
            if dataset_conf
            else ""
        )


class VertexAIDatasetListLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Datasets Link"""

    name = "Dataset List"
    key = "datasets_conf"

    @staticmethod
    def persist(
        context: "Context",
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=VertexAIDatasetListLink.key,
            value={
                "project_id": task_instance.project_id,
            },
        )

    def get_link(self, operator: BaseOperator, dttm: datetime):
        datasets_conf = XCom.get_one(
            key=VertexAIDatasetListLink.key,
            dag_id=operator.dag.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )
        return (
            VERTEX_AI_DATASET_LIST_LINK.format(
                project_id=datasets_conf["project_id"],
            )
            if datasets_conf
            else ""
        )
