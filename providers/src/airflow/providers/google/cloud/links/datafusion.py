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
"""This module contains Google Data Fusion links."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from airflow.models import BaseOperatorLink, XCom

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


BASE_LINK = "https://console.cloud.google.com/data-fusion"
DATAFUSION_INSTANCE_LINK = (
    BASE_LINK + "/locations/{region}/instances/{instance_name}?project={project_id}"
)
DATAFUSION_PIPELINES_LINK = "{uri}/cdap/ns/{namespace}/pipelines"
DATAFUSION_PIPELINE_LINK = "{uri}/pipelines/ns/{namespace}/view/{pipeline_name}"


class BaseGoogleLink(BaseOperatorLink):
    """
    Link for Google operators.

    Prevent adding ``https://console.cloud.google.com`` in front of every link
    where URI is used.
    """

    name: ClassVar[str]
    key: ClassVar[str]
    format_str: ClassVar[str]

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        conf = XCom.get_value(key=self.key, ti_key=ti_key)

        if not conf:
            return ""

        # Add a default value for the 'namespace' parameter for backward compatibility.
        conf.setdefault("namespace", "default")

        return self.format_str.format(**conf)


class DataFusionInstanceLink(BaseGoogleLink):
    """Helper class for constructing Data Fusion Instance link."""

    name = "Data Fusion Instance"
    key = "instance_conf"
    format_str = DATAFUSION_INSTANCE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        location: str,
        instance_name: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataFusionInstanceLink.key,
            value={
                "region": location,
                "instance_name": instance_name,
                "project_id": project_id,
            },
        )


class DataFusionPipelineLink(BaseGoogleLink):
    """Helper class for constructing Data Fusion Pipeline link."""

    name = "Data Fusion Pipeline"
    key = "pipeline_conf"
    format_str = DATAFUSION_PIPELINE_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        uri: str,
        pipeline_name: str,
        namespace: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataFusionPipelineLink.key,
            value={
                "uri": uri,
                "pipeline_name": pipeline_name,
                "namespace": namespace,
            },
        )


class DataFusionPipelinesLink(BaseGoogleLink):
    """Helper class for constructing list of Data Fusion Pipelines link."""

    name = "Data Fusion Pipelines List"
    key = "pipelines_conf"
    format_str = DATAFUSION_PIPELINES_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        uri: str,
        namespace: str,
    ):
        task_instance.xcom_push(
            context=context,
            key=DataFusionPipelinesLink.key,
            value={
                "uri": uri,
                "namespace": namespace,
            },
        )
