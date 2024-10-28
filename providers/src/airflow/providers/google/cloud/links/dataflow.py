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
"""This module contains Google Dataflow links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context

DATAFLOW_BASE_LINK = "/dataflow/jobs"
DATAFLOW_JOB_LINK = DATAFLOW_BASE_LINK + "/{region}/{job_id}?project={project_id}"

DATAFLOW_PIPELINE_BASE_LINK = "/dataflow/pipelines"
DATAFLOW_PIPELINE_LINK = (
    DATAFLOW_PIPELINE_BASE_LINK + "/{location}/{pipeline_name}?project={project_id}"
)


class DataflowJobLink(BaseGoogleLink):
    """Helper class for constructing Dataflow Job Link."""

    name = "Dataflow Job"
    key = "dataflow_job_config"
    format_str = DATAFLOW_JOB_LINK

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: Context,
        project_id: str | None,
        region: str | None,
        job_id: str | None,
    ):
        operator_instance.xcom_push(
            context,
            key=DataflowJobLink.key,
            value={"project_id": project_id, "region": region, "job_id": job_id},
        )


class DataflowPipelineLink(BaseGoogleLink):
    """Helper class for constructing Dataflow Pipeline Link."""

    name = "Dataflow Pipeline"
    key = "dataflow_pipeline_config"
    format_str = DATAFLOW_PIPELINE_LINK

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: Context,
        project_id: str | None,
        location: str | None,
        pipeline_name: str | None,
    ):
        operator_instance.xcom_push(
            context,
            key=DataflowPipelineLink.key,
            value={
                "project_id": project_id,
                "location": location,
                "pipeline_name": pipeline_name,
            },
        )
