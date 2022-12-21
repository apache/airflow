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
"""This module contains Google Cloud Run links."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

CLOUD_RUN_JOBS_BASE_LINK = "/run/jobs"
CLOUD_RUN_JOB_EXEC_LINK = (
    CLOUD_RUN_JOBS_BASE_LINK + "/executions/details/{region}/{exec_id}/general?project={project_id}"
)
CLOUD_RUN_JOB_LINK = "/details/{region}/{job_name}/executions?project={project_id}"


class CloudRunJobLink(BaseGoogleLink):
    """Helper class for constructing Cloud Run job Link"""

    name = "Cloud Run job"
    key = "cloud_run_job_config"
    format_str = CLOUD_RUN_JOB_LINK

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: Context,
        project_id: str | None,
        region: str | None,
        job_name: str | None,
    ):
        operator_instance.xcom_push(
            context,
            key=CloudRunJobLink.key,
            value={"project_id": project_id, "location": region, "job_name": job_name},
        )


class CloudRunJobExecutionLink(BaseGoogleLink):
    """Helper class for constructing Cloud Run job execution Link"""

    name = "Cloud Run job execution"
    key = "cloud_run_job_execution"
    format_str = CLOUD_RUN_JOB_EXEC_LINK

    @staticmethod
    def persist(
        operator_instance: BaseOperator,
        context: Context,
        project_id: str | None,
        region: str | None,
        execution_id: str | None,
    ):
        operator_instance.xcom_push(
            context,
            key=CloudRunJobExecutionLink.key,
            value={"project_id": project_id, "location": region, "execution_id": execution_id},
        )
