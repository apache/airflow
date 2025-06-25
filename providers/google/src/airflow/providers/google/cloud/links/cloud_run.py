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

from airflow.providers.google.cloud.links.base import BaseGoogleLink

CLOUD_RUN_BASE_LINK = "/run"
CLOUD_RUN_JOB_EXECUTION_LINK = (
    CLOUD_RUN_BASE_LINK + "/jobs/execution/{region}/{execution_name}"
    "?project={project_id}"
)


class CloudRunJobExecutionLink(BaseGoogleLink):
    """Helper class for constructing Cloud Run Job Execution Link."""

    name = "Cloud Run Job Execution"
    key = "cloud_run_job_execution"
    format_str = CLOUD_RUN_JOB_EXECUTION_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        project_id: str,
        region: str,
        execution_name: str,
    ):
        task_instance.xcom_push(
            context,
            key=CloudRunJobExecutionLink.key,
            value={
                "project_id": project_id,
                "region": region,
                "execution_name": execution_name,
            },
        )


class CloudRunJobLoggingLink(BaseGoogleLink):
    """Helper class for constructing Cloud Run Job Logging link."""

    name = "Cloud Run Job Logging"
    key = "log_uri"
    format_str = "{log_uri}"
