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

from typing import TYPE_CHECKING, Any

from airflow.providers.google.cloud.links.base import BASE_LINK, BaseGoogleLink

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


CLOUD_RUN_BASE_LINK = "/run"
CLOUD_RUN_JOB_EXECUTION_DETAILS_LINK = (
    CLOUD_RUN_BASE_LINK + "/jobs/details/{region}/{job_name}/executions?project={project_id}"
)


class CloudRunJobLoggingLink(BaseGoogleLink):
    """Helper class for constructing Cloud Run Job Logging link."""

    name = "Cloud Run Job Logging"
    key = "log_uri"
    format_str = "{log_uri}"

    @classmethod
    def persist(cls, context: Context, **value: Any) -> None:
        """Persist the complete URL expected by serialized operator links."""
        context["ti"].xcom_push(key=cls.key, value=value["log_uri"])


class CloudRunJobExecutionDetailsLink(BaseGoogleLink):
    """Helper class for constructing a Cloud Run Job execution details link."""

    name = "Cloud Run Job Execution Details"
    key = "cloud_run_job_execution_details"
    format_str = CLOUD_RUN_JOB_EXECUTION_DETAILS_LINK

    @classmethod
    def persist(cls, context: Context, **value: Any) -> None:
        """Persist the complete URL so it is usable while the execution is running."""
        context["ti"].xcom_push(
            key=cls.key,
            value=BASE_LINK + cls.format_str.format(**value),
        )
