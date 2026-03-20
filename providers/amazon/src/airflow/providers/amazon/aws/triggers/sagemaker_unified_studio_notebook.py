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

"""Trigger for monitoring SageMaker Unified Studio Notebook runs asynchronously."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookHook,
)
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

TWELVE_HOURS_IN_MINUTES = 12 * 60


class SageMakerUnifiedStudioNotebookTrigger(AwsBaseWaiterTrigger):
    """
    Watches an asynchronous notebook run, triggering when it reaches a terminal state.

    Uses a custom boto waiter (``notebook_run_complete``) defined in
    ``waiters/datazone.json`` to poll the DataZone ``GetNotebookRun`` API.

    :param notebook_run_id: The ID of the notebook run to monitor.
    :param domain_id: The ID of the DataZone domain.
    :param project_id: The ID of the DataZone project.
    :param waiter_delay: Interval in seconds between polls (default: 10).
    :param waiter_max_attempts: Maximum number of poll attempts.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param timeout_configuration: Optional timeout settings. When provided, the maximum
        number of poll attempts is derived from ``run_timeout_in_minutes * 60 / waiter_delay``.
        Defaults to a 12-hour timeout when omitted.
        Example: {"run_timeout_in_minutes": 720}
    """

    def __init__(
        self,
        notebook_run_id: str,
        domain_id: str,
        project_id: str,
        waiter_delay: int = 10,
        timeout_configuration: dict | None = None,
        aws_conn_id: str | None = None,
        **kwargs,
    ):
        run_timeout = (timeout_configuration or {}).get("run_timeout_in_minutes", TWELVE_HOURS_IN_MINUTES)
        waiter_max_attempts = int(run_timeout * 60 / waiter_delay)

        self.notebook_run_id = notebook_run_id
        self.domain_id = domain_id
        self.project_id = project_id
        self.timeout_configuration = timeout_configuration

        super().__init__(
            serialized_fields={
                "notebook_run_id": notebook_run_id,
                "domain_id": domain_id,
                "project_id": project_id,
                "timeout_configuration": timeout_configuration,
            },
            waiter_name="notebook_run_complete",
            waiter_args={
                "domain_id": domain_id,
                "notebook_run_id": notebook_run_id,
            },
            failure_message=f"Notebook run {notebook_run_id} failed",
            status_message=f"Notebook run {notebook_run_id} is currently",
            status_queries=["status"],
            return_key="notebook_run_id",
            return_value=notebook_run_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return SageMakerUnifiedStudioNotebookHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
