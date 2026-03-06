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

"""
This module contains the Amazon SageMaker Unified Studio Notebook Run sensor.

This sensor polls the DataZone GetNotebookRun API until the notebook run
reaches a terminal state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookHook,
)
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor

if TYPE_CHECKING:
    from airflow.sdk import Context


class SageMakerUnifiedStudioNotebookSensor(AwsBaseSensor[SageMakerUnifiedStudioNotebookHook]):
    """
    Polls a SageMakerUnifiedStudio Workflow asynchronous Notebook execution until it reaches a terminal state.

    'SUCCEEDED', 'FAILED', 'STOPPED'

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookSensor,
        )

        notebook_sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="wait_for_notebook",
            domain_id="dzd_example",
            project_id="proj_example",
            notebook_run_id="nr-1234567890",
        )

    :param domain_id: The ID of the SageMaker Unified Studio domain containing the notebook.
    :param project_id: The ID of the SageMaker Unified Studio project containing the notebook.
    :param notebook_run_id: The ID of the notebook run to monitor.
        This is returned by the ``SageMakerUnifiedStudioNotebookOperator``.
    """

    aws_hook_class = SageMakerUnifiedStudioNotebookHook

    def __init__(
        self,
        *,
        domain_id: str,
        project_id: str,
        notebook_run_id: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_id = domain_id
        self.project_id = project_id
        self.notebook_run_id = notebook_run_id
        self.success_states = ["SUCCEEDED"]
        self.in_progress_states = ["QUEUED", "STARTING", "RUNNING", "STOPPING"]

    # override from base sensor
    def poke(self, context: Context) -> bool:
        response = self.hook.get_notebook_run(self.notebook_run_id, domain_id=self.domain_id)
        status = response.get("status", "")

        if status in self.success_states:
            self.log.info("Exiting notebook run %s. State: %s", self.notebook_run_id, status)
            return True

        if status in self.in_progress_states:
            return False

        error_message = f"Exiting notebook run {self.notebook_run_id}. State: {status}"
        self.log.info(error_message)
        raise RuntimeError(error_message)

    def execute(self, context: Context):
        # This will invoke poke method in the base sensor
        self.log.info("Polling notebook run %s in domain %s", self.notebook_run_id, self.domain_id)
        super().execute(context=context)
