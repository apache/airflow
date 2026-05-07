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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
    NOTEBOOK_OUTPUT_KEY_PREFIX,
    NOTEBOOK_RUN_IN_PROGRESS_STATES,
    NOTEBOOK_RUN_SUCCESS_STATES,
    SageMakerUnifiedStudioNotebookHook,
)
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.sdk import Context


class SageMakerUnifiedStudioNotebookSensor(AwsBaseSensor[SageMakerUnifiedStudioNotebookHook]):
    """
    Polls a SageMaker Unified Studio notebook execution until it reaches a terminal state.

    'SUCCEEDED', 'FAILED', 'STOPPED'

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.sensors.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookSensor,
        )

        notebook_sensor = SageMakerUnifiedStudioNotebookSensor(
            task_id="wait_for_notebook",
            domain_identifier="dzd_example",
            owning_project_identifier="proj_example",
            notebook_run_id="nr-1234567890",
        )

    :param domain_identifier: The ID of the SageMaker Unified Studio domain containing the notebook.
    :param owning_project_identifier: The ID of the SageMaker Unified Studio project containing the notebook.
    :param notebook_run_id: The ID of the notebook run to monitor.
        This is returned by the ``SageMakerUnifiedStudioNotebookOperator``.
    :param notebook_identifier: The ID of the notebook that was executed.
        Required to read notebook outputs from S3 after the run completes.
    """

    aws_hook_class = SageMakerUnifiedStudioNotebookHook
    template_fields: Sequence[str] = aws_template_fields(
        "domain_identifier",
        "endpoint_url",
        "notebook_identifier",
        "notebook_run_id",
        "owning_project_identifier",
    )

    def __init__(
        self,
        *,
        domain_identifier: str,
        owning_project_identifier: str,
        notebook_run_id: str,
        notebook_identifier: str,
        endpoint_url: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.domain_identifier = domain_identifier
        self.owning_project_identifier = owning_project_identifier
        self.notebook_run_id = notebook_run_id
        self.notebook_identifier = notebook_identifier
        self.endpoint_url = endpoint_url

    @property
    def _hook_parameters(self):
        params = super()._hook_parameters
        if self.endpoint_url:
            params["endpoint_url"] = self.endpoint_url
        return params

    # override from base sensor
    def poke(self, context: Context) -> bool:
        response = self.hook.get_notebook_run(self.notebook_run_id, domain_identifier=self.domain_identifier)
        status = response.get("status", "")

        if status in NOTEBOOK_RUN_SUCCESS_STATES:
            self.log.info("Exiting notebook run %s. State: %s", self.notebook_run_id, status)
            return True

        if status in NOTEBOOK_RUN_IN_PROGRESS_STATES:
            return False

        error_message = f"Exiting notebook run {self.notebook_run_id}. State: {status}"
        self.log.info(error_message)
        raise RuntimeError(error_message)

    def execute(self, context: Context):
        # This will invoke poke method in the base sensor
        self.log.info("Polling notebook run %s in domain %s", self.notebook_run_id, self.domain_identifier)
        super().execute(context=context)

        # After successful completion, read notebook outputs from S3 and push to xcom
        outputs = self.hook.get_notebook_outputs(
            notebook_identifier=self.notebook_identifier,
            notebook_run_id=self.notebook_run_id,
            owning_project_identifier=self.owning_project_identifier,
        )
        if outputs:
            for key, value in outputs.items():
                context["ti"].xcom_push(key=f"{NOTEBOOK_OUTPUT_KEY_PREFIX}.{key}", value=value)
