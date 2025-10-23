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

"""This module contains the Amazon SageMaker Unified Studio Notebook operator."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio import (
    SageMakerNotebookHook,
)
from airflow.providers.amazon.aws.links.sagemaker_unified_studio import (
    SageMakerUnifiedStudioLink,
)
from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio import (
    SageMakerNotebookJobTrigger,
)
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SageMakerNotebookOperator(BaseOperator):
    """
    Provides Artifact execution functionality for Sagemaker Unified Studio Workflows.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import SageMakerNotebookOperator

        notebook_operator = SageMakerNotebookOperator(
            task_id="notebook_task",
            input_config={"input_path": "path/to/notebook.ipynb", "input_params": ""},
            output_config={"output_format": "ipynb"},
            wait_for_completion=True,
            waiter_delay=10,
            waiter_max_attempts=1440,
        )

    :param task_id: A unique, meaningful id for the task.
    :param input_config: Configuration for the input file. Input path should be specified as a relative path.
        The provided relative path will be automatically resolved to an absolute path within
        the context of the user's home directory in the IDE. Input params should be a dict.
        Example: {'input_path': 'folder/input/notebook.ipynb', 'input_params':{'key': 'value'}}
    :param output_config:  Configuration for the output format. It should include an output_format parameter to control
        the format of the notebook execution output.
        Example: {"output_formats": ["NOTEBOOK"]}
    :param compute: compute configuration to use for the artifact execution. This is a required attribute
        if the execution is on a remote compute.
        Example: { "InstanceType": "ml.m5.large", "VolumeSizeInGB": 30, "VolumeKmsKeyId": "", "ImageUri": "string", "ContainerEntrypoint": [ "string" ]}
    :param termination_condition: conditions to match to terminate the remote execution.
        Example: { "MaxRuntimeInSeconds": 3600 }
    :param tags: tags to be associated with the remote execution runs.
        Example: { "md_analytics": "logs" }
    :param wait_for_completion: Indicates whether to wait for the notebook execution to complete. If True, wait for completion; if False, don't wait.
    :param waiter_delay: Interval in seconds to check the notebook execution status.
    :param waiter_max_attempts: Number of attempts to wait before returning FAILED.
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerNotebookOperator`
    """

    operator_extra_links = (SageMakerUnifiedStudioLink(),)

    def __init__(
        self,
        task_id: str,
        input_config: dict,
        output_config: dict | None = None,
        compute: dict | None = None,
        termination_condition: dict | None = None,
        tags: dict | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 10,
        waiter_max_attempts: int = 1440,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.execution_name = task_id
        self.input_config = input_config
        self.output_config = output_config or {"output_formats": ["NOTEBOOK"]}
        self.compute = compute or {}
        self.termination_condition = termination_condition or {}
        self.tags = tags or {}
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable
        self.input_kwargs = kwargs

    @cached_property
    def notebook_execution_hook(self):
        if not self.input_config:
            raise AirflowException("input_config is required")

        if "input_path" not in self.input_config:
            raise AirflowException("input_path is a required field in the input_config")

        return SageMakerNotebookHook(
            input_config=self.input_config,
            output_config=self.output_config,
            execution_name=self.execution_name,
            compute=self.compute,
            termination_condition=self.termination_condition,
            tags=self.tags,
            waiter_delay=self.waiter_delay,
            waiter_max_attempts=self.waiter_max_attempts,
        )

    def execute(self, context: Context):
        notebook_execution = self.notebook_execution_hook.start_notebook_execution()
        execution_id = notebook_execution["execution_id"]

        if self.deferrable:
            self.defer(
                trigger=SageMakerNotebookJobTrigger(
                    execution_id=execution_id,
                    execution_name=self.execution_name,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            response = self.notebook_execution_hook.wait_for_execution_completion(execution_id, context)
            status = response["Status"]
            log_info_message = (
                f"Notebook Execution: {self.execution_name} Status: {status}. Run Id: {execution_id}"
            )
            self.log.info(log_info_message)
