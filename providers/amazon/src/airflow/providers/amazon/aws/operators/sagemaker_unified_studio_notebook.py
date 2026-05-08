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
This module contains the Amazon SageMaker Unified Studio Notebook operator.

This operator supports asynchronous notebook execution in SageMaker Unified
Studio.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio_notebook import (
    NOTEBOOK_OUTPUT_KEY_PREFIX,
    SageMakerUnifiedStudioNotebookHook,
)
from airflow.providers.amazon.aws.links.sagemaker_unified_studio import (
    SageMakerUnifiedStudioLink,
)
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio_notebook import (
    SageMakerUnifiedStudioNotebookTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class SageMakerUnifiedStudioNotebookOperator(AwsBaseOperator[SageMakerUnifiedStudioNotebookHook]):
    """
    Execute a notebook in SageMaker Unified Studio.

    This operator calls the DataZone StartNotebookRun API to kick off
    headless notebook execution. When not configured otherwise, polls
    the GetNotebookRun API until the run reaches a terminal state.

    Examples:
     .. code-block:: python

        from airflow.providers.amazon.aws.operators.sagemaker_unified_studio_notebook import (
            SageMakerUnifiedStudioNotebookOperator,
        )

        notebook_operator = SageMakerUnifiedStudioNotebookOperator(
            task_id="run_notebook",
            notebook_identifier="nb-1234567890",
            domain_identifier="dzd_example",
            owning_project_identifier="proj_example",
            notebook_parameters={"param1": "value1"},
            compute_configuration={"instanceType": "sc.m5.large"},
            timeout_configuration={"runTimeoutInMinutes": 1440},
        )

    :param notebook_identifier: The ID of the notebook to execute.
    :param domain_identifier: The ID of the SageMaker Unified Studio domain containing the notebook.
    :param owning_project_identifier: The ID of the SageMaker Unified Studio project containing the notebook.
    :param client_token: Optional idempotency token. Auto-generated if not provided.
    :param notebook_parameters: Optional dict of parameters to pass to the notebook.
    :param compute_configuration: Optional compute config.
        Example: {"instanceType": "sc.m5.large"}
    :param timeout_configuration: Optional timeout settings.
        Example: {"runTimeoutInMinutes": 1440}
    :param wait_for_completion: If True, wait for the notebook run to finish before
        completing the task. If False, the operator returns immediately after starting
        the run. (default: True)
    :param waiter_delay: Interval in seconds to poll the notebook run status (default: 10).
    :param deferrable: If True, the operator will defer polling to the trigger,
        freeing up the worker slot while waiting. (default: False)
    :param endpoint_url: Optional custom endpoint URL for the DataZone API.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerUnifiedStudioNotebookOperator`
    """

    operator_extra_links = (SageMakerUnifiedStudioLink(),)
    aws_hook_class = SageMakerUnifiedStudioNotebookHook
    template_fields: Sequence[str] = aws_template_fields(
        "client_token",
        "compute_configuration",
        "domain_identifier",
        "notebook_identifier",
        "notebook_parameters",
        "owning_project_identifier",
        "timeout_configuration",
    )

    def __init__(
        self,
        *,
        notebook_identifier: str,
        domain_identifier: str,
        owning_project_identifier: str,
        client_token: str | None = None,
        notebook_parameters: dict | None = None,
        compute_configuration: dict | None = None,
        timeout_configuration: dict | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 10,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        endpoint_url: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.notebook_identifier = notebook_identifier
        self.domain_identifier = domain_identifier
        self.owning_project_identifier = owning_project_identifier
        self.client_token = client_token
        self.notebook_parameters = notebook_parameters
        self.compute_configuration = compute_configuration
        self.timeout_configuration = timeout_configuration
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.deferrable = deferrable
        self.endpoint_url = endpoint_url

    @property
    def _hook_parameters(self):
        params = super()._hook_parameters
        if self.endpoint_url:
            params["endpoint_url"] = self.endpoint_url
        return params

    def _push_notebook_outputs(self, context: Context, notebook_run_id: str) -> dict[str, Any]:
        """
        Read notebook outputs from S3 and push each key-value pair to xcom.

        Each output becomes a top-level xcom key so downstream tasks can
        reference them as ``task.output.<key>``.

        :param context: The Airflow context.
        :param notebook_run_id: The ID of the completed notebook run.
        :return: A flat dict containing notebook_run_id and all notebook outputs.
        """
        result: dict[str, Any] = {"notebook_run_id": notebook_run_id}
        context["ti"].xcom_push(key="notebook_run_id", value=notebook_run_id)
        outputs = self.hook.get_notebook_outputs(
            notebook_identifier=self.notebook_identifier,
            notebook_run_id=notebook_run_id,
            owning_project_identifier=self.owning_project_identifier,
        )
        if outputs:
            for key, value in outputs.items():
                context["ti"].xcom_push(key=f"{NOTEBOOK_OUTPUT_KEY_PREFIX}.{key}", value=value)
            result.update({f"{NOTEBOOK_OUTPUT_KEY_PREFIX}.{k}": v for k, v in outputs.items()})
        return result

    def execute(self, context: Context):
        SageMakerUnifiedStudioLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
        )
        workflow_name = context["dag"].dag_id  # Workflow name is the same as the dag_id
        response = self.hook.start_notebook_run(
            notebook_identifier=self.notebook_identifier,
            domain_identifier=self.domain_identifier,
            owning_project_identifier=self.owning_project_identifier,
            client_token=self.client_token,
            notebook_parameters=self.notebook_parameters,
            compute_configuration=self.compute_configuration,
            timeout_configuration=self.timeout_configuration,
            workflow_name=workflow_name,
        )
        notebook_run_id = response["id"]
        self.log.info("Started notebook run %s for notebook %s", notebook_run_id, self.notebook_identifier)

        if self.deferrable:
            self.defer(
                trigger=SageMakerUnifiedStudioNotebookTrigger(
                    notebook_run_id=notebook_run_id,
                    domain_identifier=self.domain_identifier,
                    owning_project_identifier=self.owning_project_identifier,
                    waiter_delay=self.waiter_delay,
                    timeout_configuration=self.timeout_configuration,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.hook.wait_for_notebook_run(
                notebook_run_id,
                domain_identifier=self.domain_identifier,
                waiter_delay=self.waiter_delay,
                timeout_configuration=self.timeout_configuration,
            )
            return self._push_notebook_outputs(context, notebook_run_id)

        return {"notebook_run_id": notebook_run_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, Any]:
        validated_event = validate_execute_complete_event(event)

        if validated_event.get("status") != "success":
            raise RuntimeError(f"Notebook run did not succeed: {validated_event}")

        notebook_run_id = validated_event["notebook_run_id"]
        self.log.info("Notebook run %s completed for notebook %s", notebook_run_id, self.notebook_identifier)
        return self._push_notebook_outputs(context, notebook_run_id)
