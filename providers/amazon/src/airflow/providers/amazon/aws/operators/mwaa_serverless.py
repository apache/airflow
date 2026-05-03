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
"""Amazon MWAA Serverless operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.sdk import Context


class MwaaServerlessStartWorkflowRunOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Start a new execution of an Amazon MWAA Serverless workflow.

    This operator triggers a workflow run that executes the tasks defined in the
    workflow. MWAA Serverless handles task scheduling, worker scaling, dependency
    resolution, and monitoring.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MwaaServerlessStartWorkflowRunOperator`

    :param workflow_arn: The ARN of the workflow to run. (templated)
    :param override_parameters: Optional parameters to override defaults for this run. (templated)
    :param workflow_version: Optional version of the workflow to execute. (templated)
    """

    template_fields: tuple[str, ...] = aws_template_fields(
        "workflow_arn",
        "override_parameters",
        "workflow_version",
    )
    template_fields_renderers = {"override_parameters": "json"}
    aws_hook_class = AwsBaseHook

    def __init__(
        self,
        *,
        workflow_arn: str,
        override_parameters: dict[str, Any] | None = None,
        workflow_version: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.workflow_arn = workflow_arn
        self.override_parameters = override_parameters
        self.workflow_version = workflow_version

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "mwaa-serverless"}

    def execute(self, context: Context) -> str:
        self.log.info("Starting MWAA Serverless workflow run for %s", self.workflow_arn)
        kwargs: dict[str, Any] = prune_dict(
            {
                "WorkflowArn": self.workflow_arn,
                "OverrideParameters": self.override_parameters,
                "WorkflowVersion": self.workflow_version,
            }
        )
        response = self.hook.conn.start_workflow_run(**kwargs)
        run_id = response["RunId"]
        self.log.info("Started workflow run %s (status: %s)", run_id, response.get("Status"))
        return run_id
