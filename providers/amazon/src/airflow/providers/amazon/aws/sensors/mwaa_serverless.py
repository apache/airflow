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
"""Amazon MWAA Serverless sensors."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.sdk import Context


class MwaaServerlessWorkflowRunSensor(AwsBaseSensor[AwsBaseHook]):
    """
    Wait for an Amazon MWAA Serverless workflow run to reach a terminal state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:MwaaServerlessWorkflowRunSensor`

    :param workflow_arn: The ARN of the workflow. (templated)
    :param run_id: The ID of the workflow run to monitor. (templated)
    :param success_states: Set of states considered successful. Default: ``{"SUCCESS"}``.
    :param failure_states: Set of states that raise an exception.
        Default: ``{"FAILED", "TIMEOUT", "STOPPED"}``.
    """

    aws_hook_class = AwsBaseHook
    template_fields: tuple[str, ...] = aws_template_fields("workflow_arn", "run_id")

    def __init__(
        self,
        *,
        workflow_arn: str,
        run_id: str,
        success_states: set[str] | None = None,
        failure_states: set[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.workflow_arn = workflow_arn
        self.run_id = run_id
        self.success_states = success_states or {"SUCCESS"}
        self.failure_states = failure_states or {"FAILED", "TIMEOUT", "STOPPED"}

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "mwaa-serverless"}

    def poke(self, context: Context) -> bool:
        response = self.hook.conn.get_workflow_run(WorkflowArn=self.workflow_arn, RunId=self.run_id)
        state = response["RunDetail"]["RunState"]
        self.log.info("Workflow run %s state: %s", self.run_id, state)

        if state in self.failure_states:
            error_msg = response["RunDetail"].get("ErrorMessage", "")
            raise RuntimeError(f"Workflow run {self.run_id} failed with state {state}: {error_msg}")

        return state in self.success_states
