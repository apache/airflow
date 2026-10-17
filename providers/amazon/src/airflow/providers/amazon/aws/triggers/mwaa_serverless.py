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
"""Amazon MWAA Serverless triggers."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

WORKFLOW_RUN_STATES = frozenset(
    {"STARTING", "QUEUED", "RUNNING", "STOPPING", "SUCCESS", "FAILED", "TIMEOUT", "STOPPED"}
)
DEFAULT_SUCCESS_STATES = frozenset({"SUCCESS"})
DEFAULT_FAILURE_STATES = frozenset({"FAILED", "TIMEOUT", "STOPPED"})


def get_failure_states(success_states: set[str], failure_states: Collection[str] | None) -> set[str]:
    """
    Return the failure states to use for a workflow run.

    The defaults leave out any state the caller listed in ``success_states``, so for example
    ``success_states={"SUCCESS", "STOPPED"}`` treats a stopped run as successful instead of failed.
    """
    if failure_states:
        return set(failure_states)
    return set(DEFAULT_FAILURE_STATES - success_states)


class MwaaServerlessWorkflowRunCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an Amazon MWAA Serverless workflow run reaches a terminal state.

    :param workflow_arn: The ARN of the workflow.
    :param run_id: The ID of the workflow run to wait for.
    :param success_states: Collection of run states that mark the run as successful.
        Default: ``{"SUCCESS"}``.
    :param failure_states: Collection of run states that mark the run as failed.
        Default: ``{"FAILED", "TIMEOUT", "STOPPED"}``, minus any state listed in ``success_states``.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 720)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    aws_hook_class = AwsBaseHook

    def __init__(
        self,
        *,
        workflow_arn: str,
        run_id: str,
        success_states: Collection[str] | None = None,
        failure_states: Collection[str] | None = None,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 720,
        **kwargs,
    ) -> None:
        self.success_states = set(success_states) if success_states else set(DEFAULT_SUCCESS_STATES)
        self.failure_states = get_failure_states(self.success_states, failure_states)

        if self.success_states & self.failure_states:
            raise ValueError("success_states and failure_states must not have any values in common")

        in_progress_states = WORKFLOW_RUN_STATES - self.success_states - self.failure_states

        super().__init__(
            serialized_fields={
                "workflow_arn": workflow_arn,
                "run_id": run_id,
                "success_states": sorted(self.success_states),
                "failure_states": sorted(self.failure_states),
            },
            waiter_name="workflow_run_complete",
            waiter_args={"WorkflowArn": workflow_arn, "RunId": run_id},
            failure_message=f"MWAA Serverless workflow run {run_id} of {workflow_arn} failed",
            status_message="State of workflow run",
            status_queries=["RunDetail.RunState", "RunDetail.ErrorMessage"],
            return_key="run_id",
            return_value=run_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            waiter_config_overrides={
                "acceptors": _build_waiter_acceptors(
                    success_states=self.success_states,
                    failure_states=self.failure_states,
                    in_progress_states=in_progress_states,
                )
            },
            **kwargs,
        )

    @property
    def _hook_parameters(self) -> dict[str, Any]:
        return {**super()._hook_parameters, "client_type": "mwaa-serverless"}


def _build_waiter_acceptors(
    success_states: set[str], failure_states: set[str], in_progress_states: Collection[str]
) -> list[dict[str, str]]:
    return [
        {
            "matcher": "path",
            "argument": "RunDetail.RunState",
            "expected": state,
            "state": waiter_state,
        }
        for states, waiter_state in (
            (success_states, "success"),
            (failure_states, "failure"),
            (in_progress_states, "retry"),
        )
        for state in sorted(states)
    ]
