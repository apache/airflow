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

from typing import TYPE_CHECKING, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.workflows.executions_v1beta import Execution

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.workflows import WorkflowsHook
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.utils.context import Context


class WorkflowExecutionSensor(BaseSensorOperator):
    """
    Checks state of an execution for the given ``workflow_id`` and ``execution_id``.

    :param workflow_id: Required. The ID of the workflow.
    :param execution_id: Required. The ID of the execution.
    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param location: Required. The Cloud Dataproc region in which to handle the request.
    :param success_states: Execution states to be considered as successful, by default
        it's only ``SUCCEEDED`` state
    :param failure_states: Execution states to be considered as failures, by default
        they are ``FAILED`` and ``CANCELLED`` states.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param request_timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = ("location", "workflow_id", "execution_id")

    def __init__(
        self,
        *,
        workflow_id: str,
        execution_id: str,
        location: str,
        project_id: str = PROVIDE_PROJECT_ID,
        success_states: set[Execution.State] | None = None,
        failure_states: set[Execution.State] | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        request_timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.success_states = success_states or {
            Execution.State(Execution.State.SUCCEEDED)
        }
        self.failure_states = failure_states or {
            Execution.State(Execution.State.FAILED),
            Execution.State(Execution.State.CANCELLED),
        }
        self.workflow_id = workflow_id
        self.execution_id = execution_id
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.request_timeout = request_timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context):
        hook = WorkflowsHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.log.info(
            "Checking state of execution %s for workflow %s",
            self.execution_id,
            self.workflow_id,
        )
        execution: Execution = hook.get_execution(
            workflow_id=self.workflow_id,
            execution_id=self.execution_id,
            location=self.location,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.request_timeout,
            metadata=self.metadata,
        )

        state = execution.state
        if state in self.failure_states:
            message = (
                f"Execution {self.execution_id} for workflow {self.execution_id} "
                f"failed and is in `{state}` state"
            )
            raise AirflowException(message)

        if state in self.success_states:
            self.log.info(
                "Execution %s for workflow %s completed with state: %s",
                self.execution_id,
                self.workflow_id,
                state,
            )
            return True

        self.log.info(
            "Execution %s for workflow %s does not completed yet, current state: %s",
            self.execution_id,
            self.workflow_id,
            state,
        )
        return False
