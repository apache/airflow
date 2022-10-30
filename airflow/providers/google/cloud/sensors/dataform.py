#
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
"""This module contains a Google Cloud Dataform sensor."""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataform import DataformHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataformWorkflowInvocationStateSensor(BaseSensorOperator):
    """
    Checks for the status of a Workflow Invocation in Google Cloud Dataform.

    :param project_id: Required, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param region: Required, The location of the Dataform workflow invocation (for example europe-west1).
    :param repository_id: Required. The ID of the Dataform repository that the task belongs to.
    :param workflow_invocation_id: Required, ID of the workflow invocation to be checked.
    :param expected_statuses: The expected state of the operation.
        See:
        https://cloud.google.com/python/docs/reference/dataform/latest/google.cloud.dataform_v1beta1.types.WorkflowInvocation.State
    :param failure_statuses: State that will terminate the sensor with an exception
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled. See:
        https://developers.google.com/identity/protocols/oauth2/service-account#delegatingauthority
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("workflow_invocation_id",)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        repository_id: str,
        workflow_invocation_id: str,
        expected_statuses: set[int] | int,
        failure_statuses: Iterable[int] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.repository_id = repository_id
        self.workflow_invocation_id = workflow_invocation_id
        self.expected_statuses = (
            {expected_statuses} if isinstance(expected_statuses, int) else expected_statuses
        )
        self.failure_statuses = failure_statuses
        self.project_id = project_id
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook: DataformHook | None = None

    def poke(self, context: Context) -> bool:
        self.hook = DataformHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        workflow_invocation = self.hook.get_workflow_invocation(
            project_id=self.project_id,
            region=self.region,
            repository_id=self.repository_id,
            workflow_invocation_id=self.workflow_invocation_id,
        )
        workflow_status = workflow_invocation.state
        if workflow_status is not None:
            if self.failure_statuses and workflow_status in self.failure_statuses:
                raise AirflowException(
                    f"Workflow Invocation with id '{self.workflow_invocation_id}' "
                    f"state is: {workflow_status}. Terminating sensor..."
                )

        return workflow_status in self.expected_statuses
