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
"""This module contains a Cloud Composer sensor."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.triggers.cloud_composer import CloudComposerExecutionTrigger
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudComposerEnvironmentSensor(BaseSensorOperator):
    """
    Check the status of the Cloud Composer Environment task

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param operation_name: The name of the operation resource
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate, if any. For this to work, the service account making the
        request must have  domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param pooling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
    """

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        operation_name: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        pooling_period_seconds: int = 30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.operation_name = operation_name
        self.pooling_period_seconds = pooling_period_seconds
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            trigger=CloudComposerExecutionTrigger(
                project_id=self.project_id,
                region=self.region,
                operation_name=self.operation_name,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                delegate_to=self.delegate_to,
                pooling_period_seconds=self.pooling_period_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: dict[str, Any], event: dict[str, str] | None = None) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event.get("operation_done"):
                return event["operation_done"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
