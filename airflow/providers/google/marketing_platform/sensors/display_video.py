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
"""Sensor for detecting the completion of DV360 reports."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow import AirflowException
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GoogleDisplayVideo360ReportSensor(BaseSensorOperator):
    """
    Sensor for detecting the completion of DV360 reports.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360ReportSensor`

    :param report_id: Report ID to delete.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "report_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        report_id: str,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.report_id = report_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        response = hook.get_query(query_id=self.report_id)
        if response and not response.get("metadata", {}).get("running"):
            return True
        return False


class GoogleDisplayVideo360GetSDFDownloadOperationSensor(BaseSensorOperator):
    """
    Sensor for detecting the completion of SDF operation.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360GetSDFDownloadOperationSensor`

    :param operation_name: The name of the operation resource
    :param api_version: The version of the api that will be requested for example 'v1'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields: Sequence[str] = (
        "operation_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        operation_name: str,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        mode: str = "reschedule",
        poke_interval: int = 60 * 5,
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.mode = mode
        self.poke_interval = poke_interval
        self.operation_name = operation_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        operation = hook.get_sdf_download_operation(operation_name=self.operation_name)

        if "error" in operation:
            raise AirflowException(f'The operation finished in error with {operation["error"]}')
        if operation and operation.get("done"):
            return True
        return False
