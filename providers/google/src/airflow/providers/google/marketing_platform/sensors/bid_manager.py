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
"""Sensor for detecting the completion of DV360 Bid Manager reports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.google.marketing_platform.hooks.bid_manager import GoogleBidManagerHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class GoogleBidManagerRunQuerySensor(BaseSensorOperator):
    """
    Sensor for detecting the completion of DV360 Bid Manager reports for API v2.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleBidManagerRunQuerySensor`

    :param query_id: Query ID for which report was generated
    :param report_id: Report ID for which you want to wait
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "query_id",
        "report_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        query_id: str,
        report_id: str,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query_id = query_id
        self.report_id = report_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        hook = GoogleBidManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        response = hook.get_report(query_id=self.query_id, report_id=self.report_id)
        status = response.get("metadata", {}).get("status", {}).get("state")
        self.log.info("STATUS OF THE REPORT %s FOR QUERY %s: %s", self.report_id, self.query_id, status)
        if response and status in ["DONE", "FAILED"]:
            return True
        return False
