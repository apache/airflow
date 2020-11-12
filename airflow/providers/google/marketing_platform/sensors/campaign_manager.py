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
"""This module contains Google Campaign Manager sensor."""
from typing import Dict, Optional, Sequence, Union

from airflow.providers.google.marketing_platform.hooks.campaign_manager import GoogleCampaignManagerHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class GoogleCampaignManagerReportSensor(BaseSensorOperator):
    """
    Check if report is ready.

    .. seealso::
        Check official API docs:
        https://developers.google.com/doubleclick-advertisers/v3.3/reports/get

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCampaignManagerReportSensor`

    :param profile_id: The DFA user profile ID.
    :type profile_id: str
    :param report_id: The ID of the report.
    :type report_id: str
    :param file_id: The ID of the report file.
    :type file_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        "profile_id",
        "report_id",
        "file_id",
        "impersonation_chain",
    )

    def poke(self, context: Dict) -> bool:
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        response = hook.get_report(profile_id=self.profile_id, report_id=self.report_id, file_id=self.file_id)
        self.log.info("Report status: %s", response["status"])
        return response["status"] != "PROCESSING"

    @apply_defaults
    def __init__(
        self,
        *,
        profile_id: str,
        report_id: str,
        file_id: str,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        mode: str = "reschedule",
        poke_interval: int = 60 * 5,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mode = mode
        self.poke_interval = poke_interval
        self.profile_id = profile_id
        self.report_id = report_id
        self.file_id = file_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
