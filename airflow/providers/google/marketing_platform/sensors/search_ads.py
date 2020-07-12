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
"""
This module contains Google Search Ads sensor.
"""
from typing import Dict, Optional

from airflow.providers.google.marketing_platform.hooks.search_ads import GoogleSearchAdsHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class GoogleSearchAdsReportSensor(BaseSensorOperator):
    """
    Polls for the status of a report request.

    .. seealso::
        For API documentation check:
        https://developers.google.com/search-ads/v2/reference/reports/get

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsReportSensor`

    :param report_id: ID of the report request being polled.
    :type report_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("report_id",)

    @apply_defaults
    def __init__(
        self,
        report_id: str,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        mode: str = "reschedule",
        poke_interval: int = 5 * 60,
        **kwargs
    ):
        super().__init__(mode=mode, poke_interval=poke_interval, **kwargs)
        self.report_id = report_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def poke(self, context: Dict):
        hook = GoogleSearchAdsHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to, api_version=self.api_version
        )
        self.log.info('Checking status of %s report.', self.report_id)
        response = hook.get(report_id=self.report_id)
        return response['isReportReady']
