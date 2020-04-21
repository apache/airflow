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
Sensor for detecting the completion of DV360 reports.
"""
from typing import Dict, Optional

from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.sensors.base_sensor_operator import BaseSensorOperator


class GoogleDisplayVideo360ReportSensor(BaseSensorOperator):
    """
    Sensor for detecting the completion of DV360 reports.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360ReportSensor`

    :param report_id: Report ID to delete.
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

    def __init__(
        self,
        report_id: str,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.report_id = report_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def poke(self, context: Dict) -> bool:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )

        response = hook.get_query(query_id=self.report_id)
        if response and not response.get("metadata", {}).get("running"):
            return True
        return False
