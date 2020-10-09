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
from unittest import TestCase, mock

from airflow.providers.google.marketing_platform.sensors.search_ads import GoogleSearchAdsReportSensor

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"


class TestSearchAdsReportSensor(TestCase):
    @mock.patch("airflow.providers.google.marketing_platform.sensors." "search_ads.GoogleSearchAdsHook")
    @mock.patch("airflow.providers.google.marketing_platform.sensors." "search_ads.BaseSensorOperator")
    def test_poke(self, mock_base_op, hook_mock):
        report_id = "REPORT_ID"
        op = GoogleSearchAdsReportSensor(report_id=report_id, api_version=API_VERSION, task_id="test_task")
        op.poke(context=None)
        hook_mock.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            delegate_to=None,
            api_version=API_VERSION,
            impersonation_chain=None,
        )
        hook_mock.return_value.get.assert_called_once_with(report_id=report_id)
