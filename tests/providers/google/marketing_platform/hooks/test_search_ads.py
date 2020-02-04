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

from airflow.providers.google.marketing_platform.hooks.search_ads import GoogleSearchAdsHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v2"
GCP_CONN_ID = "google_cloud_default"


class TestSearchAdsHook(TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleSearchAdsHook(gcp_conn_id=GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "search_ads.GoogleSearchAdsHook._authorize"
    )
    @mock.patch("airflow.providers.google.marketing_platform.hooks.search_ads.build")
    def test_gen_conn(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "doubleclicksearch",
            API_VERSION,
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        self.assertEqual(mock_build.return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "search_ads.GoogleSearchAdsHook.get_conn"
    )
    def test_insert(self, get_conn_mock):
        report = {"report": "test"}

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.request.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.insert_report(report=report)

        get_conn_mock.return_value.reports.return_value.request.assert_called_once_with(
            body=report
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "search_ads.GoogleSearchAdsHook.get_conn"
    )
    def test_get(self, get_conn_mock):
        report_id = "REPORT_ID"

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.get.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.get(report_id=report_id)

        get_conn_mock.return_value.reports.return_value.get.assert_called_once_with(
            reportId=report_id
        )

        self.assertEqual(return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "search_ads.GoogleSearchAdsHook.get_conn"
    )
    def test_get_file(self, get_conn_mock):
        report_fragment = 42
        report_id = "REPORT_ID"

        return_value = "TEST"
        get_conn_mock.return_value.reports.return_value.getFile.return_value.execute.return_value = (
            return_value
        )

        result = self.hook.get_file(
            report_fragment=report_fragment, report_id=report_id
        )

        get_conn_mock.return_value.reports.return_value.getFile.assert_called_once_with(
            reportFragment=report_fragment, reportId=report_id
        )

        self.assertEqual(return_value, result)
