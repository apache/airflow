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

from unittest import mock

import pytest

from airflow.providers.facebook.ads.hooks.ads import FacebookAdsReportingHook

API_VERSION = "api_version"
EXTRAS = {"account_id": "act_12345", "app_id": "12345", "app_secret": "1fg444", "access_token": "Ab35gf7E"}
FIELDS = [
    "campaign_name",
    "campaign_id",
    "ad_id",
    "clicks",
    "impressions",
]
PARAMS = {"level": "ad", "date_preset": "yesterday"}


@pytest.fixture()
def mock_hook():
    with mock.patch("airflow.hooks.base_hook.BaseHook.get_connection") as conn:
        hook = FacebookAdsReportingHook(api_version=API_VERSION)
        conn.return_value.extra_dejson = EXTRAS
        yield hook


class TestFacebookAdsReportingHook:
    @mock.patch("airflow.providers.facebook.ads.hooks.ads.FacebookAdsApi")
    def test_get_service(self, mock_api, mock_hook):
        mock_hook._get_service()
        api = mock_api.init
        api.assert_called_once_with(
            app_id=EXTRAS["app_id"],
            app_secret=EXTRAS["app_secret"],
            access_token=EXTRAS["access_token"],
            account_id=EXTRAS["account_id"],
            api_version=API_VERSION,
        )

    @mock.patch("airflow.providers.facebook.ads.hooks.ads.AdAccount")
    @mock.patch("airflow.providers.facebook.ads.hooks.ads.FacebookAdsApi")
    def test_bulk_facebook_report(self, mock_client, mock_ad_account, mock_hook):
        mock_client = mock_client.init()
        ad_account = mock_ad_account().get_insights
        ad_account.return_value.api_get.return_value = {
            "async_status": "Job Completed",
            "report_run_id": "12345",
            "async_percent_completion": 100,
        }
        mock_hook.bulk_facebook_report(params=PARAMS, fields=FIELDS)
        mock_ad_account.assert_has_calls([mock.call(mock_client.get_default_account_id(), api=mock_client)])
        ad_account.assert_called_once_with(params=PARAMS, fields=FIELDS, is_async=True)
        ad_account.return_value.api_get.assert_has_calls([mock.call(), mock.call()])
