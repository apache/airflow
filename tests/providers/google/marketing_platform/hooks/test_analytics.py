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
import unittest
from unittest import mock

from airflow.providers.google.marketing_platform.hooks.analytics import GoogleAnalyticsHook
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v3"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleAnalyticsHook(unittest.TestCase):

    def setUp(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.base.CloudBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleAnalyticsHook(API_VERSION, GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook._authorize"
    )
    @mock.patch("airflow.providers.google.marketing_platform.hooks.analytics.build")
    def test_gen_conn(self, mock_build, mock_authorize):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "analytics",
            API_VERSION,
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        self.assertEqual(mock_build.return_value, result)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_accounts(self, get_conn_mock):
        mock_accounts = get_conn_mock.return_value.management.return_value.accounts
        mock_list = mock_accounts.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.return_value = {"items": ["a", "b"], "totalResults": 2}
        list_accounts = self.hook.list_accounts()
        self.assertEqual(list_accounts, ["a", "b"])

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_accounts_for_multiple_pages(self, get_conn_mock):
        mock_accounts = get_conn_mock.return_value.management.return_value.accounts
        mock_list = mock_accounts.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.side_effect = [
            {"items": ["a"], "totalResults": 2},
            {"items": ["b"], "totalResults": 2},
        ]
        list_accounts = self.hook.list_accounts()
        self.assertEqual(list_accounts, ["a", "b"])

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_get_ad_words_links_call(self, get_conn_mock):
        num_retries = 5
        account_id = "holy_hand_grenade"
        web_property_id = "UA-123456-1"
        web_property_ad_words_link_id = "AAIIRRFFLLOOWW"

        self.hook.get_ad_words_link(account_id=account_id,
                                    web_property_id=web_property_id,
                                    web_property_ad_words_link_id=web_property_ad_words_link_id, )

        get_conn_mock.return_value\
            .management.return_value\
            .webPropertyAdWordsLinks.return_value\
            .get.return_value\
            .execute.assert_called_once_with(num_retries=num_retries)

        get_conn_mock.return_value \
            .management.return_value \
            .webPropertyAdWordsLinks.return_value \
            .get.assert_called_once_with(accountId=account_id,
                                         webPropertyId=web_property_id,
                                         webPropertyAdWordsLinkId=web_property_ad_words_link_id,)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_ad_words_links(self, get_conn_mock):
        account_id = "the_knight_who_says_ni!"
        web_property_id = "web_property_id"
        mock_ads_links = get_conn_mock.return_value.management.return_value.webPropertyAdWordsLinks
        mock_list = mock_ads_links.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.return_value = {"items": ["a", "b"], "totalResults": 2}
        list_ads_links = self.hook.list_ad_words_links(account_id=account_id, web_property_id=web_property_id)
        self.assertEqual(list_ads_links, ["a", "b"])

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_ad_words_links_for_multiple_pages(self, get_conn_mock):
        account_id = "the_knight_who_says_ni!"
        web_property_id = "web_property_id"
        mock_ads_links = get_conn_mock.return_value.management.return_value.webPropertyAdWordsLinks
        mock_list = mock_ads_links.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.side_effect = [
            {"items": ["a"], "totalResults": 2},
            {"items": ["b"], "totalResults": 2},
        ]
        list_ads_links = self.hook.list_ad_words_links(account_id=account_id, web_property_id=web_property_id)
        self.assertEqual(list_ads_links, ["a", "b"])
