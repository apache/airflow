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

WEB_PROPERTY_AD_WORDS_LINK_ID = "AAIIRRFFLLOOWW"
WEB_PROPERTY_ID = "web_property_id"
ACCOUNT_ID = "the_knight_who_says_ni!"
DATA_SOURCE = "Monthy Python"
API_VERSION = "v3"
GCP_CONN_ID = "test_gcp_conn_id"
DELEGATE_TO = "TEST_DELEGATE_TO"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestGoogleAnalyticsHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleAnalyticsHook(API_VERSION, GCP_CONN_ID)

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__")
    def test_init(self, mock_base_init):
        hook = GoogleAnalyticsHook(
            API_VERSION,
            GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_base_init.assert_called_once_with(
            GCP_CONN_ID,
            delegate_to=DELEGATE_TO,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        self.assertEqual(hook.api_version, API_VERSION)

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
        self.hook.get_ad_words_link(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            web_property_ad_words_link_id=WEB_PROPERTY_AD_WORDS_LINK_ID,
        )

        get_conn_mock.return_value.management.return_value.webPropertyAdWordsLinks.\
            return_value.get.return_value.execute.assert_called_once_with(
                num_retries=num_retries
            )

        get_conn_mock.return_value.management.return_value.webPropertyAdWordsLinks.\
            return_value.get.assert_called_once_with(
                accountId=ACCOUNT_ID,
                webPropertyId=WEB_PROPERTY_ID,
                webPropertyAdWordsLinkId=WEB_PROPERTY_AD_WORDS_LINK_ID,
            )

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_ad_words_links(self, get_conn_mock):
        mock_ads_links = (
            get_conn_mock.return_value.management.return_value.webPropertyAdWordsLinks
        )
        mock_list = mock_ads_links.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.return_value = {"items": ["a", "b"], "totalResults": 2}
        list_ads_links = self.hook.list_ad_words_links(
            account_id=ACCOUNT_ID, web_property_id=WEB_PROPERTY_ID
        )
        self.assertEqual(list_ads_links, ["a", "b"])

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_ad_words_links_for_multiple_pages(self, get_conn_mock):
        mock_ads_links = (
            get_conn_mock.return_value.management.return_value.webPropertyAdWordsLinks
        )
        mock_list = mock_ads_links.return_value.list
        mock_execute = mock_list.return_value.execute
        mock_execute.side_effect = [
            {"items": ["a"], "totalResults": 2},
            {"items": ["b"], "totalResults": 2},
        ]
        list_ads_links = self.hook.list_ad_words_links(
            account_id=ACCOUNT_ID, web_property_id=WEB_PROPERTY_ID
        )
        self.assertEqual(list_ads_links, ["a", "b"])

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks." "analytics.MediaFileUpload"
    )
    def test_upload_data(self, media_mock, get_conn_mock):
        temp_name = "temp/file"
        self.hook.upload_data(
            file_location=temp_name,
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            custom_data_source_id=DATA_SOURCE,
            resumable_upload=True,
        )

        media_mock.assert_called_once_with(
            temp_name, mimetype="application/octet-stream", resumable=True
        )
        get_conn_mock.return_value.management.return_value.uploads.return_value.uploadData.\
            assert_called_once_with(
                accountId=ACCOUNT_ID,
                webPropertyId=WEB_PROPERTY_ID,
                customDataSourceId=DATA_SOURCE,
                media_body=media_mock.return_value,
            )

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_delete_upload_data(self, get_conn_mock):
        body = {"key": "temp/file"}
        self.hook.delete_upload_data(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            custom_data_source_id=DATA_SOURCE,
            delete_request_body=body,
        )

        get_conn_mock.return_value.management.return_value.uploads.return_value.deleteUploadData.\
            assert_called_once_with(
                accountId=ACCOUNT_ID,
                webPropertyId=WEB_PROPERTY_ID,
                customDataSourceId=DATA_SOURCE,
                body=body,
            )

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "analytics.GoogleAnalyticsHook.get_conn"
    )
    def test_list_upload(self, get_conn_mock):
        uploads = (
            get_conn_mock.return_value.management.return_value.uploads.return_value
        )
        uploads.list.return_value.execute.return_value = {
            "items": ["a", "b"],
            "totalResults": 2,
        }
        result = self.hook.list_uploads(
            account_id=ACCOUNT_ID,
            web_property_id=WEB_PROPERTY_ID,
            custom_data_source_id=DATA_SOURCE,
        )
        self.assertEqual(result, ["a", "b"])
