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

from airflow.providers.google.marketing_platform.operators.analytics import (
    GoogleAnalyticsGetAdsLinkOperator, GoogleAnalyticsListAccountsOperator,
    GoogleAnalyticsRetrieveAdsLinksListOperator,
)

API_VERSION = "api_version"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleAnalyticsListAccountsOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        op = GoogleAnalyticsListAccountsOperator(
            api_version=API_VERSION,
            gcp_connection_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.list_accounts.assert_called_once()


class TestGoogleAnalyticsRetrieveAdsLinksListOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        account_id = "the_knight_who_says_ni!"
        web_property_id = "42"

        op = GoogleAnalyticsRetrieveAdsLinksListOperator(
            account_id=account_id,
            web_property_id=web_property_id,
            api_version=API_VERSION,
            gcp_connection_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.list_ad_words_links.assert_called_once()
        hook_mock.assert_called_once_with(
            gcp_connection_id=GCP_CONN_ID, api_version=API_VERSION
        )
        hook_mock.return_value.list_ad_words_links.assert_called_once_with(
            account_id=account_id, web_property_id=web_property_id
        )


class TestGoogleAnalyticsGetAdsLinkOperator(unittest.TestCase):
    @mock.patch(
        "airflow.providers.google.marketing_platform.operators."
        "analytics.GoogleAnalyticsHook"
    )
    def test_execute(self, hook_mock):
        account_id = "the_knight_who_says_ni!"
        web_property_id = "42"
        web_property_ad_words_link_id = "holy_hand_grenade"

        op = GoogleAnalyticsGetAdsLinkOperator(
            account_id=account_id,
            web_property_id=web_property_id,
            web_property_ad_words_link_id=web_property_ad_words_link_id,
            api_version=API_VERSION,
            gcp_connection_id=GCP_CONN_ID,
            task_id="test_task",
        )
        op.execute(context=None)
        hook_mock.assert_called_once()
        hook_mock.return_value.get_ad_words_link.assert_called_once()
        hook_mock.assert_called_once_with(
            gcp_connection_id=GCP_CONN_ID, api_version=API_VERSION
        )
        hook_mock.return_value.get_ad_words_link.assert_called_once_with(
            account_id=account_id, web_property_id=web_property_id,
            web_property_ad_words_link_id=web_property_ad_words_link_id,
        )
