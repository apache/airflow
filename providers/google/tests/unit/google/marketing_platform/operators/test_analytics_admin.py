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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.marketing_platform.operators.analytics_admin import (
    GoogleAnalyticsAdminCreateDataStreamOperator,
    GoogleAnalyticsAdminCreatePropertyOperator,
    GoogleAnalyticsAdminDeleteDataStreamOperator,
    GoogleAnalyticsAdminDeletePropertyOperator,
    GoogleAnalyticsAdminGetGoogleAdsLinkOperator,
    GoogleAnalyticsAdminListAccountsOperator,
    GoogleAnalyticsAdminListGoogleAdsLinksOperator,
)

GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TEST_GA_GOOGLE_ADS_PROPERTY_ID = "123456789"
TEST_GA_GOOGLE_ADS_LINK_ID = "987654321"
TEST_GA_GOOGLE_ADS_LINK_NAME = (
    f"properties/{TEST_GA_GOOGLE_ADS_PROPERTY_ID}/googleAdsLinks/{TEST_GA_GOOGLE_ADS_LINK_ID}"
)
TEST_PROPERTY_ID = "123456789"
TEST_PROPERTY_NAME = f"properties/{TEST_PROPERTY_ID}"
TEST_DATASTREAM_ID = "987654321"
TEST_DATASTREAM_NAME = f"properties/{TEST_PROPERTY_ID}/dataStreams/{TEST_DATASTREAM_ID}"
ANALYTICS_PATH = "airflow.providers.google.marketing_platform.operators.analytics_admin"


class TestGoogleAnalyticsAdminListAccountsOperator:
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    @mock.patch(f"{ANALYTICS_PATH}.Account.to_dict")
    def test_execute(self, account_to_dict_mock, hook_mock):
        list_accounts_returned = (mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        hook_mock.return_value.list_accounts.return_value = list_accounts_returned

        list_accounts_serialized = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        account_to_dict_mock.side_effect = list_accounts_serialized

        mock_page_size, mock_page_token, mock_show_deleted, mock_retry, mock_timeout, mock_metadata = (
            mock.MagicMock() for _ in range(6)
        )

        retrieved_accounts_list = GoogleAnalyticsAdminListAccountsOperator(
            task_id="test_task",
            page_size=mock_page_size,
            page_token=mock_page_token,
            show_deleted=mock_show_deleted,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.list_accounts.assert_called_once_with(
            page_size=mock_page_size,
            page_token=mock_page_token,
            show_deleted=mock_show_deleted,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        account_to_dict_mock.assert_has_calls([mock.call(item) for item in list_accounts_returned])
        assert retrieved_accounts_list == list_accounts_serialized


class TestGoogleAnalyticsAdminCreatePropertyOperator:
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsPropertyLink")
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    @mock.patch(f"{ANALYTICS_PATH}.Property.to_dict")
    def test_execute(self, property_to_dict_mock, hook_mock, _):
        property_returned = mock.MagicMock()
        hook_mock.return_value.create_property.return_value = property_returned

        property_serialized = mock.MagicMock()
        property_to_dict_mock.return_value = property_serialized

        mock_property, mock_retry, mock_timeout, mock_metadata = (mock.MagicMock() for _ in range(4))

        property_created = GoogleAnalyticsAdminCreatePropertyOperator(
            task_id="test_task",
            analytics_property=mock_property,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.create_property.assert_called_once_with(
            analytics_property=mock_property,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        property_to_dict_mock.assert_called_once_with(property_returned)
        assert property_created == property_serialized


class TestGoogleAnalyticsAdminDeletePropertyOperator:
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    @mock.patch(f"{ANALYTICS_PATH}.Property.to_dict")
    def test_execute(self, property_to_dict_mock, hook_mock):
        property_returned = mock.MagicMock()
        hook_mock.return_value.delete_property.return_value = property_returned

        property_serialized = mock.MagicMock()
        property_to_dict_mock.return_value = property_serialized

        mock_retry, mock_timeout, mock_metadata = (mock.MagicMock() for _ in range(3))

        property_deleted = GoogleAnalyticsAdminDeletePropertyOperator(
            task_id="test_task",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            property_id=TEST_PROPERTY_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.delete_property.assert_called_once_with(
            property_id=TEST_PROPERTY_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        property_to_dict_mock.assert_called_once_with(property_returned)
        assert property_deleted == property_serialized


class TestGoogleAnalyticsAdminCreateDataStreamOperator:
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    @mock.patch(f"{ANALYTICS_PATH}.DataStream.to_dict")
    def test_execute(self, data_stream_to_dict_mock, hook_mock):
        data_stream_returned = mock.MagicMock()
        hook_mock.return_value.create_data_stream.return_value = data_stream_returned

        data_stream_serialized = mock.MagicMock()
        data_stream_to_dict_mock.return_value = data_stream_serialized

        mock_parent, mock_data_stream, mock_retry, mock_timeout, mock_metadata = (
            mock.MagicMock() for _ in range(5)
        )

        data_stream_created = GoogleAnalyticsAdminCreateDataStreamOperator(
            task_id="test_task",
            property_id=TEST_PROPERTY_ID,
            data_stream=mock_data_stream,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.create_data_stream.assert_called_once_with(
            property_id=TEST_PROPERTY_ID,
            data_stream=mock_data_stream,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        data_stream_to_dict_mock.assert_called_once_with(data_stream_returned)
        assert data_stream_created == data_stream_serialized


class TestGoogleAnalyticsAdminDeleteDataStreamOperator:
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    def test_execute(self, hook_mock):
        mock_retry, mock_timeout, mock_metadata = (mock.MagicMock() for _ in range(3))

        GoogleAnalyticsAdminDeleteDataStreamOperator(
            task_id="test_task",
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            property_id=TEST_PROPERTY_ID,
            data_stream_id=TEST_DATASTREAM_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.delete_data_stream.assert_called_once_with(
            property_id=TEST_PROPERTY_ID,
            data_stream_id=TEST_DATASTREAM_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )


class TestGoogleAnalyticsAdminListGoogleAdsLinksOperator:
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAdsLink.to_dict")
    def test_execute(self, ads_link_to_dict_mock, hook_mock):
        list_ads_links_returned = (mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
        hook_mock.return_value.list_google_ads_links.return_value = list_ads_links_returned

        list_ads_links_serialized = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        ads_link_to_dict_mock.side_effect = list_ads_links_serialized

        mock_page_size, mock_page_token, mock_show_deleted, mock_retry, mock_timeout, mock_metadata = (
            mock.MagicMock() for _ in range(6)
        )

        retrieved_ads_links = GoogleAnalyticsAdminListGoogleAdsLinksOperator(
            task_id="test_task",
            property_id=TEST_PROPERTY_ID,
            page_size=mock_page_size,
            page_token=mock_page_token,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.list_google_ads_links.assert_called_once_with(
            property_id=TEST_PROPERTY_ID,
            page_size=mock_page_size,
            page_token=mock_page_token,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        ads_link_to_dict_mock.assert_has_calls([mock.call(item) for item in list_ads_links_returned])
        assert retrieved_ads_links == list_ads_links_serialized


class TestGoogleAnalyticsAdminGetGoogleAdsLinkOperator:
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    @mock.patch(f"{ANALYTICS_PATH}.GoogleAdsLink")
    def test_execute(self, mock_google_ads_link, hook_mock):
        mock_ad_link = mock.MagicMock()
        mock_ad_link.name = TEST_GA_GOOGLE_ADS_LINK_NAME
        list_ads_links = hook_mock.return_value.list_google_ads_links
        list_ads_links.return_value = [mock_ad_link]
        mock_retry, mock_timeout, mock_metadata = (mock.MagicMock() for _ in range(3))

        GoogleAnalyticsAdminGetGoogleAdsLinkOperator(
            task_id="test_task",
            property_id=TEST_GA_GOOGLE_ADS_PROPERTY_ID,
            google_ads_link_id=TEST_GA_GOOGLE_ADS_LINK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.list_google_ads_links.assert_called_once_with(
            property_id=TEST_PROPERTY_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        mock_google_ads_link.to_dict.assert_called_once_with(mock_ad_link)

    @mock.patch(f"{ANALYTICS_PATH}.GoogleAnalyticsAdminHook")
    def test_execute_not_found(self, hook_mock):
        list_ads_links = hook_mock.return_value.list_google_ads_links
        list_ads_links.return_value = []
        mock_retry, mock_timeout, mock_metadata = (mock.MagicMock() for _ in range(3))

        with pytest.raises(AirflowNotFoundException):
            GoogleAnalyticsAdminGetGoogleAdsLinkOperator(
                task_id="test_task",
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
                property_id=TEST_GA_GOOGLE_ADS_PROPERTY_ID,
                google_ads_link_id=TEST_GA_GOOGLE_ADS_LINK_ID,
                retry=mock_retry,
                timeout=mock_timeout,
                metadata=mock_metadata,
            ).execute(context=None)

        hook_mock.assert_called_once()
        hook_mock.return_value.list_google_ads_links.assert_called_once_with(
            property_id=TEST_PROPERTY_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
