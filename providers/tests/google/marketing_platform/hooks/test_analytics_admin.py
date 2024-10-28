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
from __future__ import annotations

from unittest import mock

from airflow.providers.google.marketing_platform.hooks.analytics_admin import (
    GoogleAnalyticsAdminHook,
)

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

GCP_CONN_ID = "test_gcp_conn_id"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TEST_PROPERTY_ID = "123456789"
TEST_PROPERTY_NAME = f"properties/{TEST_PROPERTY_ID}"
TEST_DATASTREAM_ID = "987654321"
TEST_DATASTREAM_NAME = f"properties/{TEST_PROPERTY_ID}/dataStreams/{TEST_DATASTREAM_ID}"
ANALYTICS_HOOK_PATH = "airflow.providers.google.marketing_platform.hooks.analytics_admin"


class TestGoogleAnalyticsAdminHook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleAnalyticsAdminHook(GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__"
    )
    def test_init(self, mock_base_init):
        GoogleAnalyticsAdminHook(
            GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_base_init.assert_called_once_with(
            GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

    @mock.patch(f"{ANALYTICS_HOOK_PATH}.CLIENT_INFO")
    @mock.patch(f"{ANALYTICS_HOOK_PATH}.GoogleAnalyticsAdminHook.get_credentials")
    @mock.patch(f"{ANALYTICS_HOOK_PATH}.AnalyticsAdminServiceClient")
    def test_get_conn(self, mock_client, get_credentials, mock_client_info):
        mock_credentials = mock.MagicMock()
        get_credentials.return_value = mock_credentials

        result = self.hook.get_conn()

        mock_client.assert_called_once_with(
            credentials=mock_credentials, client_info=mock_client_info
        )
        assert self.hook._conn == result

    @mock.patch(f"{ANALYTICS_HOOK_PATH}.GoogleAnalyticsAdminHook.get_conn")
    def test_list_accounts(self, mock_get_conn):
        list_accounts_expected = mock.MagicMock()
        mock_list_accounts = mock_get_conn.return_value.list_accounts
        mock_list_accounts.return_value = list_accounts_expected
        (
            mock_page_size,
            mock_page_token,
            mock_show_deleted,
            mock_retry,
            mock_timeout,
            mock_metadata,
        ) = (mock.MagicMock() for _ in range(6))

        request = {
            "page_size": mock_page_size,
            "page_token": mock_page_token,
            "show_deleted": mock_show_deleted,
        }

        list_accounts_received = self.hook.list_accounts(
            page_size=mock_page_size,
            page_token=mock_page_token,
            show_deleted=mock_show_deleted,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        mock_list_accounts.assert_called_once_with(
            request=request,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        assert list_accounts_received == list_accounts_expected

    @mock.patch(f"{ANALYTICS_HOOK_PATH}.GoogleAnalyticsAdminHook.get_conn")
    def test_create_property(self, mock_get_conn):
        property_expected = mock.MagicMock()

        mock_create_property = mock_get_conn.return_value.create_property
        mock_create_property.return_value = property_expected
        mock_property, mock_retry, mock_timeout, mock_metadata = (
            mock.MagicMock() for _ in range(4)
        )

        property_created = self.hook.create_property(
            analytics_property=mock_property,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        request = {"property": mock_property}
        mock_create_property.assert_called_once_with(
            request=request,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        assert property_created == property_expected

    @mock.patch(f"{ANALYTICS_HOOK_PATH}.GoogleAnalyticsAdminHook.get_conn")
    def test_delete_property(self, mock_get_conn):
        property_expected = mock.MagicMock()
        mock_delete_property = mock_get_conn.return_value.delete_property
        mock_delete_property.return_value = property_expected
        mock_retry, mock_timeout, mock_metadata = (mock.MagicMock() for _ in range(3))

        property_deleted = self.hook.delete_property(
            property_id=TEST_PROPERTY_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        request = {"name": TEST_PROPERTY_NAME}
        mock_delete_property.assert_called_once_with(
            request=request,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        assert property_deleted == property_expected

    @mock.patch(f"{ANALYTICS_HOOK_PATH}.GoogleAnalyticsAdminHook.get_conn")
    def test_create_data_stream(self, mock_get_conn):
        data_stream_expected = mock.MagicMock()
        mock_create_data_stream = mock_get_conn.return_value.create_data_stream
        mock_create_data_stream.return_value = data_stream_expected
        mock_data_stream, mock_retry, mock_timeout, mock_metadata = (
            mock.MagicMock() for _ in range(4)
        )

        data_stream_created = self.hook.create_data_stream(
            property_id=TEST_PROPERTY_ID,
            data_stream=mock_data_stream,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        request = {"parent": TEST_PROPERTY_NAME, "data_stream": mock_data_stream}
        mock_create_data_stream.assert_called_once_with(
            request=request,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
        assert data_stream_created == data_stream_expected

    @mock.patch(f"{ANALYTICS_HOOK_PATH}.GoogleAnalyticsAdminHook.get_conn")
    def test_delete_data_stream(self, mock_get_conn):
        mock_retry, mock_timeout, mock_metadata = (mock.MagicMock() for _ in range(3))

        self.hook.delete_data_stream(
            property_id=TEST_PROPERTY_ID,
            data_stream_id=TEST_DATASTREAM_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        request = {"name": TEST_DATASTREAM_NAME}
        mock_get_conn.return_value.delete_data_stream.assert_called_once_with(
            request=request,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

    @mock.patch(f"{ANALYTICS_HOOK_PATH}.GoogleAnalyticsAdminHook.get_conn")
    def test_list_ads_links(self, mock_get_conn):
        mock_page_size, mock_page_token, mock_retry, mock_timeout, mock_metadata = (
            mock.MagicMock() for _ in range(5)
        )

        self.hook.list_google_ads_links(
            property_id=TEST_PROPERTY_ID,
            page_size=mock_page_size,
            page_token=mock_page_token,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        request = {
            "parent": TEST_PROPERTY_NAME,
            "page_size": mock_page_size,
            "page_token": mock_page_token,
        }
        mock_get_conn.return_value.list_google_ads_links.assert_called_once_with(
            request=request,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
