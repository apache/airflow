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
Hooks for Google Analytics (GA4) Admin service.

.. spelling:word-list::

    DataStream
    ListAccountsPager
    ListGoogleAdsLinksPager
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.analytics.admin_v1beta import (
    AnalyticsAdminServiceClient,
    DataStream,
    Property,
)
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.analytics.admin_v1beta.services.analytics_admin_service.pagers import (
        ListAccountsPager,
        ListGoogleAdsLinksPager,
    )
    from google.api_core.retry import Retry


class GoogleAnalyticsAdminHook(GoogleBaseHook):
    """Hook for Google Analytics 4 (GA4) Admin API."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._conn: AnalyticsAdminServiceClient | None = None

    def get_conn(self) -> AnalyticsAdminServiceClient:
        if not self._conn:
            self._conn = AnalyticsAdminServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._conn

    def list_accounts(
        self,
        page_size: int | None = None,
        page_token: str | None = None,
        show_deleted: bool | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListAccountsPager:
        """
        Get list of accounts in Google Analytics.

        .. seealso::
            For more details please check the client library documentation:
            https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/accounts/list

        :param page_size: Optional, number of results to return in the list.
        :param page_token: Optional. The next_page_token value returned from a previous List request, if any.
        :param show_deleted: Optional. Whether to include soft-deleted (ie: "trashed") Accounts in the results.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.

        :returns: List of Google Analytics accounts.
        """
        request = {
            "page_size": page_size,
            "page_token": page_token,
            "show_deleted": show_deleted,
        }
        client = self.get_conn()
        return client.list_accounts(
            request=request, retry=retry, timeout=timeout, metadata=metadata
        )

    def create_property(
        self,
        analytics_property: Property | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Property:
        """
        Create Google Analytics property.

        .. seealso::
            For more details please check the client library documentation:
            https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties/create

        :param analytics_property: The property to create. Note: the supplied property must specify its
            parent.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.

        :returns: Created Google Analytics property.
        """
        client = self.get_conn()
        return client.create_property(
            request={"property": analytics_property},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def delete_property(
        self,
        property_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Property:
        """
        Soft delete Google Analytics property.

        .. seealso::
            For more details please check the client library documentation:
            https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties/delete

        :param property_id: ID of the Property to soft-delete. Format: properties/{property_id}.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.

        :returns: Resource message representing Google Analytics property.
        """
        client = self.get_conn()
        request = {"name": f"properties/{property_id}"}
        return client.delete_property(
            request=request, retry=retry, timeout=timeout, metadata=metadata
        )

    def create_data_stream(
        self,
        property_id: str,
        data_stream: DataStream | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> DataStream:
        """
        Create Google Analytics data stream.

        .. seealso::
            For more details please check the client library documentation:
            https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties.dataStreams/create

        :param property_id: ID of the parent property for the data stream.
        :param data_stream: The data stream to create.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.

        :returns: Created Google Analytics data stream.
        """
        client = self.get_conn()
        return client.create_data_stream(
            request={"parent": f"properties/{property_id}", "data_stream": data_stream},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def delete_data_stream(
        self,
        property_id: str,
        data_stream_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete Google Analytics data stream.

        .. seealso::
            For more details please check the client library documentation:
            https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties.dataStreams/delete

        :param property_id: ID of the parent property for the data stream.
        :param data_stream_id: The data stream id to delete.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.
        """
        client = self.get_conn()
        return client.delete_data_stream(
            request={"name": f"properties/{property_id}/dataStreams/{data_stream_id}"},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    def list_google_ads_links(
        self,
        property_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListGoogleAdsLinksPager:
        """
        Get list of Google Ads links.

        .. seealso::
            For more details please check the client library documentation:
            https://googleapis.dev/python/analyticsadmin/latest/admin_v1beta/analytics_admin_service.html#google.analytics.admin_v1beta.services.analytics_admin_service.AnalyticsAdminServiceAsyncClient.list_google_ads_links

        :param property_id: ID of the parent property.
        :param page_size: Optional, number of results to return in the list.
        :param page_token: Optional. The next_page_token value returned from a previous List request, if any.
        :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
        :param timeout: Optional. The timeout for this request.
        :param metadata: Optional. Strings which should be sent along with the request as metadata.

        :returns: List of Google Analytics accounts.
        """
        client = self.get_conn()
        request = {
            "parent": f"properties/{property_id}",
            "page_size": page_size,
            "page_token": page_token,
        }
        return client.list_google_ads_links(
            request=request, retry=retry, timeout=timeout, metadata=metadata
        )
