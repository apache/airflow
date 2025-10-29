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
"""This module contains Google Analytics 4 (GA4) operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from google.analytics.admin_v1beta import (
    Account,
    DataStream,
    GoogleAdsLink,
    Property,
)
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.marketing_platform.hooks.analytics_admin import GoogleAnalyticsAdminHook
from airflow.providers.google.marketing_platform.links.analytics_admin import GoogleAnalyticsPropertyLink

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.protobuf.message import Message

    from airflow.providers.common.compat.sdk import Context


class GoogleAnalyticsAdminListAccountsOperator(GoogleCloudBaseOperator):
    """
    Lists all accounts to which the user has access.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsAdminListAccountsOperator`

    :param page_size: Optional, number of results to return in the list.
    :param page_token: Optional. The next_page_token value returned from a previous List request, if any.
    :param show_deleted: Optional. Whether to include soft-deleted (ie: "trashed") Accounts in the results.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
            will not be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
        "page_size",
        "page_token",
    )

    def __init__(
        self,
        *,
        page_size: int | None = None,
        page_token: str | None = None,
        show_deleted: bool | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.page_size = page_size
        self.page_token = page_token
        self.show_deleted = show_deleted
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(
        self,
        context: Context,
    ) -> Sequence[Message]:
        hook = GoogleAnalyticsAdminHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info(
            "Requesting list of Google Analytics accounts. Page size: %s, page token: %s",
            self.page_size,
            self.page_token,
        )
        accounts = hook.list_accounts(
            page_size=self.page_size,
            page_token=self.page_token,
            show_deleted=self.show_deleted,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        accounts_list: Sequence[Message] = [Account.to_dict(item) for item in accounts]
        n = len(accounts_list)
        self.log.info("Successful request. Retrieved %s item%s.", n, "s" if n > 1 else "")
        return accounts_list


class GoogleAnalyticsAdminCreatePropertyOperator(GoogleCloudBaseOperator):
    """
    Creates property.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsAdminCreatePropertyOperator`

    :param analytics_property: The property to create. Note: the supplied property must specify its parent.
        For more details see: https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties#Property
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
        "analytics_property",
    )
    operator_extra_links = (GoogleAnalyticsPropertyLink(),)

    def __init__(
        self,
        *,
        analytics_property: Property | dict[str, Any],
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.analytics_property = analytics_property
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(
        self,
        context: Context,
    ) -> Message:
        hook = GoogleAnalyticsAdminHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating a Google Analytics property.")
        prop = hook.create_property(
            analytics_property=self.analytics_property,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("The Google Analytics property %s was created successfully.", prop.name)
        GoogleAnalyticsPropertyLink.persist(
            context=context,
            property_id=prop.name.lstrip("properties/"),
        )

        return Property.to_dict(prop)


class GoogleAnalyticsAdminDeletePropertyOperator(GoogleCloudBaseOperator):
    """
    Soft-delete property.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsAdminDeletePropertyOperator`

    :param property_id: The id of the Property to soft-delete.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
        "property_id",
    )

    def __init__(
        self,
        *,
        property_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.property_id = property_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(
        self,
        context: Context,
    ) -> Message:
        hook = GoogleAnalyticsAdminHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting a Google Analytics property.")
        prop = hook.delete_property(
            property_id=self.property_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("The Google Analytics property %s was soft-deleted successfully.", prop.name)
        return Property.to_dict(prop)


class GoogleAnalyticsAdminCreateDataStreamOperator(GoogleCloudBaseOperator):
    """
    Creates Data stream.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsAdminCreateDataStreamOperator`

    :param property_id: ID of the parent property for the data stream.
    :param data_stream: The data stream to create.
        For more details see: https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties.dataStreams#DataStream
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
        "property_id",
        "data_stream",
    )

    def __init__(
        self,
        *,
        property_id: str,
        data_stream: DataStream | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.property_id = property_id
        self.data_stream = data_stream
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(
        self,
        context: Context,
    ) -> Message:
        hook = GoogleAnalyticsAdminHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating a Google Analytics data stream.")
        data_stream = hook.create_data_stream(
            property_id=self.property_id,
            data_stream=self.data_stream,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("The Google Analytics data stream %s was created successfully.", data_stream.name)
        return DataStream.to_dict(data_stream)


class GoogleAnalyticsAdminDeleteDataStreamOperator(GoogleCloudBaseOperator):
    """
    Deletes Data stream.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsAdminDeleteDataStreamOperator`

    :param property_id: ID of the property which is parent for the data stream.
    :param data_stream_id: ID of the data stream to delete.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
        "property_id",
        "data_stream_id",
    )

    def __init__(
        self,
        *,
        property_id: str,
        data_stream_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.property_id = property_id
        self.data_stream_id = data_stream_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(
        self,
        context: Context,
    ) -> None:
        hook = GoogleAnalyticsAdminHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting a Google Analytics data stream (id %s).", self.data_stream_id)
        hook.delete_data_stream(
            property_id=self.property_id,
            data_stream_id=self.data_stream_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("The Google Analytics data stream was deleted successfully.")
        return None


class GoogleAnalyticsAdminListGoogleAdsLinksOperator(GoogleCloudBaseOperator):
    """
    Lists all Google Ads links associated with a given property.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsAdminListGoogleAdsLinksOperator`

    :param property_id: ID of the parent property.
    :param page_size: Optional, number of results to return in the list.
    :param page_token: Optional. The next_page_token value returned from a previous List request, if any.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
        "property_id",
        "page_size",
        "page_token",
    )

    def __init__(
        self,
        *,
        property_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.property_id = property_id
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(
        self,
        context: Context,
    ) -> Sequence[Message]:
        hook = GoogleAnalyticsAdminHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info(
            "Requesting list of Google Ads links accounts for the property_id %s, "
            "page size %s, page token %s",
            self.property_id,
            self.page_size,
            self.page_token,
        )
        google_ads_links = hook.list_google_ads_links(
            property_id=self.property_id,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        ads_links_list: Sequence[Message] = [GoogleAdsLink.to_dict(item) for item in google_ads_links]
        n = len(ads_links_list)
        self.log.info("Successful request. Retrieved %s item%s.", n, "s" if n > 1 else "")
        return ads_links_list


class GoogleAnalyticsAdminGetGoogleAdsLinkOperator(GoogleCloudBaseOperator):
    """
    Gets a Google Ads link associated with a given property.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleAnalyticsAdminGetGoogleAdsLinkOperator`

    :param property_id: Parent property id.
    :param google_ads_link_id: Google Ads link id.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional. The timeout for this request.
    :param metadata: Optional. Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
        "google_ads_link_id",
        "property_id",
    )

    def __init__(
        self,
        *,
        property_id: str,
        google_ads_link_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.property_id = property_id
        self.google_ads_link_id = google_ads_link_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(
        self,
        context: Context,
    ) -> Message:
        hook = GoogleAnalyticsAdminHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info(
            "Requesting the Google Ads link with id %s for the property_id %s",
            self.google_ads_link_id,
            self.property_id,
        )
        ads_links = hook.list_google_ads_links(
            property_id=self.property_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        find_link = (item for item in ads_links if item.name.split("/")[-1] == self.google_ads_link_id)
        if ads_link := next(find_link, None):
            self.log.info("Successful request.")
            return GoogleAdsLink.to_dict(ads_link)
        raise AirflowNotFoundException(
            f"Google Ads Link with id {self.google_ads_link_id} and property id {self.property_id} not found"
        )
