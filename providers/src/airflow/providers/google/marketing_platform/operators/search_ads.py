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
"""This module contains Google Search Ads operators."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.marketing_platform.hooks.search_ads import (
    GoogleSearchAdsReportingHook,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class _GoogleSearchAdsBaseOperator(BaseOperator):
    """
    Base class to use in NextGen operator.

    :param api_version: The version of the API that will be requested for example 'v0'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    """

    template_fields: Sequence[str] = (
        "api_version",
        "gcp_conn_id",
    )

    def __init__(
        self,
        *,
        api_version: str = "v0",
        gcp_conn_id: str = "google_search_ads_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id

    @cached_property
    def hook(self):
        return GoogleSearchAdsReportingHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
        )


class GoogleSearchAdsSearchOperator(_GoogleSearchAdsBaseOperator):
    """
    Search a report by query.

    .. seealso:
        For API documentation check:
        https://developers.google.com/search-ads/reporting/api/reference/rest/v0/customers.searchAds360/search

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsSearchOperator`

    :param customer_id: The ID of the customer being queried.
    :param query: The query to execute.
    :param page_token: Token of the page to retrieve. If not specified, the first page of results will be
        returned. Use the value obtained from `next_page_token` in the previous response
        in order to request the next page of results.
    :param page_size: Number of elements to retrieve in a single page. When too large a page is requested,
        the server may decide to further limit the number of returned resources.
        Default is 10000.
    :param return_total_results_count: If true, the total number of results that match the query ignoring
        the LIMIT clause will be included in the response. Default is false.
    :param summary_row_setting: Determines whether a summary row will be returned. By default,
        summary row is not returned. If requested, the summary row will be sent
        in a response by itself after all others query results are returned.
    :param validate_only: If true, the request is validated but not executed. Default is false.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param api_version: The version of the API that will be requested for example 'v0'.
    """

    template_fields: Sequence[str] = (
        *_GoogleSearchAdsBaseOperator.template_fields,
        "page_token",
        "page_size",
    )

    def __init__(
        self,
        *,
        customer_id: str,
        query: str,
        page_token: str | None = None,
        page_size: int = 10000,
        return_total_results_count: bool = False,
        summary_row_setting: str | None = None,
        validate_only: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.customer_id = customer_id
        self.query = query
        self.page_token = page_token
        self.page_size = page_size
        self.return_total_results_count = return_total_results_count
        self.summary_row_setting = summary_row_setting
        self.validate_only = validate_only

    def execute(self, context: Context):
        self.log.info("Querying Search Ads")
        response = self.hook.search(
            customer_id=self.customer_id,
            query=self.query,
            page_size=self.page_size,
            page_token=self.page_token,
            return_total_results_count=self.return_total_results_count,
            summary_row_setting=self.summary_row_setting,
            validate_only=self.validate_only,
        )
        self.log.info("Query result: %s", response)
        return response


class GoogleSearchAdsGetFieldOperator(_GoogleSearchAdsBaseOperator):
    """
    Retrieve metadata for a resource or a field.

    .. seealso:
        For API documentation check:
        https://developers.google.com/search-ads/reporting/api/reference/rest/v0/searchAds360Fields/get

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsGetFieldOperator`

    :param field_name: The name of the field.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param api_version: The version of the API that will be requested for example 'v0'.
    """

    def __init__(
        self,
        *,
        field_name: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.field_name = field_name

    def execute(self, context: Context) -> Any:
        self.log.info("Retrieving the metadata for the field '%s'", self.field_name)
        response = self.hook.get_field(field_name=self.field_name)
        self.log.info("Retrieved field: %s", response["resourceName"])
        return response


class GoogleSearchAdsSearchFieldsOperator(_GoogleSearchAdsBaseOperator):
    """
    Retrieve metadata for resource(s) or field(s) by the query syntax.

    .. seealso:
        For API documentation check:
        https://developers.google.com/search-ads/reporting/api/reference/rest/v0/searchAds360Fields/search

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsSearchFieldsOperator`

    :param query: The query string to execute.
    :param page_token: Token of the page to retrieve. If not specified, the first page of results will be
        returned. Use the value obtained from `next_page_token` in the previous response
        in order to request the next page of results.
    :param page_size: Number of elements to retrieve in a single page. When too large a page is requested,
        the server may decide to further limit the number of returned resources.
        Default 10000.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param api_version: The version of the API that will be requested for example 'v0'.
    """

    template_fields: Sequence[str] = (
        *_GoogleSearchAdsBaseOperator.template_fields,
        "page_token",
        "page_size",
    )

    def __init__(
        self,
        *,
        query: str,
        page_token: str | None = None,
        page_size: int = 10000,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.query = query

        self.page_token = page_token
        self.page_size = page_size

    def execute(self, context: Context) -> Any:
        self.log.info("Retrieving the metadata for %s", self.query)
        response = self.hook.search_fields(
            query=self.query,
            page_token=self.page_token,
            page_size=self.page_size,
        )
        self.log.info("Num of fields retrieved, #%d", len(response["results"]))
        return response


class GoogleSearchAdsGetCustomColumnOperator(_GoogleSearchAdsBaseOperator):
    """
    Retrieve details of a custom column for the given customer_id and campaign_id.

    .. seealso:
        For API documentation check:
        https://developers.google.com/search-ads/reporting/api/reference/rest/v0/customers.customColumns/get

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsGetCustomColumnOperator`

    :param customer_id: The customer ID for the custom column.
    :param custom_column_id: The ID for the custom column.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param api_version: The version of the API that will be requested for example 'v0'.
    """

    def __init__(
        self,
        *,
        customer_id: str,
        custom_column_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.customer_id = customer_id
        self.custom_column_id = custom_column_id

    def execute(self, context: Context):
        self.log.info(
            "Retrieving the custom column for the customer %s with the id of %s",
            self.customer_id,
            self.custom_column_id,
        )
        response = self.hook.get_custom_column(
            customer_id=self.customer_id,
            custom_column_id=self.custom_column_id,
        )
        self.log.info("Retrieved custom column: %s", response["id"])
        return response


class GoogleSearchAdsListCustomColumnsOperator(_GoogleSearchAdsBaseOperator):
    """
    List all custom columns.

    .. seealso:
        For API documentation check:
        https://developers.google.com/search-ads/reporting/api/reference/rest/v0/customers.customColumns/list

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsListCustomColumnsOperator`

    :param customer_id: The customer ID for the custom column.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param api_version: The version of the API that will be requested for example 'v0'.
    """

    def __init__(
        self,
        *,
        customer_id: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.customer_id = customer_id

    def execute(self, context: Context):
        self.log.info("Listing the custom columns for %s", self.customer_id)
        response = self.hook.list_custom_columns(customer_id=self.customer_id)
        self.log.info(
            "Num of retrieved custom column: %d", len(response.get("customColumns"))
        )
        return response
