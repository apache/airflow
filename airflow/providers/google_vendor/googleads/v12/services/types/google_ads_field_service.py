# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import proto  # type: ignore

from airflow.providers.google_vendor.googleads.v12.resources.types import google_ads_field


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "GetGoogleAdsFieldRequest",
        "SearchGoogleAdsFieldsRequest",
        "SearchGoogleAdsFieldsResponse",
    },
)


class GetGoogleAdsFieldRequest(proto.Message):
    r"""Request message for
    [GoogleAdsFieldService.GetGoogleAdsField][google.ads.googleads.v12.services.GoogleAdsFieldService.GetGoogleAdsField].

    Attributes:
        resource_name (str):
            Required. The resource name of the field to
            get.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class SearchGoogleAdsFieldsRequest(proto.Message):
    r"""Request message for
    [GoogleAdsFieldService.SearchGoogleAdsFields][google.ads.googleads.v12.services.GoogleAdsFieldService.SearchGoogleAdsFields].

    Attributes:
        query (str):
            Required. The query string.
        page_token (str):
            Token of the page to retrieve. If not specified, the first
            page of results will be returned. Use the value obtained
            from ``next_page_token`` in the previous response in order
            to request the next page of results.
        page_size (int):
            Number of elements to retrieve in a single
            page. When too large a page is requested, the
            server may decide to further limit the number of
            returned resources.
    """

    query = proto.Field(proto.STRING, number=1,)
    page_token = proto.Field(proto.STRING, number=2,)
    page_size = proto.Field(proto.INT32, number=3,)


class SearchGoogleAdsFieldsResponse(proto.Message):
    r"""Response message for
    [GoogleAdsFieldService.SearchGoogleAdsFields][google.ads.googleads.v12.services.GoogleAdsFieldService.SearchGoogleAdsFields].

    Attributes:
        results (Sequence[google.ads.googleads.v12.resources.types.GoogleAdsField]):
            The list of fields that matched the query.
        next_page_token (str):
            Pagination token used to retrieve the next page of results.
            Pass the content of this string as the ``page_token``
            attribute of the next request. ``next_page_token`` is not
            returned for the last page.
        total_results_count (int):
            Total number of results that match the query
            ignoring the LIMIT clause.
    """

    @property
    def raw_page(self):
        return self

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message=google_ads_field.GoogleAdsField,
    )
    next_page_token = proto.Field(proto.STRING, number=2,)
    total_results_count = proto.Field(proto.INT64, number=3,)


__all__ = tuple(sorted(__protobuf__.manifest))
