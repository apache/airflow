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


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"DynamicSearchAdsSearchTermView",},
)


class DynamicSearchAdsSearchTermView(proto.Message):
    r"""A dynamic search ads search term view.

    Attributes:
        resource_name (str):
            Output only. The resource name of the dynamic search ads
            search term view. Dynamic search ads search term view
            resource names have the form:

            ``customers/{customer_id}/dynamicSearchAdsSearchTermViews/{ad_group_id}~{search_term_fingerprint}~{headline_fingerprint}~{landing_page_fingerprint}~{page_url_fingerprint}``
        search_term (str):
            Output only. Search term
            This field is read-only.

            This field is a member of `oneof`_ ``_search_term``.
        headline (str):
            Output only. The dynamically generated
            headline of the Dynamic Search Ad.
            This field is read-only.

            This field is a member of `oneof`_ ``_headline``.
        landing_page (str):
            Output only. The dynamically selected landing
            page URL of the impression.
            This field is read-only.

            This field is a member of `oneof`_ ``_landing_page``.
        page_url (str):
            Output only. The URL of page feed item served
            for the impression.
            This field is read-only.

            This field is a member of `oneof`_ ``_page_url``.
        has_negative_keyword (bool):
            Output only. True if query matches a negative
            keyword.
            This field is read-only.

            This field is a member of `oneof`_ ``_has_negative_keyword``.
        has_matching_keyword (bool):
            Output only. True if query is added to
            targeted keywords.
            This field is read-only.

            This field is a member of `oneof`_ ``_has_matching_keyword``.
        has_negative_url (bool):
            Output only. True if query matches a negative
            url.
            This field is read-only.

            This field is a member of `oneof`_ ``_has_negative_url``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    search_term = proto.Field(proto.STRING, number=9, optional=True,)
    headline = proto.Field(proto.STRING, number=10, optional=True,)
    landing_page = proto.Field(proto.STRING, number=11, optional=True,)
    page_url = proto.Field(proto.STRING, number=12, optional=True,)
    has_negative_keyword = proto.Field(proto.BOOL, number=13, optional=True,)
    has_matching_keyword = proto.Field(proto.BOOL, number=14, optional=True,)
    has_negative_url = proto.Field(proto.BOOL, number=15, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
