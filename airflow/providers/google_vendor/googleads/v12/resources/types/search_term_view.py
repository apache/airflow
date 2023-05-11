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

from airflow.providers.google_vendor.googleads.v12.enums.types import search_term_targeting_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"SearchTermView",},
)


class SearchTermView(proto.Message):
    r"""A search term view with metrics aggregated by search term at
    the ad group level.

    Attributes:
        resource_name (str):
            Output only. The resource name of the search term view.
            Search term view resource names have the form:

            ``customers/{customer_id}/searchTermViews/{campaign_id}~{ad_group_id}~{URL-base64_search_term}``
        search_term (str):
            Output only. The search term.

            This field is a member of `oneof`_ ``_search_term``.
        ad_group (str):
            Output only. The ad group the search term
            served in.

            This field is a member of `oneof`_ ``_ad_group``.
        status (google.ads.googleads.v12.enums.types.SearchTermTargetingStatusEnum.SearchTermTargetingStatus):
            Output only. Indicates whether the search
            term is currently one of your targeted or
            excluded keywords.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    search_term = proto.Field(proto.STRING, number=5, optional=True,)
    ad_group = proto.Field(proto.STRING, number=6, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=search_term_targeting_status.SearchTermTargetingStatusEnum.SearchTermTargetingStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
