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

from airflow.providers.google_vendor.googleads.v12.common.types import click_location
from airflow.providers.google_vendor.googleads.v12.common.types import criteria


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"ClickView",},
)


class ClickView(proto.Message):
    r"""A click view with metrics aggregated at each click level,
    including both valid and invalid clicks. For non-Search
    campaigns, metrics.clicks represents the number of valid and
    invalid interactions. Queries including ClickView must have a
    filter limiting the results to one day and can be requested for
    dates back to 90 days before the time of the request.

    Attributes:
        resource_name (str):
            Output only. The resource name of the click view. Click view
            resource names have the form:

            ``customers/{customer_id}/clickViews/{date (yyyy-MM-dd)}~{gclid}``
        gclid (str):
            Output only. The Google Click ID.

            This field is a member of `oneof`_ ``_gclid``.
        area_of_interest (google.ads.googleads.v12.common.types.ClickLocation):
            Output only. The location criteria matching
            the area of interest associated with the
            impression.
        location_of_presence (google.ads.googleads.v12.common.types.ClickLocation):
            Output only. The location criteria matching
            the location of presence associated with the
            impression.
        page_number (int):
            Output only. Page number in search results
            where the ad was shown.

            This field is a member of `oneof`_ ``_page_number``.
        ad_group_ad (str):
            Output only. The associated ad.

            This field is a member of `oneof`_ ``_ad_group_ad``.
        campaign_location_target (str):
            Output only. The associated campaign location
            target, if one exists.

            This field is a member of `oneof`_ ``_campaign_location_target``.
        user_list (str):
            Output only. The associated user list, if one
            exists.

            This field is a member of `oneof`_ ``_user_list``.
        keyword (str):
            Output only. The associated keyword, if one
            exists and the click corresponds to the SEARCH
            channel.
        keyword_info (google.ads.googleads.v12.common.types.KeywordInfo):
            Output only. Basic information about the
            associated keyword, if it exists.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    gclid = proto.Field(proto.STRING, number=8, optional=True,)
    area_of_interest = proto.Field(
        proto.MESSAGE, number=3, message=click_location.ClickLocation,
    )
    location_of_presence = proto.Field(
        proto.MESSAGE, number=4, message=click_location.ClickLocation,
    )
    page_number = proto.Field(proto.INT64, number=9, optional=True,)
    ad_group_ad = proto.Field(proto.STRING, number=10, optional=True,)
    campaign_location_target = proto.Field(
        proto.STRING, number=11, optional=True,
    )
    user_list = proto.Field(proto.STRING, number=12, optional=True,)
    keyword = proto.Field(proto.STRING, number=13,)
    keyword_info = proto.Field(
        proto.MESSAGE, number=14, message=criteria.KeywordInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
