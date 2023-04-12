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
    package="airflow.providers.google_vendor.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupTypeEnum",},
)


class AdGroupTypeEnum(proto.Message):
    r"""Defines types of an ad group, specific to a particular
    campaign channel type. This type drives validations that
    restrict which entities can be added to the ad group.

    """

    class AdGroupType(proto.Enum):
        r"""Enum listing the possible types of an ad group."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        SEARCH_STANDARD = 2
        DISPLAY_STANDARD = 3
        SHOPPING_PRODUCT_ADS = 4
        HOTEL_ADS = 6
        SHOPPING_SMART_ADS = 7
        VIDEO_BUMPER = 8
        VIDEO_TRUE_VIEW_IN_STREAM = 9
        VIDEO_TRUE_VIEW_IN_DISPLAY = 10
        VIDEO_NON_SKIPPABLE_IN_STREAM = 11
        VIDEO_OUTSTREAM = 12
        SEARCH_DYNAMIC_ADS = 13
        SHOPPING_COMPARISON_LISTING_ADS = 14
        PROMOTED_HOTEL_ADS = 15
        VIDEO_RESPONSIVE = 16
        VIDEO_EFFICIENT_REACH = 17
        SMART_CAMPAIGN_ADS = 18


__all__ = tuple(sorted(__protobuf__.manifest))
