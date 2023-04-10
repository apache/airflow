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
    manifest={"AdvertisingChannelSubTypeEnum",},
)


class AdvertisingChannelSubTypeEnum(proto.Message):
    r"""An immutable specialization of an Advertising Channel.
    """

    class AdvertisingChannelSubType(proto.Enum):
        r"""Enum describing the different channel subtypes."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        SEARCH_MOBILE_APP = 2
        DISPLAY_MOBILE_APP = 3
        SEARCH_EXPRESS = 4
        DISPLAY_EXPRESS = 5
        SHOPPING_SMART_ADS = 6
        DISPLAY_GMAIL_AD = 7
        DISPLAY_SMART_CAMPAIGN = 8
        VIDEO_OUTSTREAM = 9
        VIDEO_ACTION = 10
        VIDEO_NON_SKIPPABLE = 11
        APP_CAMPAIGN = 12
        APP_CAMPAIGN_FOR_ENGAGEMENT = 13
        LOCAL_CAMPAIGN = 14
        SHOPPING_COMPARISON_LISTING_ADS = 15
        SMART_CAMPAIGN = 16
        VIDEO_SEQUENCE = 17
        APP_CAMPAIGN_FOR_PRE_REGISTRATION = 18


__all__ = tuple(sorted(__protobuf__.manifest))
