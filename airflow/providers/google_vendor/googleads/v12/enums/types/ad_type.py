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
    manifest={"AdTypeEnum",},
)


class AdTypeEnum(proto.Message):
    r"""Container for enum describing possible types of an ad.
    """

    class AdType(proto.Enum):
        r"""The possible types of an ad."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        TEXT_AD = 2
        EXPANDED_TEXT_AD = 3
        EXPANDED_DYNAMIC_SEARCH_AD = 7
        HOTEL_AD = 8
        SHOPPING_SMART_AD = 9
        SHOPPING_PRODUCT_AD = 10
        VIDEO_AD = 12
        IMAGE_AD = 14
        RESPONSIVE_SEARCH_AD = 15
        LEGACY_RESPONSIVE_DISPLAY_AD = 16
        APP_AD = 17
        LEGACY_APP_INSTALL_AD = 18
        RESPONSIVE_DISPLAY_AD = 19
        LOCAL_AD = 20
        HTML5_UPLOAD_AD = 21
        DYNAMIC_HTML5_AD = 22
        APP_ENGAGEMENT_AD = 23
        SHOPPING_COMPARISON_LISTING_AD = 24
        VIDEO_BUMPER_AD = 25
        VIDEO_NON_SKIPPABLE_IN_STREAM_AD = 26
        VIDEO_OUTSTREAM_AD = 27
        VIDEO_TRUEVIEW_IN_STREAM_AD = 29
        VIDEO_RESPONSIVE_AD = 30
        SMART_CAMPAIGN_AD = 31
        CALL_AD = 32
        APP_PRE_REGISTRATION_AD = 33
        IN_FEED_VIDEO_AD = 34
        DISCOVERY_MULTI_ASSET_AD = 35
        DISCOVERY_CAROUSEL_AD = 36


__all__ = tuple(sorted(__protobuf__.manifest))
