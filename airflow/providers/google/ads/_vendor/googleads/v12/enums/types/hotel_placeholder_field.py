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
    package="google.ads.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"HotelPlaceholderFieldEnum",},
)


class HotelPlaceholderFieldEnum(proto.Message):
    r"""Values for Hotel placeholder fields.
    For more information about dynamic remarketing feeds, see
    https://support.google.com/google-ads/answer/6053288.

    """

    class HotelPlaceholderField(proto.Enum):
        r"""Possible values for Hotel placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        PROPERTY_ID = 2
        PROPERTY_NAME = 3
        DESTINATION_NAME = 4
        DESCRIPTION = 5
        ADDRESS = 6
        PRICE = 7
        FORMATTED_PRICE = 8
        SALE_PRICE = 9
        FORMATTED_SALE_PRICE = 10
        IMAGE_URL = 11
        CATEGORY = 12
        STAR_RATING = 13
        CONTEXTUAL_KEYWORDS = 14
        FINAL_URLS = 15
        FINAL_MOBILE_URLS = 16
        TRACKING_URL = 17
        ANDROID_APP_LINK = 18
        SIMILAR_PROPERTY_IDS = 19
        IOS_APP_LINK = 20
        IOS_APP_STORE_ID = 21


__all__ = tuple(sorted(__protobuf__.manifest))
