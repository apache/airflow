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
    manifest={"RealEstatePlaceholderFieldEnum",},
)


class RealEstatePlaceholderFieldEnum(proto.Message):
    r"""Values for Real Estate placeholder fields.
    For more information about dynamic remarketing feeds, see
    https://support.google.com/google-ads/answer/6053288.

    """

    class RealEstatePlaceholderField(proto.Enum):
        r"""Possible values for Real Estate placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        LISTING_ID = 2
        LISTING_NAME = 3
        CITY_NAME = 4
        DESCRIPTION = 5
        ADDRESS = 6
        PRICE = 7
        FORMATTED_PRICE = 8
        IMAGE_URL = 9
        PROPERTY_TYPE = 10
        LISTING_TYPE = 11
        CONTEXTUAL_KEYWORDS = 12
        FINAL_URLS = 13
        FINAL_MOBILE_URLS = 14
        TRACKING_URL = 15
        ANDROID_APP_LINK = 16
        SIMILAR_LISTING_IDS = 17
        IOS_APP_LINK = 18
        IOS_APP_STORE_ID = 19


__all__ = tuple(sorted(__protobuf__.manifest))
