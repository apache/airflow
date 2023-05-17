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
    manifest={"CustomPlaceholderFieldEnum",},
)


class CustomPlaceholderFieldEnum(proto.Message):
    r"""Values for Custom placeholder fields.
    For more information about dynamic remarketing feeds, see
    https://support.google.com/google-ads/answer/6053288.

    """

    class CustomPlaceholderField(proto.Enum):
        r"""Possible values for Custom placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        ID = 2
        ID2 = 3
        ITEM_TITLE = 4
        ITEM_SUBTITLE = 5
        ITEM_DESCRIPTION = 6
        ITEM_ADDRESS = 7
        PRICE = 8
        FORMATTED_PRICE = 9
        SALE_PRICE = 10
        FORMATTED_SALE_PRICE = 11
        IMAGE_URL = 12
        ITEM_CATEGORY = 13
        FINAL_URLS = 14
        FINAL_MOBILE_URLS = 15
        TRACKING_URL = 16
        CONTEXTUAL_KEYWORDS = 17
        ANDROID_APP_LINK = 18
        SIMILAR_IDS = 19
        IOS_APP_LINK = 20
        IOS_APP_STORE_ID = 21


__all__ = tuple(sorted(__protobuf__.manifest))
