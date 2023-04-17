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
    manifest={"LocalPlaceholderFieldEnum",},
)


class LocalPlaceholderFieldEnum(proto.Message):
    r"""Values for Local placeholder fields.
    For more information about dynamic remarketing feeds, see
    https://support.google.com/google-ads/answer/6053288.

    """

    class LocalPlaceholderField(proto.Enum):
        r"""Possible values for Local placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DEAL_ID = 2
        DEAL_NAME = 3
        SUBTITLE = 4
        DESCRIPTION = 5
        PRICE = 6
        FORMATTED_PRICE = 7
        SALE_PRICE = 8
        FORMATTED_SALE_PRICE = 9
        IMAGE_URL = 10
        ADDRESS = 11
        CATEGORY = 12
        CONTEXTUAL_KEYWORDS = 13
        FINAL_URLS = 14
        FINAL_MOBILE_URLS = 15
        TRACKING_URL = 16
        ANDROID_APP_LINK = 17
        SIMILAR_DEAL_IDS = 18
        IOS_APP_LINK = 19
        IOS_APP_STORE_ID = 20


__all__ = tuple(sorted(__protobuf__.manifest))
