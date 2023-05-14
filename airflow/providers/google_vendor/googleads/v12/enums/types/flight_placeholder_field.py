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
    manifest={"FlightPlaceholderFieldEnum",},
)


class FlightPlaceholderFieldEnum(proto.Message):
    r"""Values for Flight placeholder fields.
    For more information about dynamic remarketing feeds, see
    https://support.google.com/google-ads/answer/6053288.

    """

    class FlightPlaceholderField(proto.Enum):
        r"""Possible values for Flight placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DESTINATION_ID = 2
        ORIGIN_ID = 3
        FLIGHT_DESCRIPTION = 4
        ORIGIN_NAME = 5
        DESTINATION_NAME = 6
        FLIGHT_PRICE = 7
        FORMATTED_PRICE = 8
        FLIGHT_SALE_PRICE = 9
        FORMATTED_SALE_PRICE = 10
        IMAGE_URL = 11
        FINAL_URLS = 12
        FINAL_MOBILE_URLS = 13
        TRACKING_URL = 14
        ANDROID_APP_LINK = 15
        SIMILAR_DESTINATION_IDS = 16
        IOS_APP_LINK = 17
        IOS_APP_STORE_ID = 18


__all__ = tuple(sorted(__protobuf__.manifest))
