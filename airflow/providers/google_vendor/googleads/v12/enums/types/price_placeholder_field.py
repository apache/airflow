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
    manifest={"PricePlaceholderFieldEnum",},
)


class PricePlaceholderFieldEnum(proto.Message):
    r"""Values for Price placeholder fields.
    """

    class PricePlaceholderField(proto.Enum):
        r"""Possible values for Price placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        TYPE = 2
        PRICE_QUALIFIER = 3
        TRACKING_TEMPLATE = 4
        LANGUAGE = 5
        FINAL_URL_SUFFIX = 6
        ITEM_1_HEADER = 100
        ITEM_1_DESCRIPTION = 101
        ITEM_1_PRICE = 102
        ITEM_1_UNIT = 103
        ITEM_1_FINAL_URLS = 104
        ITEM_1_FINAL_MOBILE_URLS = 105
        ITEM_2_HEADER = 200
        ITEM_2_DESCRIPTION = 201
        ITEM_2_PRICE = 202
        ITEM_2_UNIT = 203
        ITEM_2_FINAL_URLS = 204
        ITEM_2_FINAL_MOBILE_URLS = 205
        ITEM_3_HEADER = 300
        ITEM_3_DESCRIPTION = 301
        ITEM_3_PRICE = 302
        ITEM_3_UNIT = 303
        ITEM_3_FINAL_URLS = 304
        ITEM_3_FINAL_MOBILE_URLS = 305
        ITEM_4_HEADER = 400
        ITEM_4_DESCRIPTION = 401
        ITEM_4_PRICE = 402
        ITEM_4_UNIT = 403
        ITEM_4_FINAL_URLS = 404
        ITEM_4_FINAL_MOBILE_URLS = 405
        ITEM_5_HEADER = 500
        ITEM_5_DESCRIPTION = 501
        ITEM_5_PRICE = 502
        ITEM_5_UNIT = 503
        ITEM_5_FINAL_URLS = 504
        ITEM_5_FINAL_MOBILE_URLS = 505
        ITEM_6_HEADER = 600
        ITEM_6_DESCRIPTION = 601
        ITEM_6_PRICE = 602
        ITEM_6_UNIT = 603
        ITEM_6_FINAL_URLS = 604
        ITEM_6_FINAL_MOBILE_URLS = 605
        ITEM_7_HEADER = 700
        ITEM_7_DESCRIPTION = 701
        ITEM_7_PRICE = 702
        ITEM_7_UNIT = 703
        ITEM_7_FINAL_URLS = 704
        ITEM_7_FINAL_MOBILE_URLS = 705
        ITEM_8_HEADER = 800
        ITEM_8_DESCRIPTION = 801
        ITEM_8_PRICE = 802
        ITEM_8_UNIT = 803
        ITEM_8_FINAL_URLS = 804
        ITEM_8_FINAL_MOBILE_URLS = 805


__all__ = tuple(sorted(__protobuf__.manifest))
