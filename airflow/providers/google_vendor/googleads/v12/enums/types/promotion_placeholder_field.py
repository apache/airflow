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
    manifest={"PromotionPlaceholderFieldEnum",},
)


class PromotionPlaceholderFieldEnum(proto.Message):
    r"""Values for Promotion placeholder fields.
    """

    class PromotionPlaceholderField(proto.Enum):
        r"""Possible values for Promotion placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        PROMOTION_TARGET = 2
        DISCOUNT_MODIFIER = 3
        PERCENT_OFF = 4
        MONEY_AMOUNT_OFF = 5
        PROMOTION_CODE = 6
        ORDERS_OVER_AMOUNT = 7
        PROMOTION_START = 8
        PROMOTION_END = 9
        OCCASION = 10
        FINAL_URLS = 11
        FINAL_MOBILE_URLS = 12
        TRACKING_URL = 13
        LANGUAGE = 14
        FINAL_URL_SUFFIX = 15


__all__ = tuple(sorted(__protobuf__.manifest))
