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
    manifest={"CriterionTypeEnum",},
)


class CriterionTypeEnum(proto.Message):
    r"""The possible types of a criterion.
    """

    class CriterionType(proto.Enum):
        r"""Enum describing possible criterion types."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        KEYWORD = 2
        PLACEMENT = 3
        MOBILE_APP_CATEGORY = 4
        MOBILE_APPLICATION = 5
        DEVICE = 6
        LOCATION = 7
        LISTING_GROUP = 8
        AD_SCHEDULE = 9
        AGE_RANGE = 10
        GENDER = 11
        INCOME_RANGE = 12
        PARENTAL_STATUS = 13
        YOUTUBE_VIDEO = 14
        YOUTUBE_CHANNEL = 15
        USER_LIST = 16
        PROXIMITY = 17
        TOPIC = 18
        LISTING_SCOPE = 19
        LANGUAGE = 20
        IP_BLOCK = 21
        CONTENT_LABEL = 22
        CARRIER = 23
        USER_INTEREST = 24
        WEBPAGE = 25
        OPERATING_SYSTEM_VERSION = 26
        APP_PAYMENT_MODEL = 27
        MOBILE_DEVICE = 28
        CUSTOM_AFFINITY = 29
        CUSTOM_INTENT = 30
        LOCATION_GROUP = 31
        CUSTOM_AUDIENCE = 32
        COMBINED_AUDIENCE = 33
        KEYWORD_THEME = 34
        AUDIENCE = 35


__all__ = tuple(sorted(__protobuf__.manifest))
