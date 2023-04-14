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
    manifest={"AudienceInsightsDimensionEnum",},
)


class AudienceInsightsDimensionEnum(proto.Message):
    r"""Container for enum describing audience insights dimensions.
    """

    class AudienceInsightsDimension(proto.Enum):
        r"""Possible audience dimensions for use in generating insights."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CATEGORY = 2
        KNOWLEDGE_GRAPH = 3
        GEO_TARGET_COUNTRY = 4
        SUB_COUNTRY_LOCATION = 5
        YOUTUBE_CHANNEL = 6
        YOUTUBE_DYNAMIC_LINEUP = 7
        AFFINITY_USER_INTEREST = 8
        IN_MARKET_USER_INTEREST = 9
        PARENTAL_STATUS = 10
        INCOME_RANGE = 11
        AGE_RANGE = 12
        GENDER = 13


__all__ = tuple(sorted(__protobuf__.manifest))
