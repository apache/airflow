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
    manifest={"PlaceholderTypeEnum",},
)


class PlaceholderTypeEnum(proto.Message):
    r"""Container for enum describing possible placeholder types for
    a feed mapping.

    """

    class PlaceholderType(proto.Enum):
        r"""Possible placeholder types for a feed mapping."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        SITELINK = 2
        CALL = 3
        APP = 4
        LOCATION = 5
        AFFILIATE_LOCATION = 6
        CALLOUT = 7
        STRUCTURED_SNIPPET = 8
        MESSAGE = 9
        PRICE = 10
        PROMOTION = 11
        AD_CUSTOMIZER = 12
        DYNAMIC_EDUCATION = 13
        DYNAMIC_FLIGHT = 14
        DYNAMIC_CUSTOM = 15
        DYNAMIC_HOTEL = 16
        DYNAMIC_REAL_ESTATE = 17
        DYNAMIC_TRAVEL = 18
        DYNAMIC_LOCAL = 19
        DYNAMIC_JOB = 20
        IMAGE = 21


__all__ = tuple(sorted(__protobuf__.manifest))
