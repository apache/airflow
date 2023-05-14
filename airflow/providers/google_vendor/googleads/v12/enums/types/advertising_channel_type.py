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
    manifest={"AdvertisingChannelTypeEnum",},
)


class AdvertisingChannelTypeEnum(proto.Message):
    r"""The channel type a campaign may target to serve on.
    """

    class AdvertisingChannelType(proto.Enum):
        r"""Enum describing the various advertising channel types."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        SEARCH = 2
        DISPLAY = 3
        SHOPPING = 4
        HOTEL = 5
        VIDEO = 6
        MULTI_CHANNEL = 7
        LOCAL = 8
        SMART = 9
        PERFORMANCE_MAX = 10
        LOCAL_SERVICES = 11
        DISCOVERY = 12


__all__ = tuple(sorted(__protobuf__.manifest))
