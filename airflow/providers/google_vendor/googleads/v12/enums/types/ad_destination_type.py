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
    manifest={"AdDestinationTypeEnum",},
)


class AdDestinationTypeEnum(proto.Message):
    r"""Container for enumeration of Google Ads destination types.
    """

    class AdDestinationType(proto.Enum):
        r"""Enumerates Google Ads destination types"""
        UNSPECIFIED = 0
        UNKNOWN = 1
        NOT_APPLICABLE = 2
        WEBSITE = 3
        APP_DEEP_LINK = 4
        APP_STORE = 5
        PHONE_CALL = 6
        MAP_DIRECTIONS = 7
        LOCATION_LISTING = 8
        MESSAGE = 9
        LEAD_FORM = 10
        YOUTUBE = 11
        UNMODELED_FOR_CONVERSIONS = 12


__all__ = tuple(sorted(__protobuf__.manifest))
