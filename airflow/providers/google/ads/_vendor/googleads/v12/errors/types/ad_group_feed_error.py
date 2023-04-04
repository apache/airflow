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
    package="google.ads.googleads.v12.errors",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupFeedErrorEnum",},
)


class AdGroupFeedErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group feed errors.
    """

    class AdGroupFeedError(proto.Enum):
        r"""Enum describing possible ad group feed errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE = 2
        CANNOT_CREATE_FOR_REMOVED_FEED = 3
        ADGROUP_FEED_ALREADY_EXISTS = 4
        CANNOT_OPERATE_ON_REMOVED_ADGROUP_FEED = 5
        INVALID_PLACEHOLDER_TYPE = 6
        MISSING_FEEDMAPPING_FOR_PLACEHOLDER_TYPE = 7
        NO_EXISTING_LOCATION_CUSTOMER_FEED = 8


__all__ = tuple(sorted(__protobuf__.manifest))
