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
    package="google.ads.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"ValueRuleGeoLocationMatchTypeEnum",},
)


class ValueRuleGeoLocationMatchTypeEnum(proto.Message):
    r"""Container for enum describing possible geographic location
    matching types used in a conversion value rule.

    """

    class ValueRuleGeoLocationMatchType(proto.Enum):
        r"""Possible geographic location matching types."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        ANY = 2
        LOCATION_OF_PRESENCE = 3


__all__ = tuple(sorted(__protobuf__.manifest))
