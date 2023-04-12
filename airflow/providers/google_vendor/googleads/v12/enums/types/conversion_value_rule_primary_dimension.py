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
    manifest={"ConversionValueRulePrimaryDimensionEnum",},
)


class ConversionValueRulePrimaryDimensionEnum(proto.Message):
    r"""Container for enum describing value rule primary dimension
    for stats.

    """

    class ConversionValueRulePrimaryDimension(proto.Enum):
        r"""Identifies the primary dimension for conversion value rule
        stats.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        NO_RULE_APPLIED = 2
        ORIGINAL = 3
        NEW_VS_RETURNING_USER = 4
        GEO_LOCATION = 5
        DEVICE = 6
        AUDIENCE = 7
        MULTIPLE = 8


__all__ = tuple(sorted(__protobuf__.manifest))
