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
    manifest={"AgeRangeTypeEnum",},
)


class AgeRangeTypeEnum(proto.Message):
    r"""Container for enum describing the type of demographic age
    ranges.

    """

    class AgeRangeType(proto.Enum):
        r"""The type of demographic age ranges (for example, between 18
        and 24 years old).
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        AGE_RANGE_18_24 = 503001
        AGE_RANGE_25_34 = 503002
        AGE_RANGE_35_44 = 503003
        AGE_RANGE_45_54 = 503004
        AGE_RANGE_55_64 = 503005
        AGE_RANGE_65_UP = 503006
        AGE_RANGE_UNDETERMINED = 503999


__all__ = tuple(sorted(__protobuf__.manifest))
