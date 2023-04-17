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
    manifest={"ReachPlanAgeRangeEnum",},
)


class ReachPlanAgeRangeEnum(proto.Message):
    r"""Message describing plannable age ranges.
    """

    class ReachPlanAgeRange(proto.Enum):
        r"""Possible plannable age range values."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        AGE_RANGE_18_24 = 503001
        AGE_RANGE_18_34 = 2
        AGE_RANGE_18_44 = 3
        AGE_RANGE_18_49 = 4
        AGE_RANGE_18_54 = 5
        AGE_RANGE_18_64 = 6
        AGE_RANGE_18_65_UP = 7
        AGE_RANGE_21_34 = 8
        AGE_RANGE_25_34 = 503002
        AGE_RANGE_25_44 = 9
        AGE_RANGE_25_49 = 10
        AGE_RANGE_25_54 = 11
        AGE_RANGE_25_64 = 12
        AGE_RANGE_25_65_UP = 13
        AGE_RANGE_35_44 = 503003
        AGE_RANGE_35_49 = 14
        AGE_RANGE_35_54 = 15
        AGE_RANGE_35_64 = 16
        AGE_RANGE_35_65_UP = 17
        AGE_RANGE_45_54 = 503004
        AGE_RANGE_45_64 = 18
        AGE_RANGE_45_65_UP = 19
        AGE_RANGE_50_65_UP = 20
        AGE_RANGE_55_64 = 503005
        AGE_RANGE_55_65_UP = 21
        AGE_RANGE_65_UP = 503006


__all__ = tuple(sorted(__protobuf__.manifest))
