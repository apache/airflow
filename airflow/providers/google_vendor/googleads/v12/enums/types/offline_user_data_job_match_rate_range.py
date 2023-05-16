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
    manifest={"OfflineUserDataJobMatchRateRangeEnum",},
)


class OfflineUserDataJobMatchRateRangeEnum(proto.Message):
    r"""Container for enum describing reasons match rate ranges for a
    customer match list upload.

    """

    class OfflineUserDataJobMatchRateRange(proto.Enum):
        r"""The match rate range of an offline user data job."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        MATCH_RANGE_LESS_THAN_20 = 2
        MATCH_RANGE_20_TO_30 = 3
        MATCH_RANGE_31_TO_40 = 4
        MATCH_RANGE_41_TO_50 = 5
        MATCH_RANGE_51_TO_60 = 6
        MATCH_RANGE_61_TO_70 = 7
        MATCH_RANGE_71_TO_80 = 8
        MATCH_RANGE_81_TO_90 = 9
        MATCH_RANGE_91_TO_100 = 10


__all__ = tuple(sorted(__protobuf__.manifest))
