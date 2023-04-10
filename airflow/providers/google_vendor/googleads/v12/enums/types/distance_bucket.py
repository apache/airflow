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
    manifest={"DistanceBucketEnum",},
)


class DistanceBucketEnum(proto.Message):
    r"""Container for distance buckets of a user's distance from an
    advertiser's location extension.

    """

    class DistanceBucket(proto.Enum):
        r"""The distance bucket for a user's distance from an
        advertiser's location extension.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        WITHIN_700M = 2
        WITHIN_1KM = 3
        WITHIN_5KM = 4
        WITHIN_10KM = 5
        WITHIN_15KM = 6
        WITHIN_20KM = 7
        WITHIN_25KM = 8
        WITHIN_30KM = 9
        WITHIN_35KM = 10
        WITHIN_40KM = 11
        WITHIN_45KM = 12
        WITHIN_50KM = 13
        WITHIN_55KM = 14
        WITHIN_60KM = 15
        WITHIN_65KM = 16
        BEYOND_65KM = 17
        WITHIN_0_7MILES = 18
        WITHIN_1MILE = 19
        WITHIN_5MILES = 20
        WITHIN_10MILES = 21
        WITHIN_15MILES = 22
        WITHIN_20MILES = 23
        WITHIN_25MILES = 24
        WITHIN_30MILES = 25
        WITHIN_35MILES = 26
        WITHIN_40MILES = 27
        BEYOND_40MILES = 28


__all__ = tuple(sorted(__protobuf__.manifest))
