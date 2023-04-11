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
    manifest={"TargetCpaOptInRecommendationGoalEnum",},
)


class TargetCpaOptInRecommendationGoalEnum(proto.Message):
    r"""Container for enum describing goals for TargetCpaOptIn
    recommendation.

    """

    class TargetCpaOptInRecommendationGoal(proto.Enum):
        r"""Goal of TargetCpaOptIn recommendation."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        SAME_COST = 2
        SAME_CONVERSIONS = 3
        SAME_CPA = 4
        CLOSEST_CPA = 5


__all__ = tuple(sorted(__protobuf__.manifest))
