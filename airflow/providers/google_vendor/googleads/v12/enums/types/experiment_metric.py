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
    manifest={"ExperimentMetricEnum",},
)


class ExperimentMetricEnum(proto.Message):
    r"""Container for enum describing the type of experiment metric.
    """

    class ExperimentMetric(proto.Enum):
        r"""The type of experiment metric."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CLICKS = 2
        IMPRESSIONS = 3
        COST = 4
        CONVERSIONS_PER_INTERACTION_RATE = 5
        COST_PER_CONVERSION = 6
        CONVERSIONS_VALUE_PER_COST = 7
        AVERAGE_CPC = 8
        CTR = 9
        INCREMENTAL_CONVERSIONS = 10
        COMPLETED_VIDEO_VIEWS = 11
        CUSTOM_ALGORITHMS = 12
        CONVERSIONS = 13
        CONVERSION_VALUE = 14


__all__ = tuple(sorted(__protobuf__.manifest))
