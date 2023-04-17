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
    manifest={"ExperimentTypeEnum",},
)


class ExperimentTypeEnum(proto.Message):
    r"""Container for enum describing the type of experiment.
    """

    class ExperimentType(proto.Enum):
        r"""The type of the experiment."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DISPLAY_AND_VIDEO_360 = 2
        AD_VARIATION = 3
        YOUTUBE_CUSTOM = 5
        DISPLAY_CUSTOM = 6
        SEARCH_CUSTOM = 7
        DISPLAY_AUTOMATED_BIDDING_STRATEGY = 8
        SEARCH_AUTOMATED_BIDDING_STRATEGY = 9
        SHOPPING_AUTOMATED_BIDDING_STRATEGY = 10
        SMART_MATCHING = 11
        HOTEL_CUSTOM = 12


__all__ = tuple(sorted(__protobuf__.manifest))
