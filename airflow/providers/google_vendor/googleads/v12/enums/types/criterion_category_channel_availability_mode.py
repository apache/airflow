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
    manifest={"CriterionCategoryChannelAvailabilityModeEnum",},
)


class CriterionCategoryChannelAvailabilityModeEnum(proto.Message):
    r"""Describes channel availability mode for a criterion
    availability - whether the availability is meant to include all
    advertising channels, or a particular channel with all its
    channel subtypes, or a channel with a certain subset of channel
    subtypes.

    """

    class CriterionCategoryChannelAvailabilityMode(proto.Enum):
        r"""Enum containing the possible
        CriterionCategoryChannelAvailabilityMode.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        ALL_CHANNELS = 2
        CHANNEL_TYPE_AND_ALL_SUBTYPES = 3
        CHANNEL_TYPE_AND_SUBSET_SUBTYPES = 4


__all__ = tuple(sorted(__protobuf__.manifest))
