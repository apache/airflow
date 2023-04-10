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
    manifest={"BiddingStrategyTypeEnum",},
)


class BiddingStrategyTypeEnum(proto.Message):
    r"""Container for enum describing possible bidding strategy
    types.

    """

    class BiddingStrategyType(proto.Enum):
        r"""Enum describing possible bidding strategy types."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        COMMISSION = 16
        ENHANCED_CPC = 2
        INVALID = 17
        MANUAL_CPA = 18
        MANUAL_CPC = 3
        MANUAL_CPM = 4
        MANUAL_CPV = 13
        MAXIMIZE_CONVERSIONS = 10
        MAXIMIZE_CONVERSION_VALUE = 11
        PAGE_ONE_PROMOTED = 5
        PERCENT_CPC = 12
        TARGET_CPA = 6
        TARGET_CPM = 14
        TARGET_IMPRESSION_SHARE = 15
        TARGET_OUTRANK_SHARE = 7
        TARGET_ROAS = 8
        TARGET_SPEND = 9


__all__ = tuple(sorted(__protobuf__.manifest))
