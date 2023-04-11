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
    manifest={"BiddingStrategySystemStatusEnum",},
)


class BiddingStrategySystemStatusEnum(proto.Message):
    r"""Message describing BiddingStrategy system statuses.
    """

    class BiddingStrategySystemStatus(proto.Enum):
        r"""The possible system statuses of a BiddingStrategy."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        ENABLED = 2
        LEARNING_NEW = 3
        LEARNING_SETTING_CHANGE = 4
        LEARNING_BUDGET_CHANGE = 5
        LEARNING_COMPOSITION_CHANGE = 6
        LEARNING_CONVERSION_TYPE_CHANGE = 7
        LEARNING_CONVERSION_SETTING_CHANGE = 8
        LIMITED_BY_CPC_BID_CEILING = 9
        LIMITED_BY_CPC_BID_FLOOR = 10
        LIMITED_BY_DATA = 11
        LIMITED_BY_BUDGET = 12
        LIMITED_BY_LOW_PRIORITY_SPEND = 13
        LIMITED_BY_LOW_QUALITY = 14
        LIMITED_BY_INVENTORY = 15
        MISCONFIGURED_ZERO_ELIGIBILITY = 16
        MISCONFIGURED_CONVERSION_TYPES = 17
        MISCONFIGURED_CONVERSION_SETTINGS = 18
        MISCONFIGURED_SHARED_BUDGET = 19
        MISCONFIGURED_STRATEGY_TYPE = 20
        PAUSED = 21
        UNAVAILABLE = 22
        MULTIPLE_LEARNING = 23
        MULTIPLE_LIMITED = 24
        MULTIPLE_MISCONFIGURED = 25
        MULTIPLE = 26


__all__ = tuple(sorted(__protobuf__.manifest))
