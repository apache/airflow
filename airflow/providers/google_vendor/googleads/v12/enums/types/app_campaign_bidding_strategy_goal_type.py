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
    manifest={"AppCampaignBiddingStrategyGoalTypeEnum",},
)


class AppCampaignBiddingStrategyGoalTypeEnum(proto.Message):
    r"""Container for enum describing goal towards which the bidding
    strategy of an app campaign should optimize for.

    """

    class AppCampaignBiddingStrategyGoalType(proto.Enum):
        r"""Goal type of App campaign BiddingStrategy."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        OPTIMIZE_INSTALLS_TARGET_INSTALL_COST = 2
        OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST = 3
        OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST = 4
        OPTIMIZE_RETURN_ON_ADVERTISING_SPEND = 5
        OPTIMIZE_PRE_REGISTRATION_CONVERSION_VOLUME = 6
        OPTIMIZE_INSTALLS_WITHOUT_TARGET_INSTALL_COST = 7


__all__ = tuple(sorted(__protobuf__.manifest))
