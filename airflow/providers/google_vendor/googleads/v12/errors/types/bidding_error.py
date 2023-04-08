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
    package="airflow.providers.google_vendor.googleads.v12.errors",
    marshal="google.ads.googleads.v12",
    manifest={"BiddingErrorEnum",},
)


class BiddingErrorEnum(proto.Message):
    r"""Container for enum describing possible bidding errors.
    """

    class BiddingError(proto.Enum):
        r"""Enum describing possible bidding errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        BIDDING_STRATEGY_TRANSITION_NOT_ALLOWED = 2
        CANNOT_ATTACH_BIDDING_STRATEGY_TO_CAMPAIGN = 7
        INVALID_ANONYMOUS_BIDDING_STRATEGY_TYPE = 10
        INVALID_BIDDING_STRATEGY_TYPE = 14
        INVALID_BID = 17
        BIDDING_STRATEGY_NOT_AVAILABLE_FOR_ACCOUNT_TYPE = 18
        CONVERSION_TRACKING_NOT_ENABLED = 19
        NOT_ENOUGH_CONVERSIONS = 20
        CANNOT_CREATE_CAMPAIGN_WITH_BIDDING_STRATEGY = 21
        CANNOT_TARGET_CONTENT_NETWORK_ONLY_WITH_CAMPAIGN_LEVEL_POP_BIDDING_STRATEGY = (
            23
        )
        BIDDING_STRATEGY_NOT_SUPPORTED_WITH_AD_SCHEDULE = 24
        PAY_PER_CONVERSION_NOT_AVAILABLE_FOR_CUSTOMER = 25
        PAY_PER_CONVERSION_NOT_ALLOWED_WITH_TARGET_CPA = 26
        BIDDING_STRATEGY_NOT_ALLOWED_FOR_SEARCH_ONLY_CAMPAIGNS = 27
        BIDDING_STRATEGY_NOT_SUPPORTED_IN_DRAFTS_OR_EXPERIMENTS = 28
        BIDDING_STRATEGY_TYPE_DOES_NOT_SUPPORT_PRODUCT_TYPE_ADGROUP_CRITERION = (
            29
        )
        BID_TOO_SMALL = 30
        BID_TOO_BIG = 31
        BID_TOO_MANY_FRACTIONAL_DIGITS = 32
        INVALID_DOMAIN_NAME = 33
        NOT_COMPATIBLE_WITH_PAYMENT_MODE = 34
        NOT_COMPATIBLE_WITH_BUDGET_TYPE = 35
        NOT_COMPATIBLE_WITH_BIDDING_STRATEGY_TYPE = 36
        BIDDING_STRATEGY_TYPE_INCOMPATIBLE_WITH_SHARED_BUDGET = 37
        BIDDING_STRATEGY_AND_BUDGET_MUST_BE_ALIGNED = 38
        BIDDING_STRATEGY_AND_BUDGET_MUST_BE_ATTACHED_TO_THE_SAME_CAMPAIGNS_TO_ALIGN = (
            39
        )
        BIDDING_STRATEGY_AND_BUDGET_MUST_BE_REMOVED_TOGETHER = 40


__all__ = tuple(sorted(__protobuf__.manifest))
