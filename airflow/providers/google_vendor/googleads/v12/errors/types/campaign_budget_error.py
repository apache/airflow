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
    manifest={"CampaignBudgetErrorEnum",},
)


class CampaignBudgetErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign budget
    errors.

    """

    class CampaignBudgetError(proto.Enum):
        r"""Enum describing possible campaign budget errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_BUDGET_CANNOT_BE_SHARED = 17
        CAMPAIGN_BUDGET_REMOVED = 2
        CAMPAIGN_BUDGET_IN_USE = 3
        CAMPAIGN_BUDGET_PERIOD_NOT_AVAILABLE = 4
        CANNOT_MODIFY_FIELD_OF_IMPLICITLY_SHARED_CAMPAIGN_BUDGET = 6
        CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_IMPLICITLY_SHARED = 7
        CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_EXPLICITLY_SHARED_WITHOUT_NAME = 8
        CANNOT_UPDATE_CAMPAIGN_BUDGET_TO_EXPLICITLY_SHARED = 9
        CANNOT_USE_IMPLICITLY_SHARED_CAMPAIGN_BUDGET_WITH_MULTIPLE_CAMPAIGNS = (
            10
        )
        DUPLICATE_NAME = 11
        MONEY_AMOUNT_IN_WRONG_CURRENCY = 12
        MONEY_AMOUNT_LESS_THAN_CURRENCY_MINIMUM_CPC = 13
        MONEY_AMOUNT_TOO_LARGE = 14
        NEGATIVE_MONEY_AMOUNT = 15
        NON_MULTIPLE_OF_MINIMUM_CURRENCY_UNIT = 16
        TOTAL_BUDGET_AMOUNT_MUST_BE_UNSET_FOR_BUDGET_PERIOD_DAILY = 18
        INVALID_PERIOD = 19


__all__ = tuple(sorted(__protobuf__.manifest))
