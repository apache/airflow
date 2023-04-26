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
    manifest={"CampaignPrimaryStatusReasonEnum",},
)


class CampaignPrimaryStatusReasonEnum(proto.Message):
    r"""Container for enum describing possible campaign primary
    status reasons.

    """

    class CampaignPrimaryStatusReason(proto.Enum):
        r"""Enum describing the possible campaign primary status reasons.
        Provides insight into why a campaign is not serving or not
        serving optimally. These reasons are aggregated to determine an
        overall campaign primary status.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_REMOVED = 2
        CAMPAIGN_PAUSED = 3
        CAMPAIGN_PENDING = 4
        CAMPAIGN_ENDED = 5
        CAMPAIGN_DRAFT = 6
        BIDDING_STRATEGY_MISCONFIGURED = 7
        BIDDING_STRATEGY_LIMITED = 8
        BIDDING_STRATEGY_LEARNING = 9
        BIDDING_STRATEGY_CONSTRAINED = 10
        BUDGET_CONSTRAINED = 11
        BUDGET_MISCONFIGURED = 12
        SEARCH_VOLUME_LIMITED = 13
        AD_GROUPS_PAUSED = 14
        NO_AD_GROUPS = 15
        KEYWORDS_PAUSED = 16
        NO_KEYWORDS = 17
        AD_GROUP_ADS_PAUSED = 18
        NO_AD_GROUP_ADS = 19
        HAS_ADS_LIMITED_BY_POLICY = 20
        HAS_ADS_DISAPPROVED = 21
        MOST_ADS_UNDER_REVIEW = 22
        MISSING_LEAD_FORM_EXTENSION = 23
        MISSING_CALL_EXTENSION = 24
        LEAD_FORM_EXTENSION_UNDER_REVIEW = 25
        LEAD_FORM_EXTENSION_DISAPPROVED = 26
        CALL_EXTENSION_UNDER_REVIEW = 27
        CALL_EXTENSION_DISAPPROVED = 28
        NO_MOBILE_APPLICATION_AD_GROUP_CRITERIA = 29
        CAMPAIGN_GROUP_PAUSED = 30
        CAMPAIGN_GROUP_ALL_GROUP_BUDGETS_ENDED = 31
        APP_NOT_RELEASED = 32
        APP_PARTIALLY_RELEASED = 33


__all__ = tuple(sorted(__protobuf__.manifest))
