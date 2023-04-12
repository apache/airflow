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
    manifest={"RecommendationTypeEnum",},
)


class RecommendationTypeEnum(proto.Message):
    r"""Container for enum describing types of recommendations.
    """

    class RecommendationType(proto.Enum):
        r"""Types of recommendations."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CAMPAIGN_BUDGET = 2
        KEYWORD = 3
        TEXT_AD = 4
        TARGET_CPA_OPT_IN = 5
        MAXIMIZE_CONVERSIONS_OPT_IN = 6
        ENHANCED_CPC_OPT_IN = 7
        SEARCH_PARTNERS_OPT_IN = 8
        MAXIMIZE_CLICKS_OPT_IN = 9
        OPTIMIZE_AD_ROTATION = 10
        CALLOUT_EXTENSION = 11
        SITELINK_EXTENSION = 12
        CALL_EXTENSION = 13
        KEYWORD_MATCH_TYPE = 14
        MOVE_UNUSED_BUDGET = 15
        FORECASTING_CAMPAIGN_BUDGET = 16
        TARGET_ROAS_OPT_IN = 17
        RESPONSIVE_SEARCH_AD = 18
        MARGINAL_ROI_CAMPAIGN_BUDGET = 19
        USE_BROAD_MATCH_KEYWORD = 20
        RESPONSIVE_SEARCH_AD_ASSET = 21
        UPGRADE_SMART_SHOPPING_CAMPAIGN_TO_PERFORMANCE_MAX = 22
        RESPONSIVE_SEARCH_AD_IMPROVE_AD_STRENGTH = 23
        DISPLAY_EXPANSION_OPT_IN = 24
        UPGRADE_LOCAL_CAMPAIGN_TO_PERFORMANCE_MAX = 25
        RAISE_TARGET_CPA_BID_TOO_LOW = 26
        FORECASTING_SET_TARGET_ROAS = 27


__all__ = tuple(sorted(__protobuf__.manifest))
