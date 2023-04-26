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
    manifest={"KeywordPlanErrorEnum",},
)


class KeywordPlanErrorEnum(proto.Message):
    r"""Container for enum describing possible errors from applying a
    keyword plan resource (keyword plan, keyword plan campaign,
    keyword plan ad group or keyword plan keyword) or
    KeywordPlanService RPC.

    """

    class KeywordPlanError(proto.Enum):
        r"""Enum describing possible errors from applying a keyword plan."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        BID_MULTIPLIER_OUT_OF_RANGE = 2
        BID_TOO_HIGH = 3
        BID_TOO_LOW = 4
        BID_TOO_MANY_FRACTIONAL_DIGITS = 5
        DAILY_BUDGET_TOO_LOW = 6
        DAILY_BUDGET_TOO_MANY_FRACTIONAL_DIGITS = 7
        INVALID_VALUE = 8
        KEYWORD_PLAN_HAS_NO_KEYWORDS = 9
        KEYWORD_PLAN_NOT_ENABLED = 10
        KEYWORD_PLAN_NOT_FOUND = 11
        MISSING_BID = 13
        MISSING_FORECAST_PERIOD = 14
        INVALID_FORECAST_DATE_RANGE = 15
        INVALID_NAME = 16


__all__ = tuple(sorted(__protobuf__.manifest))
