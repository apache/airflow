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
    manifest={"ConversionValueRuleSetErrorEnum",},
)


class ConversionValueRuleSetErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion value rule
    set errors.

    """

    class ConversionValueRuleSetError(proto.Enum):
        r"""Enum describing possible conversion value rule set errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CONFLICTING_VALUE_RULE_CONDITIONS = 2
        INVALID_VALUE_RULE = 3
        DIMENSIONS_UPDATE_ONLY_ALLOW_APPEND = 4
        CONDITION_TYPE_NOT_ALLOWED = 5
        DUPLICATE_DIMENSIONS = 6
        INVALID_CAMPAIGN_ID = 7
        CANNOT_PAUSE_UNLESS_ALL_VALUE_RULES_ARE_PAUSED = 8
        SHOULD_PAUSE_WHEN_ALL_VALUE_RULES_ARE_PAUSED = 9
        VALUE_RULES_NOT_SUPPORTED_FOR_CAMPAIGN_TYPE = 10
        INELIGIBLE_CONVERSION_ACTION_CATEGORIES = 11
        DIMENSION_NO_CONDITION_USED_WITH_OTHER_DIMENSIONS = 12
        DIMENSION_NO_CONDITION_NOT_ALLOWED = 13
        UNSUPPORTED_CONVERSION_ACTION_CATEGORIES = 14


__all__ = tuple(sorted(__protobuf__.manifest))
