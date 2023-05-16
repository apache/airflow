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
    manifest={"ConversionValueRuleErrorEnum",},
)


class ConversionValueRuleErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion value rule
    errors.

    """

    class ConversionValueRuleError(proto.Enum):
        r"""Enum describing possible conversion value rule errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_GEO_TARGET_CONSTANT = 2
        CONFLICTING_INCLUDED_AND_EXCLUDED_GEO_TARGET = 3
        CONFLICTING_CONDITIONS = 4
        CANNOT_REMOVE_IF_INCLUDED_IN_VALUE_RULE_SET = 5
        CONDITION_NOT_ALLOWED = 6
        FIELD_MUST_BE_UNSET = 7
        CANNOT_PAUSE_UNLESS_VALUE_RULE_SET_IS_PAUSED = 8
        UNTARGETABLE_GEO_TARGET = 9
        INVALID_AUDIENCE_USER_LIST = 10
        INACCESSIBLE_USER_LIST = 11
        INVALID_AUDIENCE_USER_INTEREST = 12
        CANNOT_ADD_RULE_WITH_STATUS_REMOVED = 13


__all__ = tuple(sorted(__protobuf__.manifest))
