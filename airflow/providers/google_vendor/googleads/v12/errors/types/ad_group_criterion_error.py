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
    manifest={"AdGroupCriterionErrorEnum",},
)


class AdGroupCriterionErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group criterion
    errors.

    """

    class AdGroupCriterionError(proto.Enum):
        r"""Enum describing possible ad group criterion errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_GROUP_CRITERION_LABEL_DOES_NOT_EXIST = 2
        AD_GROUP_CRITERION_LABEL_ALREADY_EXISTS = 3
        CANNOT_ADD_LABEL_TO_NEGATIVE_CRITERION = 4
        TOO_MANY_OPERATIONS = 5
        CANT_UPDATE_NEGATIVE = 6
        CONCRETE_TYPE_REQUIRED = 7
        BID_INCOMPATIBLE_WITH_ADGROUP = 8
        CANNOT_TARGET_AND_EXCLUDE = 9
        ILLEGAL_URL = 10
        INVALID_KEYWORD_TEXT = 11
        INVALID_DESTINATION_URL = 12
        MISSING_DESTINATION_URL_TAG = 13
        KEYWORD_LEVEL_BID_NOT_SUPPORTED_FOR_MANUALCPM = 14
        INVALID_USER_STATUS = 15
        CANNOT_ADD_CRITERIA_TYPE = 16
        CANNOT_EXCLUDE_CRITERIA_TYPE = 17
        CAMPAIGN_TYPE_NOT_COMPATIBLE_WITH_PARTIAL_FAILURE = 27
        OPERATIONS_FOR_TOO_MANY_SHOPPING_ADGROUPS = 28
        CANNOT_MODIFY_URL_FIELDS_WITH_DUPLICATE_ELEMENTS = 29
        CANNOT_SET_WITHOUT_FINAL_URLS = 30
        CANNOT_CLEAR_FINAL_URLS_IF_FINAL_MOBILE_URLS_EXIST = 31
        CANNOT_CLEAR_FINAL_URLS_IF_FINAL_APP_URLS_EXIST = 32
        CANNOT_CLEAR_FINAL_URLS_IF_TRACKING_URL_TEMPLATE_EXISTS = 33
        CANNOT_CLEAR_FINAL_URLS_IF_URL_CUSTOM_PARAMETERS_EXIST = 34
        CANNOT_SET_BOTH_DESTINATION_URL_AND_FINAL_URLS = 35
        CANNOT_SET_BOTH_DESTINATION_URL_AND_TRACKING_URL_TEMPLATE = 36
        FINAL_URLS_NOT_SUPPORTED_FOR_CRITERION_TYPE = 37
        FINAL_MOBILE_URLS_NOT_SUPPORTED_FOR_CRITERION_TYPE = 38


__all__ = tuple(sorted(__protobuf__.manifest))
