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
    manifest={"CampaignCriterionErrorEnum",},
)


class CampaignCriterionErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign criterion
    errors.

    """

    class CampaignCriterionError(proto.Enum):
        r"""Enum describing possible campaign criterion errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CONCRETE_TYPE_REQUIRED = 2
        INVALID_PLACEMENT_URL = 3
        CANNOT_EXCLUDE_CRITERIA_TYPE = 4
        CANNOT_SET_STATUS_FOR_CRITERIA_TYPE = 5
        CANNOT_SET_STATUS_FOR_EXCLUDED_CRITERIA = 6
        CANNOT_TARGET_AND_EXCLUDE = 7
        TOO_MANY_OPERATIONS = 8
        OPERATOR_NOT_SUPPORTED_FOR_CRITERION_TYPE = 9
        SHOPPING_CAMPAIGN_SALES_COUNTRY_NOT_SUPPORTED_FOR_SALES_CHANNEL = 10
        CANNOT_ADD_EXISTING_FIELD = 11
        CANNOT_UPDATE_NEGATIVE_CRITERION = 12
        CANNOT_SET_NEGATIVE_KEYWORD_THEME_CONSTANT_CRITERION = 13
        INVALID_KEYWORD_THEME_CONSTANT = 14
        MISSING_KEYWORD_THEME_CONSTANT_OR_FREE_FORM_KEYWORD_THEME = 15
        CANNOT_TARGET_BOTH_PROXIMITY_AND_LOCATION_CRITERIA_FOR_SMART_CAMPAIGN = (
            16
        )
        CANNOT_TARGET_MULTIPLE_PROXIMITY_CRITERIA_FOR_SMART_CAMPAIGN = 17
        LOCATION_NOT_LAUNCHED_FOR_LOCAL_SERVICES_CAMPAIGN = 18
        LOCATION_INVALID_FOR_LOCAL_SERVICES_CAMPAIGN = 19
        CANNOT_TARGET_COUNTRY_FOR_LOCAL_SERVICES_CAMPAIGN = 20
        LOCATION_NOT_IN_HOME_COUNTRY_FOR_LOCAL_SERVICES_CAMPAIGN = 21
        CANNOT_ADD_OR_REMOVE_LOCATION_FOR_LOCAL_SERVICES_CAMPAIGN = 22
        AT_LEAST_ONE_POSITIVE_LOCATION_REQUIRED_FOR_LOCAL_SERVICES_CAMPAIGN = 23


__all__ = tuple(sorted(__protobuf__.manifest))
