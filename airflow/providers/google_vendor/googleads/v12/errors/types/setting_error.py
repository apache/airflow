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
    manifest={"SettingErrorEnum",},
)


class SettingErrorEnum(proto.Message):
    r"""Container for enum describing possible setting errors.
    """

    class SettingError(proto.Enum):
        r"""Enum describing possible setting errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        SETTING_TYPE_IS_NOT_AVAILABLE = 3
        SETTING_TYPE_IS_NOT_COMPATIBLE_WITH_CAMPAIGN = 4
        TARGETING_SETTING_CONTAINS_INVALID_CRITERION_TYPE_GROUP = 5
        TARGETING_SETTING_DEMOGRAPHIC_CRITERION_TYPE_GROUPS_MUST_BE_SET_TO_TARGET_ALL = (
            6
        )
        TARGETING_SETTING_CANNOT_CHANGE_TARGET_ALL_TO_FALSE_FOR_DEMOGRAPHIC_CRITERION_TYPE_GROUP = (
            7
        )
        DYNAMIC_SEARCH_ADS_SETTING_AT_LEAST_ONE_FEED_ID_MUST_BE_PRESENT = 8
        DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_INVALID_DOMAIN_NAME = 9
        DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_SUBDOMAIN_NAME = 10
        DYNAMIC_SEARCH_ADS_SETTING_CONTAINS_INVALID_LANGUAGE_CODE = 11
        TARGET_ALL_IS_NOT_ALLOWED_FOR_PLACEMENT_IN_SEARCH_CAMPAIGN = 12
        SETTING_VALUE_NOT_COMPATIBLE_WITH_CAMPAIGN = 20
        BID_ONLY_IS_NOT_ALLOWED_TO_BE_MODIFIED_WITH_CUSTOMER_MATCH_TARGETING = (
            21
        )


__all__ = tuple(sorted(__protobuf__.manifest))
