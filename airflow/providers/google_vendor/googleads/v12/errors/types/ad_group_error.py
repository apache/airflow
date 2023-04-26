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
    manifest={"AdGroupErrorEnum",},
)


class AdGroupErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group errors.
    """

    class AdGroupError(proto.Enum):
        r"""Enum describing possible ad group errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_ADGROUP_NAME = 2
        INVALID_ADGROUP_NAME = 3
        ADVERTISER_NOT_ON_CONTENT_NETWORK = 5
        BID_TOO_BIG = 6
        BID_TYPE_AND_BIDDING_STRATEGY_MISMATCH = 7
        MISSING_ADGROUP_NAME = 8
        ADGROUP_LABEL_DOES_NOT_EXIST = 9
        ADGROUP_LABEL_ALREADY_EXISTS = 10
        INVALID_CONTENT_BID_CRITERION_TYPE_GROUP = 11
        AD_GROUP_TYPE_NOT_VALID_FOR_ADVERTISING_CHANNEL_TYPE = 12
        ADGROUP_TYPE_NOT_SUPPORTED_FOR_CAMPAIGN_SALES_COUNTRY = 13
        CANNOT_ADD_ADGROUP_OF_TYPE_DSA_TO_CAMPAIGN_WITHOUT_DSA_SETTING = 14
        PROMOTED_HOTEL_AD_GROUPS_NOT_AVAILABLE_FOR_CUSTOMER = 15
        INVALID_EXCLUDED_PARENT_ASSET_FIELD_TYPE = 16


__all__ = tuple(sorted(__protobuf__.manifest))
