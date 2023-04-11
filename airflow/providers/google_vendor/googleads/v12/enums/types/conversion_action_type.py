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
    manifest={"ConversionActionTypeEnum",},
)


class ConversionActionTypeEnum(proto.Message):
    r"""Container for enum describing possible types of a conversion
    action.

    """

    class ConversionActionType(proto.Enum):
        r"""Possible types of a conversion action."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_CALL = 2
        CLICK_TO_CALL = 3
        GOOGLE_PLAY_DOWNLOAD = 4
        GOOGLE_PLAY_IN_APP_PURCHASE = 5
        UPLOAD_CALLS = 6
        UPLOAD_CLICKS = 7
        WEBPAGE = 8
        WEBSITE_CALL = 9
        STORE_SALES_DIRECT_UPLOAD = 10
        STORE_SALES = 11
        FIREBASE_ANDROID_FIRST_OPEN = 12
        FIREBASE_ANDROID_IN_APP_PURCHASE = 13
        FIREBASE_ANDROID_CUSTOM = 14
        FIREBASE_IOS_FIRST_OPEN = 15
        FIREBASE_IOS_IN_APP_PURCHASE = 16
        FIREBASE_IOS_CUSTOM = 17
        THIRD_PARTY_APP_ANALYTICS_ANDROID_FIRST_OPEN = 18
        THIRD_PARTY_APP_ANALYTICS_ANDROID_IN_APP_PURCHASE = 19
        THIRD_PARTY_APP_ANALYTICS_ANDROID_CUSTOM = 20
        THIRD_PARTY_APP_ANALYTICS_IOS_FIRST_OPEN = 21
        THIRD_PARTY_APP_ANALYTICS_IOS_IN_APP_PURCHASE = 22
        THIRD_PARTY_APP_ANALYTICS_IOS_CUSTOM = 23
        ANDROID_APP_PRE_REGISTRATION = 24
        ANDROID_INSTALLS_ALL_OTHER_APPS = 25
        FLOODLIGHT_ACTION = 26
        FLOODLIGHT_TRANSACTION = 27
        GOOGLE_HOSTED = 28
        LEAD_FORM_SUBMIT = 29
        SALESFORCE = 30
        SEARCH_ADS_360 = 31
        SMART_CAMPAIGN_AD_CLICKS_TO_CALL = 32
        SMART_CAMPAIGN_MAP_CLICKS_TO_CALL = 33
        SMART_CAMPAIGN_MAP_DIRECTIONS = 34
        SMART_CAMPAIGN_TRACKED_CALLS = 35
        STORE_VISITS = 36


__all__ = tuple(sorted(__protobuf__.manifest))
