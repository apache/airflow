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
    manifest={"ExternalConversionSourceEnum",},
)


class ExternalConversionSourceEnum(proto.Message):
    r"""Container for enum describing the external conversion source
    that is associated with a ConversionAction.

    """

    class ExternalConversionSource(proto.Enum):
        r"""The external conversion source that is associated with a
        ConversionAction.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        WEBPAGE = 2
        ANALYTICS = 3
        UPLOAD = 4
        AD_CALL_METRICS = 5
        WEBSITE_CALL_METRICS = 6
        STORE_VISITS = 7
        ANDROID_IN_APP = 8
        IOS_IN_APP = 9
        IOS_FIRST_OPEN = 10
        APP_UNSPECIFIED = 11
        ANDROID_FIRST_OPEN = 12
        UPLOAD_CALLS = 13
        FIREBASE = 14
        CLICK_TO_CALL = 15
        SALESFORCE = 16
        STORE_SALES_CRM = 17
        STORE_SALES_PAYMENT_NETWORK = 18
        GOOGLE_PLAY = 19
        THIRD_PARTY_APP_ANALYTICS = 20
        GOOGLE_ATTRIBUTION = 21
        STORE_SALES_DIRECT_UPLOAD = 23
        STORE_SALES = 24
        SEARCH_ADS_360 = 25
        GOOGLE_HOSTED = 27
        FLOODLIGHT = 29
        ANALYTICS_SEARCH_ADS_360 = 31
        FIREBASE_SEARCH_ADS_360 = 33
        DISPLAY_AND_VIDEO_360_FLOODLIGHT = 34


__all__ = tuple(sorted(__protobuf__.manifest))
