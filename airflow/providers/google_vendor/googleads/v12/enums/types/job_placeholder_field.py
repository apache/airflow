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
    manifest={"JobPlaceholderFieldEnum",},
)


class JobPlaceholderFieldEnum(proto.Message):
    r"""Values for Job placeholder fields.
    For more information about dynamic remarketing feeds, see
    https://support.google.com/google-ads/answer/6053288.

    """

    class JobPlaceholderField(proto.Enum):
        r"""Possible values for Job placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        JOB_ID = 2
        LOCATION_ID = 3
        TITLE = 4
        SUBTITLE = 5
        DESCRIPTION = 6
        IMAGE_URL = 7
        CATEGORY = 8
        CONTEXTUAL_KEYWORDS = 9
        ADDRESS = 10
        SALARY = 11
        FINAL_URLS = 12
        FINAL_MOBILE_URLS = 14
        TRACKING_URL = 15
        ANDROID_APP_LINK = 16
        SIMILAR_JOB_IDS = 17
        IOS_APP_LINK = 18
        IOS_APP_STORE_ID = 19


__all__ = tuple(sorted(__protobuf__.manifest))
