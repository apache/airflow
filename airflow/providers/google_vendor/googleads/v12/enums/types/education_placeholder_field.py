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
    manifest={"EducationPlaceholderFieldEnum",},
)


class EducationPlaceholderFieldEnum(proto.Message):
    r"""Values for Education placeholder fields.
    For more information about dynamic remarketing feeds, see
    https://support.google.com/google-ads/answer/6053288.

    """

    class EducationPlaceholderField(proto.Enum):
        r"""Possible values for Education placeholder fields."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        PROGRAM_ID = 2
        LOCATION_ID = 3
        PROGRAM_NAME = 4
        AREA_OF_STUDY = 5
        PROGRAM_DESCRIPTION = 6
        SCHOOL_NAME = 7
        ADDRESS = 8
        THUMBNAIL_IMAGE_URL = 9
        ALTERNATIVE_THUMBNAIL_IMAGE_URL = 10
        FINAL_URLS = 11
        FINAL_MOBILE_URLS = 12
        TRACKING_URL = 13
        CONTEXTUAL_KEYWORDS = 14
        ANDROID_APP_LINK = 15
        SIMILAR_PROGRAM_IDS = 16
        IOS_APP_LINK = 17
        IOS_APP_STORE_ID = 18


__all__ = tuple(sorted(__protobuf__.manifest))
