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
    manifest={"ContentLabelTypeEnum",},
)


class ContentLabelTypeEnum(proto.Message):
    r"""Container for enum describing content label types in
    ContentLabel.

    """

    class ContentLabelType(proto.Enum):
        r"""Enum listing the content label types supported by
        ContentLabel criterion.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        SEXUALLY_SUGGESTIVE = 2
        BELOW_THE_FOLD = 3
        PARKED_DOMAIN = 4
        JUVENILE = 6
        PROFANITY = 7
        TRAGEDY = 8
        VIDEO = 9
        VIDEO_RATING_DV_G = 10
        VIDEO_RATING_DV_PG = 11
        VIDEO_RATING_DV_T = 12
        VIDEO_RATING_DV_MA = 13
        VIDEO_NOT_YET_RATED = 14
        EMBEDDED_VIDEO = 15
        LIVE_STREAMING_VIDEO = 16
        SOCIAL_ISSUES = 17


__all__ = tuple(sorted(__protobuf__.manifest))
