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
    manifest={"AssetFieldTypeEnum",},
)


class AssetFieldTypeEnum(proto.Message):
    r"""Container for enum describing the possible placements of an
    asset.

    """

    class AssetFieldType(proto.Enum):
        r"""Enum describing the possible placements of an asset."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        HEADLINE = 2
        DESCRIPTION = 3
        MANDATORY_AD_TEXT = 4
        MARKETING_IMAGE = 5
        MEDIA_BUNDLE = 6
        YOUTUBE_VIDEO = 7
        BOOK_ON_GOOGLE = 8
        LEAD_FORM = 9
        PROMOTION = 10
        CALLOUT = 11
        STRUCTURED_SNIPPET = 12
        SITELINK = 13
        MOBILE_APP = 14
        HOTEL_CALLOUT = 15
        CALL = 16
        PRICE = 24
        LONG_HEADLINE = 17
        BUSINESS_NAME = 18
        SQUARE_MARKETING_IMAGE = 19
        PORTRAIT_MARKETING_IMAGE = 20
        LOGO = 21
        LANDSCAPE_LOGO = 22
        VIDEO = 23
        CALL_TO_ACTION_SELECTION = 25
        AD_IMAGE = 26


__all__ = tuple(sorted(__protobuf__.manifest))
