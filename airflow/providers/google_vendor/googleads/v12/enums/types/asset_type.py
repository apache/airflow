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
    manifest={"AssetTypeEnum",},
)


class AssetTypeEnum(proto.Message):
    r"""Container for enum describing the types of asset.
    """

    class AssetType(proto.Enum):
        r"""Enum describing possible types of asset."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        YOUTUBE_VIDEO = 2
        MEDIA_BUNDLE = 3
        IMAGE = 4
        TEXT = 5
        LEAD_FORM = 6
        BOOK_ON_GOOGLE = 7
        PROMOTION = 8
        CALLOUT = 9
        STRUCTURED_SNIPPET = 10
        SITELINK = 11
        PAGE_FEED = 12
        DYNAMIC_EDUCATION = 13
        MOBILE_APP = 14
        HOTEL_CALLOUT = 15
        CALL = 16
        PRICE = 17
        CALL_TO_ACTION = 18
        DYNAMIC_REAL_ESTATE = 19
        DYNAMIC_CUSTOM = 20
        DYNAMIC_HOTELS_AND_RENTALS = 21
        DYNAMIC_FLIGHTS = 22
        DISCOVERY_CAROUSEL_CARD = 23
        DYNAMIC_TRAVEL = 24
        DYNAMIC_LOCAL = 25
        DYNAMIC_JOBS = 26
        LOCATION = 27


__all__ = tuple(sorted(__protobuf__.manifest))
