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
    manifest={"FeedErrorEnum",},
)


class FeedErrorEnum(proto.Message):
    r"""Container for enum describing possible feed errors.
    """

    class FeedError(proto.Enum):
        r"""Enum describing possible feed errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        ATTRIBUTE_NAMES_NOT_UNIQUE = 2
        ATTRIBUTES_DO_NOT_MATCH_EXISTING_ATTRIBUTES = 3
        CANNOT_SPECIFY_USER_ORIGIN_FOR_SYSTEM_FEED = 4
        CANNOT_SPECIFY_GOOGLE_ORIGIN_FOR_NON_SYSTEM_FEED = 5
        CANNOT_SPECIFY_FEED_ATTRIBUTES_FOR_SYSTEM_FEED = 6
        CANNOT_UPDATE_FEED_ATTRIBUTES_WITH_ORIGIN_GOOGLE = 7
        FEED_REMOVED = 8
        INVALID_ORIGIN_VALUE = 9
        FEED_ORIGIN_IS_NOT_USER = 10
        INVALID_AUTH_TOKEN_FOR_EMAIL = 11
        INVALID_EMAIL = 12
        DUPLICATE_FEED_NAME = 13
        INVALID_FEED_NAME = 14
        MISSING_OAUTH_INFO = 15
        NEW_ATTRIBUTE_CANNOT_BE_PART_OF_UNIQUE_KEY = 16
        TOO_MANY_ATTRIBUTES = 17
        INVALID_BUSINESS_ACCOUNT = 18
        BUSINESS_ACCOUNT_CANNOT_ACCESS_LOCATION_ACCOUNT = 19
        INVALID_AFFILIATE_CHAIN_ID = 20
        DUPLICATE_SYSTEM_FEED = 21
        GMB_ACCESS_ERROR = 22
        CANNOT_HAVE_LOCATION_AND_AFFILIATE_LOCATION_FEEDS = 23
        LEGACY_EXTENSION_TYPE_READ_ONLY = 24


__all__ = tuple(sorted(__protobuf__.manifest))
