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
    manifest={"FeedItemErrorEnum",},
)


class FeedItemErrorEnum(proto.Message):
    r"""Container for enum describing possible feed item errors.
    """

    class FeedItemError(proto.Enum):
        r"""Enum describing possible feed item errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_CONVERT_ATTRIBUTE_VALUE_FROM_STRING = 2
        CANNOT_OPERATE_ON_REMOVED_FEED_ITEM = 3
        DATE_TIME_MUST_BE_IN_ACCOUNT_TIME_ZONE = 4
        KEY_ATTRIBUTES_NOT_FOUND = 5
        INVALID_URL = 6
        MISSING_KEY_ATTRIBUTES = 7
        KEY_ATTRIBUTES_NOT_UNIQUE = 8
        CANNOT_MODIFY_KEY_ATTRIBUTE_VALUE = 9
        SIZE_TOO_LARGE_FOR_MULTI_VALUE_ATTRIBUTE = 10
        LEGACY_FEED_TYPE_READ_ONLY = 11


__all__ = tuple(sorted(__protobuf__.manifest))
