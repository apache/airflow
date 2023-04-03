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
    manifest={"FeedMappingErrorEnum",},
)


class FeedMappingErrorEnum(proto.Message):
    r"""Container for enum describing possible feed item errors.
    """

    class FeedMappingError(proto.Enum):
        r"""Enum describing possible feed item errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_PLACEHOLDER_FIELD = 2
        INVALID_CRITERION_FIELD = 3
        INVALID_PLACEHOLDER_TYPE = 4
        INVALID_CRITERION_TYPE = 5
        NO_ATTRIBUTE_FIELD_MAPPINGS = 7
        FEED_ATTRIBUTE_TYPE_MISMATCH = 8
        CANNOT_OPERATE_ON_MAPPINGS_FOR_SYSTEM_GENERATED_FEED = 9
        MULTIPLE_MAPPINGS_FOR_PLACEHOLDER_TYPE = 10
        MULTIPLE_MAPPINGS_FOR_CRITERION_TYPE = 11
        MULTIPLE_MAPPINGS_FOR_PLACEHOLDER_FIELD = 12
        MULTIPLE_MAPPINGS_FOR_CRITERION_FIELD = 13
        UNEXPECTED_ATTRIBUTE_FIELD_MAPPINGS = 14
        LOCATION_PLACEHOLDER_ONLY_FOR_PLACES_FEEDS = 15
        CANNOT_MODIFY_MAPPINGS_FOR_TYPED_FEED = 16
        INVALID_PLACEHOLDER_TYPE_FOR_NON_SYSTEM_GENERATED_FEED = 17
        INVALID_PLACEHOLDER_TYPE_FOR_SYSTEM_GENERATED_FEED_TYPE = 18
        ATTRIBUTE_FIELD_MAPPING_MISSING_FIELD = 19
        LEGACY_FEED_TYPE_READ_ONLY = 20


__all__ = tuple(sorted(__protobuf__.manifest))
