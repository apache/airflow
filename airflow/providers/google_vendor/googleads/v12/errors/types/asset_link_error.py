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
    manifest={"AssetLinkErrorEnum",},
)


class AssetLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible asset link errors.
    """

    class AssetLinkError(proto.Enum):
        r"""Enum describing possible asset link errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        PINNING_UNSUPPORTED = 2
        UNSUPPORTED_FIELD_TYPE = 3
        FIELD_TYPE_INCOMPATIBLE_WITH_ASSET_TYPE = 4
        FIELD_TYPE_INCOMPATIBLE_WITH_CAMPAIGN_TYPE = 5
        INCOMPATIBLE_ADVERTISING_CHANNEL_TYPE = 6
        IMAGE_NOT_WITHIN_SPECIFIED_DIMENSION_RANGE = 7
        INVALID_PINNED_FIELD = 8
        MEDIA_BUNDLE_ASSET_FILE_SIZE_TOO_LARGE = 9
        NOT_ENOUGH_AVAILABLE_ASSET_LINKS_FOR_VALID_COMBINATION = 10
        NOT_ENOUGH_AVAILABLE_ASSET_LINKS_WITH_FALLBACK = 11
        NOT_ENOUGH_AVAILABLE_ASSET_LINKS_WITH_FALLBACK_FOR_VALID_COMBINATION = (
            12
        )
        YOUTUBE_VIDEO_REMOVED = 13
        YOUTUBE_VIDEO_TOO_LONG = 14
        YOUTUBE_VIDEO_TOO_SHORT = 15
        EXCLUDED_PARENT_FIELD_TYPE = 16
        INVALID_STATUS = 17
        YOUTUBE_VIDEO_DURATION_NOT_DEFINED = 18
        CANNOT_CREATE_AUTOMATICALLY_CREATED_LINKS = 19
        CANNOT_LINK_TO_AUTOMATICALLY_CREATED_ASSET = 20
        CANNOT_MODIFY_ASSET_LINK_SOURCE = 21


__all__ = tuple(sorted(__protobuf__.manifest))
