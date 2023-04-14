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
    manifest={"MediaFileErrorEnum",},
)


class MediaFileErrorEnum(proto.Message):
    r"""Container for enum describing possible media file errors.
    """

    class MediaFileError(proto.Enum):
        r"""Enum describing possible media file errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_CREATE_STANDARD_ICON = 2
        CANNOT_SELECT_STANDARD_ICON_WITH_OTHER_TYPES = 3
        CANNOT_SPECIFY_MEDIA_FILE_ID_AND_DATA = 4
        DUPLICATE_MEDIA = 5
        EMPTY_FIELD = 6
        RESOURCE_REFERENCED_IN_MULTIPLE_OPS = 7
        FIELD_NOT_SUPPORTED_FOR_MEDIA_SUB_TYPE = 8
        INVALID_MEDIA_FILE_ID = 9
        INVALID_MEDIA_SUB_TYPE = 10
        INVALID_MEDIA_FILE_TYPE = 11
        INVALID_MIME_TYPE = 12
        INVALID_REFERENCE_ID = 13
        INVALID_YOU_TUBE_ID = 14
        MEDIA_FILE_FAILED_TRANSCODING = 15
        MEDIA_NOT_TRANSCODED = 16
        MEDIA_TYPE_DOES_NOT_MATCH_MEDIA_FILE_TYPE = 17
        NO_FIELDS_SPECIFIED = 18
        NULL_REFERENCE_ID_AND_MEDIA_ID = 19
        TOO_LONG = 20
        UNSUPPORTED_TYPE = 21
        YOU_TUBE_SERVICE_UNAVAILABLE = 22
        YOU_TUBE_VIDEO_HAS_NON_POSITIVE_DURATION = 23
        YOU_TUBE_VIDEO_NOT_FOUND = 24


__all__ = tuple(sorted(__protobuf__.manifest))
