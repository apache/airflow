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
    manifest={"MediaUploadErrorEnum",},
)


class MediaUploadErrorEnum(proto.Message):
    r"""Container for enum describing possible media uploading
    errors.

    """

    class MediaUploadError(proto.Enum):
        r"""Enum describing possible media uploading errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        FILE_TOO_BIG = 2
        UNPARSEABLE_IMAGE = 3
        ANIMATED_IMAGE_NOT_ALLOWED = 4
        FORMAT_NOT_ALLOWED = 5
        EXTERNAL_URL_NOT_ALLOWED = 6
        INVALID_URL_REFERENCE = 7
        MISSING_PRIMARY_MEDIA_BUNDLE_ENTRY = 8
        ANIMATED_VISUAL_EFFECT = 9
        ANIMATION_TOO_LONG = 10
        ASPECT_RATIO_NOT_ALLOWED = 11
        AUDIO_NOT_ALLOWED_IN_MEDIA_BUNDLE = 12
        CMYK_JPEG_NOT_ALLOWED = 13
        FLASH_NOT_ALLOWED = 14
        FRAME_RATE_TOO_HIGH = 15
        GOOGLE_WEB_DESIGNER_ZIP_FILE_NOT_PUBLISHED = 16
        IMAGE_CONSTRAINTS_VIOLATED = 17
        INVALID_MEDIA_BUNDLE = 18
        INVALID_MEDIA_BUNDLE_ENTRY = 19
        INVALID_MIME_TYPE = 20
        INVALID_PATH = 21
        LAYOUT_PROBLEM = 22
        MALFORMED_URL = 23
        MEDIA_BUNDLE_NOT_ALLOWED = 24
        MEDIA_BUNDLE_NOT_COMPATIBLE_TO_PRODUCT_TYPE = 25
        MEDIA_BUNDLE_REJECTED_BY_MULTIPLE_ASSET_SPECS = 26
        TOO_MANY_FILES_IN_MEDIA_BUNDLE = 27
        UNSUPPORTED_GOOGLE_WEB_DESIGNER_ENVIRONMENT = 28
        UNSUPPORTED_HTML5_FEATURE = 29
        URL_IN_MEDIA_BUNDLE_NOT_SSL_COMPLIANT = 30
        VIDEO_FILE_NAME_TOO_LONG = 31
        VIDEO_MULTIPLE_FILES_WITH_SAME_NAME = 32
        VIDEO_NOT_ALLOWED_IN_MEDIA_BUNDLE = 33
        CANNOT_UPLOAD_MEDIA_TYPE_THROUGH_API = 34
        DIMENSIONS_NOT_ALLOWED = 35


__all__ = tuple(sorted(__protobuf__.manifest))
