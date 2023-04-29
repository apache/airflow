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
    manifest={"ImageErrorEnum",},
)


class ImageErrorEnum(proto.Message):
    r"""Container for enum describing possible image errors.
    """

    class ImageError(proto.Enum):
        r"""Enum describing possible image errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_IMAGE = 2
        STORAGE_ERROR = 3
        BAD_REQUEST = 4
        UNEXPECTED_SIZE = 5
        ANIMATED_NOT_ALLOWED = 6
        ANIMATION_TOO_LONG = 7
        SERVER_ERROR = 8
        CMYK_JPEG_NOT_ALLOWED = 9
        FLASH_NOT_ALLOWED = 10
        FLASH_WITHOUT_CLICKTAG = 11
        FLASH_ERROR_AFTER_FIXING_CLICK_TAG = 12
        ANIMATED_VISUAL_EFFECT = 13
        FLASH_ERROR = 14
        LAYOUT_PROBLEM = 15
        PROBLEM_READING_IMAGE_FILE = 16
        ERROR_STORING_IMAGE = 17
        ASPECT_RATIO_NOT_ALLOWED = 18
        FLASH_HAS_NETWORK_OBJECTS = 19
        FLASH_HAS_NETWORK_METHODS = 20
        FLASH_HAS_URL = 21
        FLASH_HAS_MOUSE_TRACKING = 22
        FLASH_HAS_RANDOM_NUM = 23
        FLASH_SELF_TARGETS = 24
        FLASH_BAD_GETURL_TARGET = 25
        FLASH_VERSION_NOT_SUPPORTED = 26
        FLASH_WITHOUT_HARD_CODED_CLICK_URL = 27
        INVALID_FLASH_FILE = 28
        FAILED_TO_FIX_CLICK_TAG_IN_FLASH = 29
        FLASH_ACCESSES_NETWORK_RESOURCES = 30
        FLASH_EXTERNAL_JS_CALL = 31
        FLASH_EXTERNAL_FS_CALL = 32
        FILE_TOO_LARGE = 33
        IMAGE_DATA_TOO_LARGE = 34
        IMAGE_PROCESSING_ERROR = 35
        IMAGE_TOO_SMALL = 36
        INVALID_INPUT = 37
        PROBLEM_READING_FILE = 38
        IMAGE_CONSTRAINTS_VIOLATED = 39
        FORMAT_NOT_ALLOWED = 40


__all__ = tuple(sorted(__protobuf__.manifest))
