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
    manifest={"MimeTypeEnum",},
)


class MimeTypeEnum(proto.Message):
    r"""Container for enum describing the mime types.
    """

    class MimeType(proto.Enum):
        r"""The mime type"""
        UNSPECIFIED = 0
        UNKNOWN = 1
        IMAGE_JPEG = 2
        IMAGE_GIF = 3
        IMAGE_PNG = 4
        FLASH = 5
        TEXT_HTML = 6
        PDF = 7
        MSWORD = 8
        MSEXCEL = 9
        RTF = 10
        AUDIO_WAV = 11
        AUDIO_MP3 = 12
        HTML5_AD_ZIP = 13


__all__ = tuple(sorted(__protobuf__.manifest))
