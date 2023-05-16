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
    manifest={"VideoThumbnailEnum",},
)


class VideoThumbnailEnum(proto.Message):
    r"""Defines the thumbnail to use for In-Display video ads. Note that
    DEFAULT_THUMBNAIL may have been uploaded by the user while
    thumbnails 1-3 are auto-generated from the video.

    """

    class VideoThumbnail(proto.Enum):
        r"""Enum listing the possible types of a video thumbnail."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DEFAULT_THUMBNAIL = 2
        THUMBNAIL_1 = 3
        THUMBNAIL_2 = 4
        THUMBNAIL_3 = 5


__all__ = tuple(sorted(__protobuf__.manifest))
