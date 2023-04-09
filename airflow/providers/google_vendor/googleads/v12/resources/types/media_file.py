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

from airflow.providers.google_vendor.googleads.v12.enums.types import media_type
from airflow.providers.google_vendor.googleads.v12.enums.types import mime_type as gage_mime_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={
        "MediaFile",
        "MediaImage",
        "MediaBundle",
        "MediaAudio",
        "MediaVideo",
    },
)


class MediaFile(proto.Message):
    r"""A media file.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the media file. Media file
            resource names have the form:

            ``customers/{customer_id}/mediaFiles/{media_file_id}``
        id (int):
            Output only. The ID of the media file.

            This field is a member of `oneof`_ ``_id``.
        type_ (google.ads.googleads.v12.enums.types.MediaTypeEnum.MediaType):
            Immutable. Type of the media file.
        mime_type (google.ads.googleads.v12.enums.types.MimeTypeEnum.MimeType):
            Output only. The mime type of the media file.
        source_url (str):
            Immutable. The URL of where the original
            media file was downloaded from (or a file name).
            Only used for media of type AUDIO and IMAGE.

            This field is a member of `oneof`_ ``_source_url``.
        name (str):
            Immutable. The name of the media file. The
            name can be used by clients to help identify
            previously uploaded media.

            This field is a member of `oneof`_ ``_name``.
        file_size (int):
            Output only. The size of the media file in
            bytes.

            This field is a member of `oneof`_ ``_file_size``.
        image (google.ads.googleads.v12.resources.types.MediaImage):
            Immutable. Encapsulates an Image.

            This field is a member of `oneof`_ ``mediatype``.
        media_bundle (google.ads.googleads.v12.resources.types.MediaBundle):
            Immutable. A ZIP archive media the content of
            which contains HTML5 assets.

            This field is a member of `oneof`_ ``mediatype``.
        audio (google.ads.googleads.v12.resources.types.MediaAudio):
            Output only. Encapsulates an Audio.

            This field is a member of `oneof`_ ``mediatype``.
        video (google.ads.googleads.v12.resources.types.MediaVideo):
            Immutable. Encapsulates a Video.

            This field is a member of `oneof`_ ``mediatype``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=12, optional=True,)
    type_ = proto.Field(
        proto.ENUM, number=5, enum=media_type.MediaTypeEnum.MediaType,
    )
    mime_type = proto.Field(
        proto.ENUM, number=6, enum=gage_mime_type.MimeTypeEnum.MimeType,
    )
    source_url = proto.Field(proto.STRING, number=13, optional=True,)
    name = proto.Field(proto.STRING, number=14, optional=True,)
    file_size = proto.Field(proto.INT64, number=15, optional=True,)
    image = proto.Field(
        proto.MESSAGE, number=3, oneof="mediatype", message="MediaImage",
    )
    media_bundle = proto.Field(
        proto.MESSAGE, number=4, oneof="mediatype", message="MediaBundle",
    )
    audio = proto.Field(
        proto.MESSAGE, number=10, oneof="mediatype", message="MediaAudio",
    )
    video = proto.Field(
        proto.MESSAGE, number=11, oneof="mediatype", message="MediaVideo",
    )


class MediaImage(proto.Message):
    r"""Encapsulates an Image.

    Attributes:
        data (bytes):
            Immutable. Raw image data.

            This field is a member of `oneof`_ ``_data``.
        full_size_image_url (str):
            Output only. The url to the full size version
            of the image.

            This field is a member of `oneof`_ ``_full_size_image_url``.
        preview_size_image_url (str):
            Output only. The url to the preview size
            version of the image.

            This field is a member of `oneof`_ ``_preview_size_image_url``.
    """

    data = proto.Field(proto.BYTES, number=4, optional=True,)
    full_size_image_url = proto.Field(proto.STRING, number=2, optional=True,)
    preview_size_image_url = proto.Field(proto.STRING, number=3, optional=True,)


class MediaBundle(proto.Message):
    r"""Represents a ZIP archive media the content of which contains
    HTML5 assets.

    Attributes:
        data (bytes):
            Immutable. Raw zipped data.

            This field is a member of `oneof`_ ``_data``.
        url (str):
            Output only. The url to access the uploaded
            zipped data. For example,
            https://tpc.googlesyndication.com/simgad/123
            This field is read-only.

            This field is a member of `oneof`_ ``_url``.
    """

    data = proto.Field(proto.BYTES, number=3, optional=True,)
    url = proto.Field(proto.STRING, number=2, optional=True,)


class MediaAudio(proto.Message):
    r"""Encapsulates an Audio.

    Attributes:
        ad_duration_millis (int):
            Output only. The duration of the Audio in
            milliseconds.

            This field is a member of `oneof`_ ``_ad_duration_millis``.
    """

    ad_duration_millis = proto.Field(proto.INT64, number=2, optional=True,)


class MediaVideo(proto.Message):
    r"""Encapsulates a Video.

    Attributes:
        ad_duration_millis (int):
            Output only. The duration of the Video in
            milliseconds.

            This field is a member of `oneof`_ ``_ad_duration_millis``.
        youtube_video_id (str):
            Immutable. The YouTube video ID (as seen in
            YouTube URLs). Adding prefix
            "https://www.youtube.com/watch?v=" to this ID
            will get the YouTube streaming URL for this
            video.

            This field is a member of `oneof`_ ``_youtube_video_id``.
        advertising_id_code (str):
            Output only. The Advertising Digital
            Identification code for this video, as defined
            by the American Association of Advertising
            Agencies, used mainly for television
            commercials.

            This field is a member of `oneof`_ ``_advertising_id_code``.
        isci_code (str):
            Output only. The Industry Standard Commercial
            Identifier code for this video, used mainly for
            television commercials.

            This field is a member of `oneof`_ ``_isci_code``.
    """

    ad_duration_millis = proto.Field(proto.INT64, number=5, optional=True,)
    youtube_video_id = proto.Field(proto.STRING, number=6, optional=True,)
    advertising_id_code = proto.Field(proto.STRING, number=7, optional=True,)
    isci_code = proto.Field(proto.STRING, number=8, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
