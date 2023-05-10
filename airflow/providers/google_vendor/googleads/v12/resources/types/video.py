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
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"Video",},
)


class Video(proto.Message):
    r"""A video.

    Attributes:
        resource_name (str):
            Output only. The resource name of the video. Video resource
            names have the form:

            ``customers/{customer_id}/videos/{video_id}``
        id (str):
            Output only. The ID of the video.

            This field is a member of `oneof`_ ``_id``.
        channel_id (str):
            Output only. The owner channel id of the
            video.

            This field is a member of `oneof`_ ``_channel_id``.
        duration_millis (int):
            Output only. The duration of the video in
            milliseconds.

            This field is a member of `oneof`_ ``_duration_millis``.
        title (str):
            Output only. The title of the video.

            This field is a member of `oneof`_ ``_title``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.STRING, number=6, optional=True,)
    channel_id = proto.Field(proto.STRING, number=7, optional=True,)
    duration_millis = proto.Field(proto.INT64, number=8, optional=True,)
    title = proto.Field(proto.STRING, number=9, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
