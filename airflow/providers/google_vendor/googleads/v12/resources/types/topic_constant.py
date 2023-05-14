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
    manifest={"TopicConstant",},
)


class TopicConstant(proto.Message):
    r"""Use topics to target or exclude placements in the Google
    Display Network based on the category into which the placement
    falls (for example, "Pets & Animals/Pets/Dogs").

    Attributes:
        resource_name (str):
            Output only. The resource name of the topic constant. topic
            constant resource names have the form:

            ``topicConstants/{topic_id}``
        id (int):
            Output only. The ID of the topic.

            This field is a member of `oneof`_ ``_id``.
        topic_constant_parent (str):
            Output only. Resource name of parent of the
            topic constant.

            This field is a member of `oneof`_ ``_topic_constant_parent``.
        path (Sequence[str]):
            Output only. The category to target or
            exclude. Each subsequent element in the array
            describes a more specific sub-category. For
            example, {"Pets & Animals", "Pets", "Dogs"}
            represents the "Pets & Animals/Pets/Dogs"
            category. List of available topic categories at
            https://developers.google.com/adwords/api/docs/appendix/verticals
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=5, optional=True,)
    topic_constant_parent = proto.Field(proto.STRING, number=6, optional=True,)
    path = proto.RepeatedField(proto.STRING, number=7,)


__all__ = tuple(sorted(__protobuf__.manifest))
