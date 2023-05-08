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
    manifest={"LanguageConstant",},
)


class LanguageConstant(proto.Message):
    r"""A language.

    Attributes:
        resource_name (str):
            Output only. The resource name of the language constant.
            Language constant resource names have the form:

            ``languageConstants/{criterion_id}``
        id (int):
            Output only. The ID of the language constant.

            This field is a member of `oneof`_ ``_id``.
        code (str):
            Output only. The language code, for example, "en_US",
            "en_AU", "es", "fr", etc.

            This field is a member of `oneof`_ ``_code``.
        name (str):
            Output only. The full name of the language in
            English, for example, "English (US)", "Spanish",
            etc.

            This field is a member of `oneof`_ ``_name``.
        targetable (bool):
            Output only. Whether the language is
            targetable.

            This field is a member of `oneof`_ ``_targetable``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=6, optional=True,)
    code = proto.Field(proto.STRING, number=7, optional=True,)
    name = proto.Field(proto.STRING, number=8, optional=True,)
    targetable = proto.Field(proto.BOOL, number=9, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
