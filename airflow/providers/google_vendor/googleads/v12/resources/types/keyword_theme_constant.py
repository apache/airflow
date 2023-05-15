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
    manifest={"KeywordThemeConstant",},
)


class KeywordThemeConstant(proto.Message):
    r"""A Smart Campaign keyword theme constant.

    Attributes:
        resource_name (str):
            Output only. The resource name of the keyword theme
            constant. Keyword theme constant resource names have the
            form:

            ``keywordThemeConstants/{keyword_theme_id}~{sub_keyword_theme_id}``
        country_code (str):
            Output only. The ISO-3166 Alpha-2 country
            code of the constant, eg. "US". To display and
            query matching purpose, the keyword theme needs
            to be localized.

            This field is a member of `oneof`_ ``_country_code``.
        language_code (str):
            Output only. The ISO-639-1 language code with
            2 letters of the constant, eg. "en". To display
            and query matching purpose, the keyword theme
            needs to be localized.

            This field is a member of `oneof`_ ``_language_code``.
        display_name (str):
            Output only. The display name of the keyword
            theme or sub keyword theme.

            This field is a member of `oneof`_ ``_display_name``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    country_code = proto.Field(proto.STRING, number=2, optional=True,)
    language_code = proto.Field(proto.STRING, number=3, optional=True,)
    display_name = proto.Field(proto.STRING, number=4, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
