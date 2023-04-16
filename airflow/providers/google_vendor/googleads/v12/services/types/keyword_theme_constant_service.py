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

from airflow.providers.google_vendor.googleads.v12.resources.types import keyword_theme_constant


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "SuggestKeywordThemeConstantsRequest",
        "SuggestKeywordThemeConstantsResponse",
    },
)


class SuggestKeywordThemeConstantsRequest(proto.Message):
    r"""Request message for
    [KeywordThemeConstantService.SuggestKeywordThemeConstants][google.ads.googleads.v12.services.KeywordThemeConstantService.SuggestKeywordThemeConstants].

    Attributes:
        query_text (str):
            The query text of a keyword theme that will
            be used to map to similar keyword themes. For
            example, "plumber" or "roofer".
        country_code (str):
            Upper-case, two-letter country code as
            defined by ISO-3166. This for refining the scope
            of the query, default to 'US' if not set.
        language_code (str):
            The two letter language code for get
            corresponding keyword theme for refining the
            scope of the query, default to 'en' if not set.
    """

    query_text = proto.Field(proto.STRING, number=1,)
    country_code = proto.Field(proto.STRING, number=2,)
    language_code = proto.Field(proto.STRING, number=3,)


class SuggestKeywordThemeConstantsResponse(proto.Message):
    r"""Response message for
    [KeywordThemeConstantService.SuggestKeywordThemeConstants][google.ads.googleads.v12.services.KeywordThemeConstantService.SuggestKeywordThemeConstants].

    Attributes:
        keyword_theme_constants (Sequence[google.ads.googleads.v12.resources.types.KeywordThemeConstant]):
            Smart Campaign keyword theme suggestions.
    """

    keyword_theme_constants = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message=keyword_theme_constant.KeywordThemeConstant,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
