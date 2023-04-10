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
    manifest={"KeywordPlanAdGroupKeywordErrorEnum",},
)


class KeywordPlanAdGroupKeywordErrorEnum(proto.Message):
    r"""Container for enum describing possible errors from applying
    an ad group keyword or a campaign keyword from a keyword plan.

    """

    class KeywordPlanAdGroupKeywordError(proto.Enum):
        r"""Enum describing possible errors from applying a keyword plan
        ad group keyword or keyword plan campaign keyword.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        INVALID_KEYWORD_MATCH_TYPE = 2
        DUPLICATE_KEYWORD = 3
        KEYWORD_TEXT_TOO_LONG = 4
        KEYWORD_HAS_INVALID_CHARS = 5
        KEYWORD_HAS_TOO_MANY_WORDS = 6
        INVALID_KEYWORD_TEXT = 7
        NEGATIVE_KEYWORD_HAS_CPC_BID = 8
        NEW_BMM_KEYWORDS_NOT_ALLOWED = 9


__all__ = tuple(sorted(__protobuf__.manifest))
