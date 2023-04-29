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

from airflow.providers.google_vendor.googleads.v12.enums.types import keyword_match_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"KeywordPlanAdGroupKeyword",},
)


class KeywordPlanAdGroupKeyword(proto.Message):
    r"""A Keyword Plan ad group keyword.
    Max number of keyword plan keywords per plan: 10000.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the Keyword Plan ad group
            keyword. KeywordPlanAdGroupKeyword resource names have the
            form:

            ``customers/{customer_id}/keywordPlanAdGroupKeywords/{kp_ad_group_keyword_id}``
        keyword_plan_ad_group (str):
            The Keyword Plan ad group to which this
            keyword belongs.

            This field is a member of `oneof`_ ``_keyword_plan_ad_group``.
        id (int):
            Output only. The ID of the Keyword Plan
            keyword.

            This field is a member of `oneof`_ ``_id``.
        text (str):
            The keyword text.

            This field is a member of `oneof`_ ``_text``.
        match_type (google.ads.googleads.v12.enums.types.KeywordMatchTypeEnum.KeywordMatchType):
            The keyword match type.
        cpc_bid_micros (int):
            A keyword level max cpc bid in micros (for
            example, $1 = 1mm). The currency is the same as
            the account currency code. This will override
            any CPC bid set at the keyword plan ad group
            level. Not applicable for negative keywords.
            (negative = true) This field is Optional.

            This field is a member of `oneof`_ ``_cpc_bid_micros``.
        negative (bool):
            Immutable. If true, the keyword is negative.

            This field is a member of `oneof`_ ``_negative``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    keyword_plan_ad_group = proto.Field(proto.STRING, number=8, optional=True,)
    id = proto.Field(proto.INT64, number=9, optional=True,)
    text = proto.Field(proto.STRING, number=10, optional=True,)
    match_type = proto.Field(
        proto.ENUM,
        number=5,
        enum=keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType,
    )
    cpc_bid_micros = proto.Field(proto.INT64, number=11, optional=True,)
    negative = proto.Field(proto.BOOL, number=12, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
