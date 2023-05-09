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
    manifest={"KeywordPlanAdGroup",},
)


class KeywordPlanAdGroup(proto.Message):
    r"""A Keyword Planner ad group.
    Max number of keyword plan ad groups per plan: 200.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the Keyword Planner ad
            group. KeywordPlanAdGroup resource names have the form:

            ``customers/{customer_id}/keywordPlanAdGroups/{kp_ad_group_id}``
        keyword_plan_campaign (str):
            The keyword plan campaign to which this ad
            group belongs.

            This field is a member of `oneof`_ ``_keyword_plan_campaign``.
        id (int):
            Output only. The ID of the keyword plan ad
            group.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the keyword plan ad group.
            This field is required and should not be empty
            when creating keyword plan ad group.

            This field is a member of `oneof`_ ``_name``.
        cpc_bid_micros (int):
            A default ad group max cpc bid in micros in
            account currency for all biddable keywords under
            the keyword plan ad group. If not set, will
            inherit from parent campaign.

            This field is a member of `oneof`_ ``_cpc_bid_micros``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    keyword_plan_campaign = proto.Field(proto.STRING, number=6, optional=True,)
    id = proto.Field(proto.INT64, number=7, optional=True,)
    name = proto.Field(proto.STRING, number=8, optional=True,)
    cpc_bid_micros = proto.Field(proto.INT64, number=9, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
