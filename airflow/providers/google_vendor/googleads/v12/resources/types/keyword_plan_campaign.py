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

from airflow.providers.google_vendor.googleads.v12.enums.types import (
    keyword_plan_network as gage_keyword_plan_network,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"KeywordPlanCampaign", "KeywordPlanGeoTarget",},
)


class KeywordPlanCampaign(proto.Message):
    r"""A Keyword Plan campaign.
    Max number of keyword plan campaigns per plan allowed: 1.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the Keyword Plan campaign.
            KeywordPlanCampaign resource names have the form:

            ``customers/{customer_id}/keywordPlanCampaigns/{kp_campaign_id}``
        keyword_plan (str):
            The keyword plan this campaign belongs to.

            This field is a member of `oneof`_ ``_keyword_plan``.
        id (int):
            Output only. The ID of the Keyword Plan
            campaign.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the Keyword Plan campaign.
            This field is required and should not be empty
            when creating Keyword Plan campaigns.

            This field is a member of `oneof`_ ``_name``.
        language_constants (Sequence[str]):
            The languages targeted for the Keyword Plan
            campaign. Max allowed: 1.
        keyword_plan_network (google.ads.googleads.v12.enums.types.KeywordPlanNetworkEnum.KeywordPlanNetwork):
            Targeting network.
            This field is required and should not be empty
            when creating Keyword Plan campaigns.
        cpc_bid_micros (int):
            A default max cpc bid in micros, and in the
            account currency, for all ad groups under the
            campaign.
            This field is required and should not be empty
            when creating Keyword Plan campaigns.

            This field is a member of `oneof`_ ``_cpc_bid_micros``.
        geo_targets (Sequence[google.ads.googleads.v12.resources.types.KeywordPlanGeoTarget]):
            The geo targets.
            Max number allowed: 20.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    keyword_plan = proto.Field(proto.STRING, number=9, optional=True,)
    id = proto.Field(proto.INT64, number=10, optional=True,)
    name = proto.Field(proto.STRING, number=11, optional=True,)
    language_constants = proto.RepeatedField(proto.STRING, number=12,)
    keyword_plan_network = proto.Field(
        proto.ENUM,
        number=6,
        enum=gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork,
    )
    cpc_bid_micros = proto.Field(proto.INT64, number=13, optional=True,)
    geo_targets = proto.RepeatedField(
        proto.MESSAGE, number=8, message="KeywordPlanGeoTarget",
    )


class KeywordPlanGeoTarget(proto.Message):
    r"""A geo target.

    Attributes:
        geo_target_constant (str):
            Required. The resource name of the geo
            target.

            This field is a member of `oneof`_ ``_geo_target_constant``.
    """

    geo_target_constant = proto.Field(proto.STRING, number=2, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
