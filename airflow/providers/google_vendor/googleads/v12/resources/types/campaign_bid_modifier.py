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

from airflow.providers.google_vendor.googleads.v12.common.types import criteria


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CampaignBidModifier",},
)


class CampaignBidModifier(proto.Message):
    r"""Represents a bid-modifiable only criterion at the campaign
    level.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign bid modifier.
            Campaign bid modifier resource names have the form:

            ``customers/{customer_id}/campaignBidModifiers/{campaign_id}~{criterion_id}``
        campaign (str):
            Output only. The campaign to which this
            criterion belongs.

            This field is a member of `oneof`_ ``_campaign``.
        criterion_id (int):
            Output only. The ID of the criterion to bid
            modify.
            This field is ignored for mutates.

            This field is a member of `oneof`_ ``_criterion_id``.
        bid_modifier (float):
            The modifier for the bid when the criterion
            matches.

            This field is a member of `oneof`_ ``_bid_modifier``.
        interaction_type (google.ads.googleads.v12.common.types.InteractionTypeInfo):
            Immutable. Criterion for interaction type.
            Only supported for search campaigns.

            This field is a member of `oneof`_ ``criterion``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    campaign = proto.Field(proto.STRING, number=6, optional=True,)
    criterion_id = proto.Field(proto.INT64, number=7, optional=True,)
    bid_modifier = proto.Field(proto.DOUBLE, number=8, optional=True,)
    interaction_type = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="criterion",
        message=criteria.InteractionTypeInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
