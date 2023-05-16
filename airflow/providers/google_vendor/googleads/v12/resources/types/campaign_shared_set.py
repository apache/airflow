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

from airflow.providers.google_vendor.googleads.v12.enums.types import campaign_shared_set_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CampaignSharedSet",},
)


class CampaignSharedSet(proto.Message):
    r"""CampaignSharedSets are used for managing the shared sets
    associated with a campaign.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign shared set.
            Campaign shared set resource names have the form:

            ``customers/{customer_id}/campaignSharedSets/{campaign_id}~{shared_set_id}``
        campaign (str):
            Immutable. The campaign to which the campaign
            shared set belongs.

            This field is a member of `oneof`_ ``_campaign``.
        shared_set (str):
            Immutable. The shared set associated with the
            campaign. This may be a negative keyword shared
            set of another customer. This customer should be
            a manager of the other customer, otherwise the
            campaign shared set will exist but have no
            serving effect. Only negative keyword shared
            sets can be associated with Shopping campaigns.
            Only negative placement shared sets can be
            associated with Display mobile app campaigns.

            This field is a member of `oneof`_ ``_shared_set``.
        status (google.ads.googleads.v12.enums.types.CampaignSharedSetStatusEnum.CampaignSharedSetStatus):
            Output only. The status of this campaign
            shared set. Read only.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    campaign = proto.Field(proto.STRING, number=5, optional=True,)
    shared_set = proto.Field(proto.STRING, number=6, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=2,
        enum=campaign_shared_set_status.CampaignSharedSetStatusEnum.CampaignSharedSetStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
