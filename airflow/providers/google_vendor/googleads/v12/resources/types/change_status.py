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

from airflow.providers.google_vendor.googleads.v12.enums.types import change_status_operation
from airflow.providers.google_vendor.googleads.v12.enums.types import change_status_resource_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"ChangeStatus",},
)


class ChangeStatus(proto.Message):
    r"""Describes the status of returned resource. ChangeStatus could
    have up to 3 minutes delay to reflect a new change.

    Attributes:
        resource_name (str):
            Output only. The resource name of the change status. Change
            status resource names have the form:

            ``customers/{customer_id}/changeStatus/{change_status_id}``
        last_change_date_time (str):
            Output only. Time at which the most recent
            change has occurred on this resource.

            This field is a member of `oneof`_ ``_last_change_date_time``.
        resource_type (google.ads.googleads.v12.enums.types.ChangeStatusResourceTypeEnum.ChangeStatusResourceType):
            Output only. Represents the type of the changed resource.
            This dictates what fields will be set. For example, for
            AD_GROUP, campaign and ad_group fields will be set.
        campaign (str):
            Output only. The Campaign affected by this
            change.

            This field is a member of `oneof`_ ``_campaign``.
        ad_group (str):
            Output only. The AdGroup affected by this
            change.

            This field is a member of `oneof`_ ``_ad_group``.
        resource_status (google.ads.googleads.v12.enums.types.ChangeStatusOperationEnum.ChangeStatusOperation):
            Output only. Represents the status of the
            changed resource.
        ad_group_ad (str):
            Output only. The AdGroupAd affected by this
            change.

            This field is a member of `oneof`_ ``_ad_group_ad``.
        ad_group_criterion (str):
            Output only. The AdGroupCriterion affected by
            this change.

            This field is a member of `oneof`_ ``_ad_group_criterion``.
        campaign_criterion (str):
            Output only. The CampaignCriterion affected
            by this change.

            This field is a member of `oneof`_ ``_campaign_criterion``.
        feed (str):
            Output only. The Feed affected by this
            change.

            This field is a member of `oneof`_ ``_feed``.
        feed_item (str):
            Output only. The FeedItem affected by this
            change.

            This field is a member of `oneof`_ ``_feed_item``.
        ad_group_feed (str):
            Output only. The AdGroupFeed affected by this
            change.

            This field is a member of `oneof`_ ``_ad_group_feed``.
        campaign_feed (str):
            Output only. The CampaignFeed affected by
            this change.

            This field is a member of `oneof`_ ``_campaign_feed``.
        ad_group_bid_modifier (str):
            Output only. The AdGroupBidModifier affected
            by this change.

            This field is a member of `oneof`_ ``_ad_group_bid_modifier``.
        shared_set (str):
            Output only. The SharedSet affected by this
            change.
        campaign_shared_set (str):
            Output only. The CampaignSharedSet affected
            by this change.
        asset (str):
            Output only. The Asset affected by this
            change.
        customer_asset (str):
            Output only. The CustomerAsset affected by
            this change.
        campaign_asset (str):
            Output only. The CampaignAsset affected by
            this change.
        ad_group_asset (str):
            Output only. The AdGroupAsset affected by
            this change.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    last_change_date_time = proto.Field(proto.STRING, number=24, optional=True,)
    resource_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=change_status_resource_type.ChangeStatusResourceTypeEnum.ChangeStatusResourceType,
    )
    campaign = proto.Field(proto.STRING, number=17, optional=True,)
    ad_group = proto.Field(proto.STRING, number=18, optional=True,)
    resource_status = proto.Field(
        proto.ENUM,
        number=8,
        enum=change_status_operation.ChangeStatusOperationEnum.ChangeStatusOperation,
    )
    ad_group_ad = proto.Field(proto.STRING, number=25, optional=True,)
    ad_group_criterion = proto.Field(proto.STRING, number=26, optional=True,)
    campaign_criterion = proto.Field(proto.STRING, number=27, optional=True,)
    feed = proto.Field(proto.STRING, number=28, optional=True,)
    feed_item = proto.Field(proto.STRING, number=29, optional=True,)
    ad_group_feed = proto.Field(proto.STRING, number=30, optional=True,)
    campaign_feed = proto.Field(proto.STRING, number=31, optional=True,)
    ad_group_bid_modifier = proto.Field(proto.STRING, number=32, optional=True,)
    shared_set = proto.Field(proto.STRING, number=33,)
    campaign_shared_set = proto.Field(proto.STRING, number=34,)
    asset = proto.Field(proto.STRING, number=35,)
    customer_asset = proto.Field(proto.STRING, number=36,)
    campaign_asset = proto.Field(proto.STRING, number=37,)
    ad_group_asset = proto.Field(proto.STRING, number=38,)


__all__ = tuple(sorted(__protobuf__.manifest))
