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
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_item_target_device
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_item_target_status
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    feed_item_target_type as gage_feed_item_target_type,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"FeedItemTarget",},
)


class FeedItemTarget(proto.Message):
    r"""A feed item target.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the feed item target. Feed
            item target resource names have the form:
            ``customers/{customer_id}/feedItemTargets/{feed_id}~{feed_item_id}~{feed_item_target_type}~{feed_item_target_id}``
        feed_item (str):
            Immutable. The feed item to which this feed
            item target belongs.

            This field is a member of `oneof`_ ``_feed_item``.
        feed_item_target_type (google.ads.googleads.v12.enums.types.FeedItemTargetTypeEnum.FeedItemTargetType):
            Output only. The target type of this feed
            item target. This field is read-only.
        feed_item_target_id (int):
            Output only. The ID of the targeted resource.
            This field is read-only.

            This field is a member of `oneof`_ ``_feed_item_target_id``.
        status (google.ads.googleads.v12.enums.types.FeedItemTargetStatusEnum.FeedItemTargetStatus):
            Output only. Status of the feed item target.
            This field is read-only.
        campaign (str):
            Immutable. The targeted campaign.

            This field is a member of `oneof`_ ``target``.
        ad_group (str):
            Immutable. The targeted ad group.

            This field is a member of `oneof`_ ``target``.
        keyword (google.ads.googleads.v12.common.types.KeywordInfo):
            Immutable. The targeted keyword.

            This field is a member of `oneof`_ ``target``.
        geo_target_constant (str):
            Immutable. The targeted geo target constant
            resource name.

            This field is a member of `oneof`_ ``target``.
        device (google.ads.googleads.v12.enums.types.FeedItemTargetDeviceEnum.FeedItemTargetDevice):
            Immutable. The targeted device.

            This field is a member of `oneof`_ ``target``.
        ad_schedule (google.ads.googleads.v12.common.types.AdScheduleInfo):
            Immutable. The targeted schedule.

            This field is a member of `oneof`_ ``target``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    feed_item = proto.Field(proto.STRING, number=12, optional=True,)
    feed_item_target_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_feed_item_target_type.FeedItemTargetTypeEnum.FeedItemTargetType,
    )
    feed_item_target_id = proto.Field(proto.INT64, number=13, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=11,
        enum=feed_item_target_status.FeedItemTargetStatusEnum.FeedItemTargetStatus,
    )
    campaign = proto.Field(proto.STRING, number=14, oneof="target",)
    ad_group = proto.Field(proto.STRING, number=15, oneof="target",)
    keyword = proto.Field(
        proto.MESSAGE, number=7, oneof="target", message=criteria.KeywordInfo,
    )
    geo_target_constant = proto.Field(proto.STRING, number=16, oneof="target",)
    device = proto.Field(
        proto.ENUM,
        number=9,
        oneof="target",
        enum=feed_item_target_device.FeedItemTargetDeviceEnum.FeedItemTargetDevice,
    )
    ad_schedule = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="target",
        message=criteria.AdScheduleInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
