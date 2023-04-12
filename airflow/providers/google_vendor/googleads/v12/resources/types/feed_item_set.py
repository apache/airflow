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

from airflow.providers.google_vendor.googleads.v12.common.types import (
    feed_item_set_filter_type_infos,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import feed_item_set_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"FeedItemSet",},
)


class FeedItemSet(proto.Message):
    r"""Represents a set of feed items. The set can be used and
    shared among certain feed item features. For instance, the set
    can be referenced within the matching functions of CustomerFeed,
    CampaignFeed, and AdGroupFeed.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the feed item set. Feed item
            set resource names have the form:
            ``customers/{customer_id}/feedItemSets/{feed_id}~{feed_item_set_id}``
        feed (str):
            Immutable. The resource name of the feed
            containing the feed items in the set. Immutable.
            Required.
        feed_item_set_id (int):
            Output only. ID of the set.
        display_name (str):
            Name of the set. Must be unique within the
            account.
        status (google.ads.googleads.v12.enums.types.FeedItemSetStatusEnum.FeedItemSetStatus):
            Output only. Status of the feed item set.
            This field is read-only.
        dynamic_location_set_filter (google.ads.googleads.v12.common.types.DynamicLocationSetFilter):
            Filter for dynamic location set.
            It is only used for sets of locations.

            This field is a member of `oneof`_ ``dynamic_set_filter``.
        dynamic_affiliate_location_set_filter (google.ads.googleads.v12.common.types.DynamicAffiliateLocationSetFilter):
            Filter for dynamic affiliate location set.
            This field doesn't apply generally to feed item
            sets. It is only used for sets of affiliate
            locations.

            This field is a member of `oneof`_ ``dynamic_set_filter``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    feed = proto.Field(proto.STRING, number=2,)
    feed_item_set_id = proto.Field(proto.INT64, number=3,)
    display_name = proto.Field(proto.STRING, number=4,)
    status = proto.Field(
        proto.ENUM,
        number=8,
        enum=feed_item_set_status.FeedItemSetStatusEnum.FeedItemSetStatus,
    )
    dynamic_location_set_filter = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="dynamic_set_filter",
        message=feed_item_set_filter_type_infos.DynamicLocationSetFilter,
    )
    dynamic_affiliate_location_set_filter = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="dynamic_set_filter",
        message=feed_item_set_filter_type_infos.DynamicAffiliateLocationSetFilter,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
