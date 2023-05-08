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
    manifest={"FeedItemSetLink",},
)


class FeedItemSetLink(proto.Message):
    r"""Represents a link between a FeedItem and a FeedItemSet.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the feed item set link. Feed
            item set link resource names have the form:
            ``customers/{customer_id}/feedItemSetLinks/{feed_id}~{feed_item_set_id}~{feed_item_id}``
        feed_item (str):
            Immutable. The linked FeedItem.
        feed_item_set (str):
            Immutable. The linked FeedItemSet.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    feed_item = proto.Field(proto.STRING, number=2,)
    feed_item_set = proto.Field(proto.STRING, number=3,)


__all__ = tuple(sorted(__protobuf__.manifest))
