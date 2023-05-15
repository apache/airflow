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
    feed_item_set_string_filter_type,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "DynamicLocationSetFilter",
        "BusinessNameFilter",
        "DynamicAffiliateLocationSetFilter",
    },
)


class DynamicLocationSetFilter(proto.Message):
    r"""Represents a filter on locations in a feed item set.
    Only applicable if the parent Feed of the FeedItemSet is a
    LOCATION feed.

    Attributes:
        labels (Sequence[str]):
            If multiple labels are set, then only
            feeditems marked with all the labels will be
            added to the FeedItemSet.
        business_name_filter (google.ads.googleads.v12.common.types.BusinessNameFilter):
            Business name filter.
    """

    labels = proto.RepeatedField(proto.STRING, number=1,)
    business_name_filter = proto.Field(
        proto.MESSAGE, number=2, message="BusinessNameFilter",
    )


class BusinessNameFilter(proto.Message):
    r"""Represents a business name filter on locations in a
    FeedItemSet.

    Attributes:
        business_name (str):
            Business name string to use for filtering.
        filter_type (google.ads.googleads.v12.enums.types.FeedItemSetStringFilterTypeEnum.FeedItemSetStringFilterType):
            The type of string matching to use when filtering with
            business_name.
    """

    business_name = proto.Field(proto.STRING, number=1,)
    filter_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=feed_item_set_string_filter_type.FeedItemSetStringFilterTypeEnum.FeedItemSetStringFilterType,
    )


class DynamicAffiliateLocationSetFilter(proto.Message):
    r"""Represents a filter on affiliate locations in a FeedItemSet. Only
    applicable if the parent Feed of the FeedItemSet is an
    AFFILIATE_LOCATION feed.

    Attributes:
        chain_ids (Sequence[int]):
            Used to filter affiliate locations by chain
            ids. Only affiliate locations that belong to the
            specified chain(s) will be added to the
            FeedItemSet.
    """

    chain_ids = proto.RepeatedField(proto.INT64, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
