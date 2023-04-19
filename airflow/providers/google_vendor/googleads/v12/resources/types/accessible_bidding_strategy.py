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

from airflow.providers.google_vendor.googleads.v12.enums.types import bidding_strategy_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    target_impression_share_location,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AccessibleBiddingStrategy",},
)


class AccessibleBiddingStrategy(proto.Message):
    r"""Represents a view of BiddingStrategies owned by and shared
    with the customer.
    In contrast to BiddingStrategy, this resource includes
    strategies owned by managers of the customer and shared with
    this customer - in addition to strategies owned by this
    customer. This resource does not provide metrics and only
    exposes a limited subset of the BiddingStrategy attributes.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the accessible bidding
            strategy. AccessibleBiddingStrategy resource names have the
            form:

            ``customers/{customer_id}/accessibleBiddingStrategies/{bidding_strategy_id}``
        id (int):
            Output only. The ID of the bidding strategy.
        name (str):
            Output only. The name of the bidding
            strategy.
        type_ (google.ads.googleads.v12.enums.types.BiddingStrategyTypeEnum.BiddingStrategyType):
            Output only. The type of the bidding
            strategy.
        owner_customer_id (int):
            Output only. The ID of the Customer which
            owns the bidding strategy.
        owner_descriptive_name (str):
            Output only. descriptive_name of the Customer which owns the
            bidding strategy.
        maximize_conversion_value (google.ads.googleads.v12.resources.types.AccessibleBiddingStrategy.MaximizeConversionValue):
            Output only. An automated bidding strategy to
            help get the most conversion value for your
            campaigns while spending your budget.

            This field is a member of `oneof`_ ``scheme``.
        maximize_conversions (google.ads.googleads.v12.resources.types.AccessibleBiddingStrategy.MaximizeConversions):
            Output only. An automated bidding strategy to
            help get the most conversions for your campaigns
            while spending your budget.

            This field is a member of `oneof`_ ``scheme``.
        target_cpa (google.ads.googleads.v12.resources.types.AccessibleBiddingStrategy.TargetCpa):
            Output only. A bidding strategy that sets
            bids to help get as many conversions as possible
            at the target cost-per-acquisition (CPA) you
            set.

            This field is a member of `oneof`_ ``scheme``.
        target_impression_share (google.ads.googleads.v12.resources.types.AccessibleBiddingStrategy.TargetImpressionShare):
            Output only. A bidding strategy that
            automatically optimizes towards a chosen
            percentage of impressions.

            This field is a member of `oneof`_ ``scheme``.
        target_roas (google.ads.googleads.v12.resources.types.AccessibleBiddingStrategy.TargetRoas):
            Output only. A bidding strategy that helps
            you maximize revenue while averaging a specific
            target Return On Ad Spend (ROAS).

            This field is a member of `oneof`_ ``scheme``.
        target_spend (google.ads.googleads.v12.resources.types.AccessibleBiddingStrategy.TargetSpend):
            Output only. A bid strategy that sets your
            bids to help get as many clicks as possible
            within your budget.

            This field is a member of `oneof`_ ``scheme``.
    """

    class MaximizeConversionValue(proto.Message):
        r"""An automated bidding strategy to help get the most conversion
        value for your campaigns while spending your budget.

        Attributes:
            target_roas (float):
                Output only. The target return on ad spend
                (ROAS) option. If set, the bid strategy will
                maximize revenue while averaging the target
                return on ad spend. If the target ROAS is high,
                the bid strategy may not be able to spend the
                full budget. If the target ROAS is not set, the
                bid strategy will aim to achieve the highest
                possible ROAS for the budget.
        """

        target_roas = proto.Field(proto.DOUBLE, number=1,)

    class MaximizeConversions(proto.Message):
        r"""An automated bidding strategy to help get the most
        conversions for your campaigns while spending your budget.

        Attributes:
            target_cpa_micros (int):
                Output only. The target cost per acquisition
                (CPA) option. This is the average amount that
                you would like to spend per acquisition.
        """

        target_cpa_micros = proto.Field(proto.INT64, number=2,)

    class TargetCpa(proto.Message):
        r"""An automated bid strategy that sets bids to help get as many
        conversions as possible at the target cost-per-acquisition (CPA)
        you set.

        Attributes:
            target_cpa_micros (int):
                Output only. Average CPA target.
                This target should be greater than or equal to
                minimum billable unit based on the currency for
                the account.

                This field is a member of `oneof`_ ``_target_cpa_micros``.
        """

        target_cpa_micros = proto.Field(proto.INT64, number=1, optional=True,)

    class TargetImpressionShare(proto.Message):
        r"""An automated bidding strategy that sets bids so that a
        certain percentage of search ads are shown at the top of the
        first page (or other targeted location).

        Attributes:
            location (google.ads.googleads.v12.enums.types.TargetImpressionShareLocationEnum.TargetImpressionShareLocation):
                Output only. The targeted location on the
                search results page.
            location_fraction_micros (int):
                The chosen fraction of ads to be shown in the
                targeted location in micros. For example, 1%
                equals 10,000.

                This field is a member of `oneof`_ ``_location_fraction_micros``.
            cpc_bid_ceiling_micros (int):
                Output only. The highest CPC bid the
                automated bidding system is permitted to
                specify. This is a required field entered by the
                advertiser that sets the ceiling and specified
                in local micros.

                This field is a member of `oneof`_ ``_cpc_bid_ceiling_micros``.
        """

        location = proto.Field(
            proto.ENUM,
            number=1,
            enum=target_impression_share_location.TargetImpressionShareLocationEnum.TargetImpressionShareLocation,
        )
        location_fraction_micros = proto.Field(
            proto.INT64, number=2, optional=True,
        )
        cpc_bid_ceiling_micros = proto.Field(
            proto.INT64, number=3, optional=True,
        )

    class TargetRoas(proto.Message):
        r"""An automated bidding strategy that helps you maximize revenue
        while averaging a specific target return on ad spend (ROAS).

        Attributes:
            target_roas (float):
                Output only. The chosen revenue (based on
                conversion data) per unit of spend.

                This field is a member of `oneof`_ ``_target_roas``.
        """

        target_roas = proto.Field(proto.DOUBLE, number=1, optional=True,)

    class TargetSpend(proto.Message):
        r"""An automated bid strategy that sets your bids to help get as
        many clicks as possible within your budget.

        Attributes:
            target_spend_micros (int):
                Output only. The spend target under which to
                maximize clicks. A TargetSpend bidder will
                attempt to spend the smaller of this value or
                the natural throttling spend amount.
                If not specified, the budget is used as the
                spend target. This field is deprecated and
                should no longer be used. See
                https://ads-developers.googleblog.com/2020/05/reminder-about-sunset-creation-of.html
                for details.

                This field is a member of `oneof`_ ``_target_spend_micros``.
            cpc_bid_ceiling_micros (int):
                Output only. Maximum bid limit that can be
                set by the bid strategy. The limit applies to
                all keywords managed by the strategy.

                This field is a member of `oneof`_ ``_cpc_bid_ceiling_micros``.
        """

        target_spend_micros = proto.Field(proto.INT64, number=1, optional=True,)
        cpc_bid_ceiling_micros = proto.Field(
            proto.INT64, number=2, optional=True,
        )

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    name = proto.Field(proto.STRING, number=3,)
    type_ = proto.Field(
        proto.ENUM,
        number=4,
        enum=bidding_strategy_type.BiddingStrategyTypeEnum.BiddingStrategyType,
    )
    owner_customer_id = proto.Field(proto.INT64, number=5,)
    owner_descriptive_name = proto.Field(proto.STRING, number=6,)
    maximize_conversion_value = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="scheme",
        message=MaximizeConversionValue,
    )
    maximize_conversions = proto.Field(
        proto.MESSAGE, number=8, oneof="scheme", message=MaximizeConversions,
    )
    target_cpa = proto.Field(
        proto.MESSAGE, number=9, oneof="scheme", message=TargetCpa,
    )
    target_impression_share = proto.Field(
        proto.MESSAGE, number=10, oneof="scheme", message=TargetImpressionShare,
    )
    target_roas = proto.Field(
        proto.MESSAGE, number=11, oneof="scheme", message=TargetRoas,
    )
    target_spend = proto.Field(
        proto.MESSAGE, number=12, oneof="scheme", message=TargetSpend,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
