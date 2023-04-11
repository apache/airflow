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
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    bid_modifier_source as gage_bid_modifier_source,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupBidModifier",},
)


class AdGroupBidModifier(proto.Message):
    r"""Represents an ad group bid modifier.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the ad group bid modifier.
            Ad group bid modifier resource names have the form:

            ``customers/{customer_id}/adGroupBidModifiers/{ad_group_id}~{criterion_id}``
        ad_group (str):
            Immutable. The ad group to which this
            criterion belongs.

            This field is a member of `oneof`_ ``_ad_group``.
        criterion_id (int):
            Output only. The ID of the criterion to bid
            modify.
            This field is ignored for mutates.

            This field is a member of `oneof`_ ``_criterion_id``.
        bid_modifier (float):
            The modifier for the bid when the criterion
            matches. The modifier must be in the range: 0.1
            - 10.0. The range is 1.0 - 6.0 for
            PreferredContent. Use 0 to opt out of a Device
            type.

            This field is a member of `oneof`_ ``_bid_modifier``.
        base_ad_group (str):
            Output only. The base ad group from which this draft/trial
            adgroup bid modifier was created. If ad_group is a base ad
            group then this field will be equal to ad_group. If the ad
            group was created in the draft or trial and has no
            corresponding base ad group, then this field will be null.
            This field is readonly.

            This field is a member of `oneof`_ ``_base_ad_group``.
        bid_modifier_source (google.ads.googleads.v12.enums.types.BidModifierSourceEnum.BidModifierSource):
            Output only. Bid modifier source.
        hotel_date_selection_type (google.ads.googleads.v12.common.types.HotelDateSelectionTypeInfo):
            Immutable. Criterion for hotel date selection
            (default dates versus user selected).

            This field is a member of `oneof`_ ``criterion``.
        hotel_advance_booking_window (google.ads.googleads.v12.common.types.HotelAdvanceBookingWindowInfo):
            Immutable. Criterion for number of days prior
            to the stay the booking is being made.

            This field is a member of `oneof`_ ``criterion``.
        hotel_length_of_stay (google.ads.googleads.v12.common.types.HotelLengthOfStayInfo):
            Immutable. Criterion for length of hotel stay
            in nights.

            This field is a member of `oneof`_ ``criterion``.
        hotel_check_in_day (google.ads.googleads.v12.common.types.HotelCheckInDayInfo):
            Immutable. Criterion for day of the week the
            booking is for.

            This field is a member of `oneof`_ ``criterion``.
        device (google.ads.googleads.v12.common.types.DeviceInfo):
            Immutable. A device criterion.

            This field is a member of `oneof`_ ``criterion``.
        preferred_content (google.ads.googleads.v12.common.types.PreferredContentInfo):
            Immutable. A preferred content criterion.

            This field is a member of `oneof`_ ``criterion``.
        hotel_check_in_date_range (google.ads.googleads.v12.common.types.HotelCheckInDateRangeInfo):
            Immutable. Criterion for a hotel check-in
            date range.

            This field is a member of `oneof`_ ``criterion``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group = proto.Field(proto.STRING, number=13, optional=True,)
    criterion_id = proto.Field(proto.INT64, number=14, optional=True,)
    bid_modifier = proto.Field(proto.DOUBLE, number=15, optional=True,)
    base_ad_group = proto.Field(proto.STRING, number=16, optional=True,)
    bid_modifier_source = proto.Field(
        proto.ENUM,
        number=10,
        enum=gage_bid_modifier_source.BidModifierSourceEnum.BidModifierSource,
    )
    hotel_date_selection_type = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="criterion",
        message=criteria.HotelDateSelectionTypeInfo,
    )
    hotel_advance_booking_window = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="criterion",
        message=criteria.HotelAdvanceBookingWindowInfo,
    )
    hotel_length_of_stay = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="criterion",
        message=criteria.HotelLengthOfStayInfo,
    )
    hotel_check_in_day = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="criterion",
        message=criteria.HotelCheckInDayInfo,
    )
    device = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="criterion",
        message=criteria.DeviceInfo,
    )
    preferred_content = proto.Field(
        proto.MESSAGE,
        number=12,
        oneof="criterion",
        message=criteria.PreferredContentInfo,
    )
    hotel_check_in_date_range = proto.Field(
        proto.MESSAGE,
        number=17,
        oneof="criterion",
        message=criteria.HotelCheckInDateRangeInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
