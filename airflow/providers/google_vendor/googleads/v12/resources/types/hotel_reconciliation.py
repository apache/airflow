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

from airflow.providers.google_vendor.googleads.v12.enums.types import hotel_reconciliation_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"HotelReconciliation",},
)


class HotelReconciliation(proto.Message):
    r"""A hotel reconciliation. It contains conversion information
    from Hotel bookings to reconcile with advertiser records. These
    rows may be updated or canceled before billing through Bulk
    Uploads.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the hotel reconciliation.
            Hotel reconciliation resource names have the form:

            ``customers/{customer_id}/hotelReconciliations/{commission_id}``
        commission_id (str):
            Required. Output only. The commission ID is
            Google's ID for this booking. Every booking
            event is assigned a Commission ID to help you
            match it to a guest stay.
        order_id (str):
            Output only. The order ID is the identifier for this booking
            as provided in the 'transaction_id' parameter of the
            conversion tracking tag.
        campaign (str):
            Output only. The resource name for the
            Campaign associated with the conversion.
        hotel_center_id (int):
            Output only. Identifier for the Hotel Center
            account which provides the rates for the Hotel
            campaign.
        hotel_id (str):
            Output only. Unique identifier for the booked
            property, as provided in the Hotel Center feed.
            The hotel ID comes from the 'ID' parameter of
            the conversion tracking tag.
        check_in_date (str):
            Output only. Check-in date recorded when the
            booking is made. If the check-in date is
            modified at reconciliation, the revised date
            will then take the place of the original date in
            this column. Format is YYYY-MM-DD.
        check_out_date (str):
            Output only. Check-out date recorded when the
            booking is made. If the check-in date is
            modified at reconciliation, the revised date
            will then take the place of the original date in
            this column. Format is YYYY-MM-DD.
        reconciled_value_micros (int):
            Required. Output only. Reconciled value is
            the final value of a booking as paid by the
            guest. If original booking value changes for any
            reason, such as itinerary changes or room
            upsells, the reconciled value should be the full
            final amount collected. If a booking is
            canceled, the reconciled value should include
            the value of any cancellation fees or
            non-refundable nights charged. Value is in
            millionths of the base unit currency. For
            example, $12.35 would be represented as
            12350000. Currency unit is in the default
            customer currency.
        billed (bool):
            Output only. Whether a given booking has been
            billed. Once billed, a booking can't be
            modified.
        status (google.ads.googleads.v12.enums.types.HotelReconciliationStatusEnum.HotelReconciliationStatus):
            Required. Output only. Current status of a
            booking with regards to reconciliation and
            billing. Bookings should be reconciled within 45
            days after the check-out date. Any booking not
            reconciled within 45 days will be billed at its
            original value.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    commission_id = proto.Field(proto.STRING, number=2,)
    order_id = proto.Field(proto.STRING, number=3,)
    campaign = proto.Field(proto.STRING, number=11,)
    hotel_center_id = proto.Field(proto.INT64, number=4,)
    hotel_id = proto.Field(proto.STRING, number=5,)
    check_in_date = proto.Field(proto.STRING, number=6,)
    check_out_date = proto.Field(proto.STRING, number=7,)
    reconciled_value_micros = proto.Field(proto.INT64, number=8,)
    billed = proto.Field(proto.BOOL, number=9,)
    status = proto.Field(
        proto.ENUM,
        number=10,
        enum=hotel_reconciliation_status.HotelReconciliationStatusEnum.HotelReconciliationStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
