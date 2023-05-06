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

from airflow.providers.google_vendor.googleads.v12.enums.types import advertising_channel_type
from airflow.providers.google_vendor.googleads.v12.enums.types import device
from airflow.providers.google_vendor.googleads.v12.enums.types import seasonality_event_scope
from airflow.providers.google_vendor.googleads.v12.enums.types import seasonality_event_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"BiddingSeasonalityAdjustment",},
)


class BiddingSeasonalityAdjustment(proto.Message):
    r"""Represents a bidding seasonality adjustment.
    See "About seasonality adjustments" at
    https://support.google.com/google-ads/answer/10369906.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the seasonality adjustment.
            Seasonality adjustment resource names have the form:

            ``customers/{customer_id}/biddingSeasonalityAdjustments/{seasonality_adjustment_id}``
        seasonality_adjustment_id (int):
            Output only. The ID of the seasonality
            adjustment.
        scope (google.ads.googleads.v12.enums.types.SeasonalityEventScopeEnum.SeasonalityEventScope):
            The scope of the seasonality adjustment.
        status (google.ads.googleads.v12.enums.types.SeasonalityEventStatusEnum.SeasonalityEventStatus):
            Output only. The status of the seasonality
            adjustment.
        start_date_time (str):
            Required. The inclusive start time of the
            seasonality adjustment in yyyy-MM-dd HH:mm:ss
            format.
            A seasonality adjustment is forward looking and
            should be used for events that start and end in
            the future.
        end_date_time (str):
            Required. The exclusive end time of the seasonality
            adjustment in yyyy-MM-dd HH:mm:ss format.

            The length of [start_date_time, end_date_time) interval must
            be within (0, 14 days].
        name (str):
            The name of the seasonality adjustment. The
            name can be at most 255 characters.
        description (str):
            The description of the seasonality
            adjustment. The description can be at most 2048
            characters.
        devices (Sequence[google.ads.googleads.v12.enums.types.DeviceEnum.Device]):
            If not specified, all devices will be
            included in this adjustment. Otherwise, only the
            specified targeted devices will be included in
            this adjustment.
        conversion_rate_modifier (float):
            Conversion rate modifier estimated based on
            expected conversion rate changes. When this
            field is unset or set to 1.0 no adjustment will
            be applied to traffic. The allowed range is 0.1
            to 10.0.
        campaigns (Sequence[str]):
            The seasonality adjustment will apply to the campaigns
            listed when the scope of this adjustment is CAMPAIGN. The
            maximum number of campaigns per event is 2000. Note: a
            seasonality adjustment with both advertising_channel_types
            and campaign_ids is not supported.
        advertising_channel_types (Sequence[google.ads.googleads.v12.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType]):
            The seasonality adjustment will apply to all the campaigns
            under the listed channels retroactively as well as going
            forward when the scope of this adjustment is CHANNEL. The
            supported advertising channel types are DISPLAY, SEARCH and
            SHOPPING. Note: a seasonality adjustment with both
            advertising_channel_types and campaign_ids is not supported.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    seasonality_adjustment_id = proto.Field(proto.INT64, number=2,)
    scope = proto.Field(
        proto.ENUM,
        number=3,
        enum=seasonality_event_scope.SeasonalityEventScopeEnum.SeasonalityEventScope,
    )
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=seasonality_event_status.SeasonalityEventStatusEnum.SeasonalityEventStatus,
    )
    start_date_time = proto.Field(proto.STRING, number=5,)
    end_date_time = proto.Field(proto.STRING, number=6,)
    name = proto.Field(proto.STRING, number=7,)
    description = proto.Field(proto.STRING, number=8,)
    devices = proto.RepeatedField(
        proto.ENUM, number=9, enum=device.DeviceEnum.Device,
    )
    conversion_rate_modifier = proto.Field(proto.DOUBLE, number=10,)
    campaigns = proto.RepeatedField(proto.STRING, number=11,)
    advertising_channel_types = proto.RepeatedField(
        proto.ENUM,
        number=12,
        enum=advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
