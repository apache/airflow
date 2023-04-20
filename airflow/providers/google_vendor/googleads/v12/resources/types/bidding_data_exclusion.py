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
    manifest={"BiddingDataExclusion",},
)


class BiddingDataExclusion(proto.Message):
    r"""Represents a bidding data exclusion.
    See "About data exclusions" at
    https://support.google.com/google-ads/answer/10370710.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the data exclusion. Data
            exclusion resource names have the form:

            ``customers/{customer_id}/biddingDataExclusions/{data_exclusion_id}``
        data_exclusion_id (int):
            Output only. The ID of the data exclusion.
        scope (google.ads.googleads.v12.enums.types.SeasonalityEventScopeEnum.SeasonalityEventScope):
            The scope of the data exclusion.
        status (google.ads.googleads.v12.enums.types.SeasonalityEventStatusEnum.SeasonalityEventStatus):
            Output only. The status of the data
            exclusion.
        start_date_time (str):
            Required. The inclusive start time of the
            data exclusion in yyyy-MM-dd HH:mm:ss format.
            A data exclusion is backward looking and should
            be used for events that start in the past and
            end either in the past or future.
        end_date_time (str):
            Required. The exclusive end time of the data exclusion in
            yyyy-MM-dd HH:mm:ss format.

            The length of [start_date_time, end_date_time) interval must
            be within (0, 14 days].
        name (str):
            The name of the data exclusion. The name can
            be at most 255 characters.
        description (str):
            The description of the data exclusion. The
            description can be at most 2048 characters.
        devices (Sequence[google.ads.googleads.v12.enums.types.DeviceEnum.Device]):
            If not specified, all devices will be
            included in this exclusion. Otherwise, only the
            specified targeted devices will be included in
            this exclusion.
        campaigns (Sequence[str]):
            The data exclusion will apply to the campaigns listed when
            the scope of this exclusion is CAMPAIGN. The maximum number
            of campaigns per event is 2000. Note: a data exclusion with
            both advertising_channel_types and campaign_ids is not
            supported.
        advertising_channel_types (Sequence[google.ads.googleads.v12.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType]):
            The data_exclusion will apply to all the campaigns under the
            listed channels retroactively as well as going forward when
            the scope of this exclusion is CHANNEL. The supported
            advertising channel types are DISPLAY, SEARCH and SHOPPING.
            Note: a data exclusion with both advertising_channel_types
            and campaign_ids is not supported.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    data_exclusion_id = proto.Field(proto.INT64, number=2,)
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
    campaigns = proto.RepeatedField(proto.STRING, number=10,)
    advertising_channel_types = proto.RepeatedField(
        proto.ENUM,
        number=11,
        enum=advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
