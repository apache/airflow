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
    call_tracking_display_location as gage_call_tracking_display_location,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import call_type
from airflow.providers.google_vendor.googleads.v12.enums.types import google_voice_call_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CallView",},
)


class CallView(proto.Message):
    r"""A call view that includes data for call tracking of call-only
    ads or call extensions.

    Attributes:
        resource_name (str):
            Output only. The resource name of the call view. Call view
            resource names have the form:

            ``customers/{customer_id}/callViews/{call_detail_id}``
        caller_country_code (str):
            Output only. Country code of the caller.
        caller_area_code (str):
            Output only. Area code of the caller. Null if
            the call duration is shorter than 15 seconds.
        call_duration_seconds (int):
            Output only. The advertiser-provided call
            duration in seconds.
        start_call_date_time (str):
            Output only. The advertiser-provided call
            start date time.
        end_call_date_time (str):
            Output only. The advertiser-provided call end
            date time.
        call_tracking_display_location (google.ads.googleads.v12.enums.types.CallTrackingDisplayLocationEnum.CallTrackingDisplayLocation):
            Output only. The call tracking display
            location.
        type_ (google.ads.googleads.v12.enums.types.CallTypeEnum.CallType):
            Output only. The type of the call.
        call_status (google.ads.googleads.v12.enums.types.GoogleVoiceCallStatusEnum.GoogleVoiceCallStatus):
            Output only. The status of the call.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    caller_country_code = proto.Field(proto.STRING, number=2,)
    caller_area_code = proto.Field(proto.STRING, number=3,)
    call_duration_seconds = proto.Field(proto.INT64, number=4,)
    start_call_date_time = proto.Field(proto.STRING, number=5,)
    end_call_date_time = proto.Field(proto.STRING, number=6,)
    call_tracking_display_location = proto.Field(
        proto.ENUM,
        number=7,
        enum=gage_call_tracking_display_location.CallTrackingDisplayLocationEnum.CallTrackingDisplayLocation,
    )
    type_ = proto.Field(
        proto.ENUM, number=8, enum=call_type.CallTypeEnum.CallType,
    )
    call_status = proto.Field(
        proto.ENUM,
        number=9,
        enum=google_voice_call_status.GoogleVoiceCallStatusEnum.GoogleVoiceCallStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
