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

from airflow.providers.google_vendor.googleads.v12.enums.types import frequency_cap_event_type
from airflow.providers.google_vendor.googleads.v12.enums.types import frequency_cap_level
from airflow.providers.google_vendor.googleads.v12.enums.types import frequency_cap_time_unit


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"FrequencyCapEntry", "FrequencyCapKey",},
)


class FrequencyCapEntry(proto.Message):
    r"""A rule specifying the maximum number of times an ad (or some
    set of ads) can be shown to a user over a particular time
    period.

    Attributes:
        key (google.ads.googleads.v12.common.types.FrequencyCapKey):
            The key of a particular frequency cap. There
            can be no more than one frequency cap with the
            same key.
        cap (int):
            Maximum number of events allowed during the
            time range by this cap.

            This field is a member of `oneof`_ ``_cap``.
    """

    key = proto.Field(proto.MESSAGE, number=1, message="FrequencyCapKey",)
    cap = proto.Field(proto.INT32, number=3, optional=True,)


class FrequencyCapKey(proto.Message):
    r"""A group of fields used as keys for a frequency cap.
    There can be no more than one frequency cap with the same key.

    Attributes:
        level (google.ads.googleads.v12.enums.types.FrequencyCapLevelEnum.FrequencyCapLevel):
            The level on which the cap is to be applied
            (for example, ad group ad, ad group). The cap is
            applied to all the entities of this level.
        event_type (google.ads.googleads.v12.enums.types.FrequencyCapEventTypeEnum.FrequencyCapEventType):
            The type of event that the cap applies to
            (for example, impression).
        time_unit (google.ads.googleads.v12.enums.types.FrequencyCapTimeUnitEnum.FrequencyCapTimeUnit):
            Unit of time the cap is defined at (for
            example, day, week).
        time_length (int):
            Number of time units the cap lasts.

            This field is a member of `oneof`_ ``_time_length``.
    """

    level = proto.Field(
        proto.ENUM,
        number=1,
        enum=frequency_cap_level.FrequencyCapLevelEnum.FrequencyCapLevel,
    )
    event_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=frequency_cap_event_type.FrequencyCapEventTypeEnum.FrequencyCapEventType,
    )
    time_unit = proto.Field(
        proto.ENUM,
        number=2,
        enum=frequency_cap_time_unit.FrequencyCapTimeUnitEnum.FrequencyCapTimeUnit,
    )
    time_length = proto.Field(proto.INT32, number=5, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
