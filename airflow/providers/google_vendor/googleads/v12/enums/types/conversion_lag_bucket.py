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
    package="airflow.providers.google_vendor.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"ConversionLagBucketEnum",},
)


class ConversionLagBucketEnum(proto.Message):
    r"""Container for enum representing the number of days between
    impression and conversion.

    """

    class ConversionLagBucket(proto.Enum):
        r"""Enum representing the number of days between impression and
        conversion.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        LESS_THAN_ONE_DAY = 2
        ONE_TO_TWO_DAYS = 3
        TWO_TO_THREE_DAYS = 4
        THREE_TO_FOUR_DAYS = 5
        FOUR_TO_FIVE_DAYS = 6
        FIVE_TO_SIX_DAYS = 7
        SIX_TO_SEVEN_DAYS = 8
        SEVEN_TO_EIGHT_DAYS = 9
        EIGHT_TO_NINE_DAYS = 10
        NINE_TO_TEN_DAYS = 11
        TEN_TO_ELEVEN_DAYS = 12
        ELEVEN_TO_TWELVE_DAYS = 13
        TWELVE_TO_THIRTEEN_DAYS = 14
        THIRTEEN_TO_FOURTEEN_DAYS = 15
        FOURTEEN_TO_TWENTY_ONE_DAYS = 16
        TWENTY_ONE_TO_THIRTY_DAYS = 17
        THIRTY_TO_FORTY_FIVE_DAYS = 18
        FORTY_FIVE_TO_SIXTY_DAYS = 19
        SIXTY_TO_NINETY_DAYS = 20


__all__ = tuple(sorted(__protobuf__.manifest))
