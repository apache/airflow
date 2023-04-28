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
    manifest={"ConversionOrAdjustmentLagBucketEnum",},
)


class ConversionOrAdjustmentLagBucketEnum(proto.Message):
    r"""Container for enum representing the number of days between
    the impression and the conversion or between the impression and
    adjustments to the conversion.

    """

    class ConversionOrAdjustmentLagBucket(proto.Enum):
        r"""Enum representing the number of days between the impression
        and the conversion or between the impression and adjustments to
        the conversion.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        CONVERSION_LESS_THAN_ONE_DAY = 2
        CONVERSION_ONE_TO_TWO_DAYS = 3
        CONVERSION_TWO_TO_THREE_DAYS = 4
        CONVERSION_THREE_TO_FOUR_DAYS = 5
        CONVERSION_FOUR_TO_FIVE_DAYS = 6
        CONVERSION_FIVE_TO_SIX_DAYS = 7
        CONVERSION_SIX_TO_SEVEN_DAYS = 8
        CONVERSION_SEVEN_TO_EIGHT_DAYS = 9
        CONVERSION_EIGHT_TO_NINE_DAYS = 10
        CONVERSION_NINE_TO_TEN_DAYS = 11
        CONVERSION_TEN_TO_ELEVEN_DAYS = 12
        CONVERSION_ELEVEN_TO_TWELVE_DAYS = 13
        CONVERSION_TWELVE_TO_THIRTEEN_DAYS = 14
        CONVERSION_THIRTEEN_TO_FOURTEEN_DAYS = 15
        CONVERSION_FOURTEEN_TO_TWENTY_ONE_DAYS = 16
        CONVERSION_TWENTY_ONE_TO_THIRTY_DAYS = 17
        CONVERSION_THIRTY_TO_FORTY_FIVE_DAYS = 18
        CONVERSION_FORTY_FIVE_TO_SIXTY_DAYS = 19
        CONVERSION_SIXTY_TO_NINETY_DAYS = 20
        ADJUSTMENT_LESS_THAN_ONE_DAY = 21
        ADJUSTMENT_ONE_TO_TWO_DAYS = 22
        ADJUSTMENT_TWO_TO_THREE_DAYS = 23
        ADJUSTMENT_THREE_TO_FOUR_DAYS = 24
        ADJUSTMENT_FOUR_TO_FIVE_DAYS = 25
        ADJUSTMENT_FIVE_TO_SIX_DAYS = 26
        ADJUSTMENT_SIX_TO_SEVEN_DAYS = 27
        ADJUSTMENT_SEVEN_TO_EIGHT_DAYS = 28
        ADJUSTMENT_EIGHT_TO_NINE_DAYS = 29
        ADJUSTMENT_NINE_TO_TEN_DAYS = 30
        ADJUSTMENT_TEN_TO_ELEVEN_DAYS = 31
        ADJUSTMENT_ELEVEN_TO_TWELVE_DAYS = 32
        ADJUSTMENT_TWELVE_TO_THIRTEEN_DAYS = 33
        ADJUSTMENT_THIRTEEN_TO_FOURTEEN_DAYS = 34
        ADJUSTMENT_FOURTEEN_TO_TWENTY_ONE_DAYS = 35
        ADJUSTMENT_TWENTY_ONE_TO_THIRTY_DAYS = 36
        ADJUSTMENT_THIRTY_TO_FORTY_FIVE_DAYS = 37
        ADJUSTMENT_FORTY_FIVE_TO_SIXTY_DAYS = 38
        ADJUSTMENT_SIXTY_TO_NINETY_DAYS = 39
        ADJUSTMENT_NINETY_TO_ONE_HUNDRED_AND_FORTY_FIVE_DAYS = 40
        CONVERSION_UNKNOWN = 41
        ADJUSTMENT_UNKNOWN = 42


__all__ = tuple(sorted(__protobuf__.manifest))
