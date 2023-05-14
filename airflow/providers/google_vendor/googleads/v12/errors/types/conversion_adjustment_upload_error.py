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
    package="airflow.providers.google_vendor.googleads.v12.errors",
    marshal="google.ads.googleads.v12",
    manifest={"ConversionAdjustmentUploadErrorEnum",},
)


class ConversionAdjustmentUploadErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion adjustment
    upload errors.

    """

    class ConversionAdjustmentUploadError(proto.Enum):
        r"""Enum describing possible conversion adjustment upload errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        TOO_RECENT_CONVERSION_ACTION = 2
        INVALID_CONVERSION_ACTION = 3
        CONVERSION_ALREADY_RETRACTED = 4
        CONVERSION_NOT_FOUND = 5
        CONVERSION_EXPIRED = 6
        ADJUSTMENT_PRECEDES_CONVERSION = 7
        MORE_RECENT_RESTATEMENT_FOUND = 8
        TOO_RECENT_CONVERSION = 9
        CANNOT_RESTATE_CONVERSION_ACTION_THAT_ALWAYS_USES_DEFAULT_CONVERSION_VALUE = (
            10
        )
        TOO_MANY_ADJUSTMENTS_IN_REQUEST = 11
        TOO_MANY_ADJUSTMENTS = 12
        RESTATEMENT_ALREADY_EXISTS = 13
        DUPLICATE_ADJUSTMENT_IN_REQUEST = 14
        CUSTOMER_NOT_ACCEPTED_CUSTOMER_DATA_TERMS = 15
        CONVERSION_ACTION_NOT_ELIGIBLE_FOR_ENHANCEMENT = 16
        INVALID_USER_IDENTIFIER = 17
        UNSUPPORTED_USER_IDENTIFIER = 18
        GCLID_DATE_TIME_PAIR_AND_ORDER_ID_BOTH_SET = 20
        CONVERSION_ALREADY_ENHANCED = 21
        DUPLICATE_ENHANCEMENT_IN_REQUEST = 22
        CUSTOMER_DATA_POLICY_PROHIBITS_ENHANCEMENT = 23
        MISSING_ORDER_ID_FOR_WEBPAGE = 24
        ORDER_ID_CONTAINS_PII = 25


__all__ = tuple(sorted(__protobuf__.manifest))
