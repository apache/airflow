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
    manifest={"BillingSetupErrorEnum",},
)


class BillingSetupErrorEnum(proto.Message):
    r"""Container for enum describing possible billing setup errors.
    """

    class BillingSetupError(proto.Enum):
        r"""Enum describing possible billing setup errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_USE_EXISTING_AND_NEW_ACCOUNT = 2
        CANNOT_REMOVE_STARTED_BILLING_SETUP = 3
        CANNOT_CHANGE_BILLING_TO_SAME_PAYMENTS_ACCOUNT = 4
        BILLING_SETUP_NOT_PERMITTED_FOR_CUSTOMER_STATUS = 5
        INVALID_PAYMENTS_ACCOUNT = 6
        BILLING_SETUP_NOT_PERMITTED_FOR_CUSTOMER_CATEGORY = 7
        INVALID_START_TIME_TYPE = 8
        THIRD_PARTY_ALREADY_HAS_BILLING = 9
        BILLING_SETUP_IN_PROGRESS = 10
        NO_SIGNUP_PERMISSION = 11
        CHANGE_OF_BILL_TO_IN_PROGRESS = 12
        PAYMENTS_PROFILE_NOT_FOUND = 13
        PAYMENTS_ACCOUNT_NOT_FOUND = 14
        PAYMENTS_PROFILE_INELIGIBLE = 15
        PAYMENTS_ACCOUNT_INELIGIBLE = 16
        CUSTOMER_NEEDS_INTERNAL_APPROVAL = 17
        PAYMENTS_ACCOUNT_INELIGIBLE_CURRENCY_CODE_MISMATCH = 19
        FUTURE_START_TIME_PROHIBITED = 20
        TOO_MANY_BILLING_SETUPS_FOR_PAYMENTS_ACCOUNT = 21


__all__ = tuple(sorted(__protobuf__.manifest))
