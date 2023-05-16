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
    manifest={"AccountBudgetProposalErrorEnum",},
)


class AccountBudgetProposalErrorEnum(proto.Message):
    r"""Container for enum describing possible account budget
    proposal errors.

    """

    class AccountBudgetProposalError(proto.Enum):
        r"""Enum describing possible account budget proposal errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        FIELD_MASK_NOT_ALLOWED = 2
        IMMUTABLE_FIELD = 3
        REQUIRED_FIELD_MISSING = 4
        CANNOT_CANCEL_APPROVED_PROPOSAL = 5
        CANNOT_REMOVE_UNAPPROVED_BUDGET = 6
        CANNOT_REMOVE_RUNNING_BUDGET = 7
        CANNOT_END_UNAPPROVED_BUDGET = 8
        CANNOT_END_INACTIVE_BUDGET = 9
        BUDGET_NAME_REQUIRED = 10
        CANNOT_UPDATE_OLD_BUDGET = 11
        CANNOT_END_IN_PAST = 12
        CANNOT_EXTEND_END_TIME = 13
        PURCHASE_ORDER_NUMBER_REQUIRED = 14
        PENDING_UPDATE_PROPOSAL_EXISTS = 15
        MULTIPLE_BUDGETS_NOT_ALLOWED_FOR_UNAPPROVED_BILLING_SETUP = 16
        CANNOT_UPDATE_START_TIME_FOR_STARTED_BUDGET = 17
        SPENDING_LIMIT_LOWER_THAN_ACCRUED_COST_NOT_ALLOWED = 18
        UPDATE_IS_NO_OP = 19
        END_TIME_MUST_FOLLOW_START_TIME = 20
        BUDGET_DATE_RANGE_INCOMPATIBLE_WITH_BILLING_SETUP = 21
        NOT_AUTHORIZED = 22
        INVALID_BILLING_SETUP = 23
        OVERLAPS_EXISTING_BUDGET = 24
        CANNOT_CREATE_BUDGET_THROUGH_API = 25
        INVALID_MASTER_SERVICE_AGREEMENT = 26


__all__ = tuple(sorted(__protobuf__.manifest))
