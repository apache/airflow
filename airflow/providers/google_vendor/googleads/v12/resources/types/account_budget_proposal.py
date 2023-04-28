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

from airflow.providers.google_vendor.googleads.v12.enums.types import account_budget_proposal_status
from airflow.providers.google_vendor.googleads.v12.enums.types import account_budget_proposal_type
from airflow.providers.google_vendor.googleads.v12.enums.types import spending_limit_type
from airflow.providers.google_vendor.googleads.v12.enums.types import time_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AccountBudgetProposal",},
)


class AccountBudgetProposal(proto.Message):
    r"""An account-level budget proposal.

    All fields prefixed with 'proposed' may not necessarily be applied
    directly. For example, proposed spending limits may be adjusted
    before their application. This is true if the 'proposed' field has
    an 'approved' counterpart, for example, spending limits.

    Note that the proposal type (proposal_type) changes which fields are
    required and which must remain empty.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the proposal.
            AccountBudgetProposal resource names have the form:

            ``customers/{customer_id}/accountBudgetProposals/{account_budget_proposal_id}``
        id (int):
            Output only. The ID of the proposal.

            This field is a member of `oneof`_ ``_id``.
        billing_setup (str):
            Immutable. The resource name of the billing
            setup associated with this proposal.

            This field is a member of `oneof`_ ``_billing_setup``.
        account_budget (str):
            Immutable. The resource name of the
            account-level budget associated with this
            proposal.

            This field is a member of `oneof`_ ``_account_budget``.
        proposal_type (google.ads.googleads.v12.enums.types.AccountBudgetProposalTypeEnum.AccountBudgetProposalType):
            Immutable. The type of this proposal, for
            example, END to end the budget associated with
            this proposal.
        status (google.ads.googleads.v12.enums.types.AccountBudgetProposalStatusEnum.AccountBudgetProposalStatus):
            Output only. The status of this proposal.
            When a new proposal is created, the status
            defaults to PENDING.
        proposed_name (str):
            Immutable. The name to assign to the
            account-level budget.

            This field is a member of `oneof`_ ``_proposed_name``.
        approved_start_date_time (str):
            Output only. The approved start date time in
            yyyy-mm-dd hh:mm:ss format.

            This field is a member of `oneof`_ ``_approved_start_date_time``.
        proposed_purchase_order_number (str):
            Immutable. A purchase order number is a value
            that enables the user to help them reference
            this budget in their monthly invoices.

            This field is a member of `oneof`_ ``_proposed_purchase_order_number``.
        proposed_notes (str):
            Immutable. Notes associated with this budget.

            This field is a member of `oneof`_ ``_proposed_notes``.
        creation_date_time (str):
            Output only. The date time when this
            account-level budget proposal was created, which
            is not the same as its approval date time, if
            applicable.

            This field is a member of `oneof`_ ``_creation_date_time``.
        approval_date_time (str):
            Output only. The date time when this
            account-level budget was approved, if
            applicable.

            This field is a member of `oneof`_ ``_approval_date_time``.
        proposed_start_date_time (str):
            Immutable. The proposed start date time in
            yyyy-mm-dd hh:mm:ss format.

            This field is a member of `oneof`_ ``proposed_start_time``.
        proposed_start_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
            Immutable. The proposed start date time as a
            well-defined type, for example, NOW.

            This field is a member of `oneof`_ ``proposed_start_time``.
        proposed_end_date_time (str):
            Immutable. The proposed end date time in
            yyyy-mm-dd hh:mm:ss format.

            This field is a member of `oneof`_ ``proposed_end_time``.
        proposed_end_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
            Immutable. The proposed end date time as a
            well-defined type, for example, FOREVER.

            This field is a member of `oneof`_ ``proposed_end_time``.
        approved_end_date_time (str):
            Output only. The approved end date time in
            yyyy-mm-dd hh:mm:ss format.

            This field is a member of `oneof`_ ``approved_end_time``.
        approved_end_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
            Output only. The approved end date time as a
            well-defined type, for example, FOREVER.

            This field is a member of `oneof`_ ``approved_end_time``.
        proposed_spending_limit_micros (int):
            Immutable. The proposed spending limit in
            micros.  One million is equivalent to one unit.

            This field is a member of `oneof`_ ``proposed_spending_limit``.
        proposed_spending_limit_type (google.ads.googleads.v12.enums.types.SpendingLimitTypeEnum.SpendingLimitType):
            Immutable. The proposed spending limit as a
            well-defined type, for example, INFINITE.

            This field is a member of `oneof`_ ``proposed_spending_limit``.
        approved_spending_limit_micros (int):
            Output only. The approved spending limit in
            micros.  One million is equivalent to one unit.

            This field is a member of `oneof`_ ``approved_spending_limit``.
        approved_spending_limit_type (google.ads.googleads.v12.enums.types.SpendingLimitTypeEnum.SpendingLimitType):
            Output only. The approved spending limit as a
            well-defined type, for example, INFINITE.

            This field is a member of `oneof`_ ``approved_spending_limit``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=25, optional=True,)
    billing_setup = proto.Field(proto.STRING, number=26, optional=True,)
    account_budget = proto.Field(proto.STRING, number=27, optional=True,)
    proposal_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=account_budget_proposal_type.AccountBudgetProposalTypeEnum.AccountBudgetProposalType,
    )
    status = proto.Field(
        proto.ENUM,
        number=15,
        enum=account_budget_proposal_status.AccountBudgetProposalStatusEnum.AccountBudgetProposalStatus,
    )
    proposed_name = proto.Field(proto.STRING, number=28, optional=True,)
    approved_start_date_time = proto.Field(
        proto.STRING, number=30, optional=True,
    )
    proposed_purchase_order_number = proto.Field(
        proto.STRING, number=35, optional=True,
    )
    proposed_notes = proto.Field(proto.STRING, number=36, optional=True,)
    creation_date_time = proto.Field(proto.STRING, number=37, optional=True,)
    approval_date_time = proto.Field(proto.STRING, number=38, optional=True,)
    proposed_start_date_time = proto.Field(
        proto.STRING, number=29, oneof="proposed_start_time",
    )
    proposed_start_time_type = proto.Field(
        proto.ENUM,
        number=7,
        oneof="proposed_start_time",
        enum=time_type.TimeTypeEnum.TimeType,
    )
    proposed_end_date_time = proto.Field(
        proto.STRING, number=31, oneof="proposed_end_time",
    )
    proposed_end_time_type = proto.Field(
        proto.ENUM,
        number=9,
        oneof="proposed_end_time",
        enum=time_type.TimeTypeEnum.TimeType,
    )
    approved_end_date_time = proto.Field(
        proto.STRING, number=32, oneof="approved_end_time",
    )
    approved_end_time_type = proto.Field(
        proto.ENUM,
        number=22,
        oneof="approved_end_time",
        enum=time_type.TimeTypeEnum.TimeType,
    )
    proposed_spending_limit_micros = proto.Field(
        proto.INT64, number=33, oneof="proposed_spending_limit",
    )
    proposed_spending_limit_type = proto.Field(
        proto.ENUM,
        number=11,
        oneof="proposed_spending_limit",
        enum=spending_limit_type.SpendingLimitTypeEnum.SpendingLimitType,
    )
    approved_spending_limit_micros = proto.Field(
        proto.INT64, number=34, oneof="approved_spending_limit",
    )
    approved_spending_limit_type = proto.Field(
        proto.ENUM,
        number=24,
        oneof="approved_spending_limit",
        enum=spending_limit_type.SpendingLimitTypeEnum.SpendingLimitType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
