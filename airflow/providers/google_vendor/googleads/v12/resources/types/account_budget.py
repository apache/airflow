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

from airflow.providers.google_vendor.googleads.v12.enums.types import account_budget_proposal_type
from airflow.providers.google_vendor.googleads.v12.enums.types import account_budget_status
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    spending_limit_type as gage_spending_limit_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import time_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AccountBudget",},
)


class AccountBudget(proto.Message):
    r"""An account-level budget. It contains information about the budget
    itself, as well as the most recently approved changes to the budget
    and proposed changes that are pending approval. The proposed changes
    that are pending approval, if any, are found in 'pending_proposal'.
    Effective details about the budget are found in fields prefixed
    'approved_', 'adjusted_' and those without a prefix. Since some
    effective details may differ from what the user had originally
    requested (for example, spending limit), these differences are
    juxtaposed through 'proposed_', 'approved_', and possibly
    'adjusted_' fields.

    This resource is mutated using AccountBudgetProposal and cannot be
    mutated directly. A budget may have at most one pending proposal at
    any given time. It is read through pending_proposal.

    Once approved, a budget may be subject to adjustments, such as
    credit adjustments. Adjustments create differences between the
    'approved' and 'adjusted' fields, which would otherwise be
    identical.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Output only. The resource name of the account-level budget.
            AccountBudget resource names have the form:

            ``customers/{customer_id}/accountBudgets/{account_budget_id}``
        id (int):
            Output only. The ID of the account-level
            budget.

            This field is a member of `oneof`_ ``_id``.
        billing_setup (str):
            Output only. The resource name of the billing setup
            associated with this account-level budget. BillingSetup
            resource names have the form:

            ``customers/{customer_id}/billingSetups/{billing_setup_id}``

            This field is a member of `oneof`_ ``_billing_setup``.
        status (google.ads.googleads.v12.enums.types.AccountBudgetStatusEnum.AccountBudgetStatus):
            Output only. The status of this account-level
            budget.
        name (str):
            Output only. The name of the account-level
            budget.

            This field is a member of `oneof`_ ``_name``.
        proposed_start_date_time (str):
            Output only. The proposed start time of the
            account-level budget in yyyy-MM-dd HH:mm:ss
            format.  If a start time type of NOW was
            proposed, this is the time of request.

            This field is a member of `oneof`_ ``_proposed_start_date_time``.
        approved_start_date_time (str):
            Output only. The approved start time of the
            account-level budget in yyyy-MM-dd HH:mm:ss
            format.
            For example, if a new budget is approved after
            the proposed start time, the approved start time
            is the time of approval.

            This field is a member of `oneof`_ ``_approved_start_date_time``.
        total_adjustments_micros (int):
            Output only. The total adjustments amount.
            An example of an adjustment is courtesy credits.
        amount_served_micros (int):
            Output only. The value of Ads that have been served, in
            micros.

            This includes overdelivery costs, in which case a credit
            might be automatically applied to the budget (see
            total_adjustments_micros).
        purchase_order_number (str):
            Output only. A purchase order number is a
            value that helps users reference this budget in
            their monthly invoices.

            This field is a member of `oneof`_ ``_purchase_order_number``.
        notes (str):
            Output only. Notes associated with the
            budget.

            This field is a member of `oneof`_ ``_notes``.
        pending_proposal (google.ads.googleads.v12.resources.types.AccountBudget.PendingAccountBudgetProposal):
            Output only. The pending proposal to modify
            this budget, if applicable.
        proposed_end_date_time (str):
            Output only. The proposed end time in
            yyyy-MM-dd HH:mm:ss format.

            This field is a member of `oneof`_ ``proposed_end_time``.
        proposed_end_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
            Output only. The proposed end time as a
            well-defined type, for example, FOREVER.

            This field is a member of `oneof`_ ``proposed_end_time``.
        approved_end_date_time (str):
            Output only. The approved end time in
            yyyy-MM-dd HH:mm:ss format.

            This field is a member of `oneof`_ ``approved_end_time``.
        approved_end_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
            Output only. The approved end time as a
            well-defined type, for example, FOREVER.

            This field is a member of `oneof`_ ``approved_end_time``.
        proposed_spending_limit_micros (int):
            Output only. The proposed spending limit in
            micros.  One million is equivalent to one unit.

            This field is a member of `oneof`_ ``proposed_spending_limit``.
        proposed_spending_limit_type (google.ads.googleads.v12.enums.types.SpendingLimitTypeEnum.SpendingLimitType):
            Output only. The proposed spending limit as a
            well-defined type, for example, INFINITE.

            This field is a member of `oneof`_ ``proposed_spending_limit``.
        approved_spending_limit_micros (int):
            Output only. The approved spending limit in
            micros.  One million is equivalent to one unit.
            This will only be populated if the proposed
            spending limit is finite, and will always be
            greater than or equal to the proposed spending
            limit.

            This field is a member of `oneof`_ ``approved_spending_limit``.
        approved_spending_limit_type (google.ads.googleads.v12.enums.types.SpendingLimitTypeEnum.SpendingLimitType):
            Output only. The approved spending limit as a
            well-defined type, for example, INFINITE.  This
            will only be populated if the approved spending
            limit is INFINITE.

            This field is a member of `oneof`_ ``approved_spending_limit``.
        adjusted_spending_limit_micros (int):
            Output only. The adjusted spending limit in
            micros.  One million is equivalent to one unit.
            If the approved spending limit is finite, the
            adjusted spending limit may vary depending on
            the types of adjustments applied to this budget,
            if applicable.

            The different kinds of adjustments are described
            here:
            https://support.google.com/google-ads/answer/1704323
            For example, a debit adjustment reduces how much
            the account is allowed to spend.

            This field is a member of `oneof`_ ``adjusted_spending_limit``.
        adjusted_spending_limit_type (google.ads.googleads.v12.enums.types.SpendingLimitTypeEnum.SpendingLimitType):
            Output only. The adjusted spending limit as a
            well-defined type, for example, INFINITE. This
            will only be populated if the adjusted spending
            limit is INFINITE, which is guaranteed to be
            true if the approved spending limit is INFINITE.

            This field is a member of `oneof`_ ``adjusted_spending_limit``.
    """

    class PendingAccountBudgetProposal(proto.Message):
        r"""A pending proposal associated with the enclosing
        account-level budget, if applicable.

        This message has `oneof`_ fields (mutually exclusive fields).
        For each oneof, at most one member field can be set at the same time.
        Setting any member of the oneof automatically clears all other
        members.

        .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

        Attributes:
            account_budget_proposal (str):
                Output only. The resource name of the proposal.
                AccountBudgetProposal resource names have the form:

                ``customers/{customer_id}/accountBudgetProposals/{account_budget_proposal_id}``

                This field is a member of `oneof`_ ``_account_budget_proposal``.
            proposal_type (google.ads.googleads.v12.enums.types.AccountBudgetProposalTypeEnum.AccountBudgetProposalType):
                Output only. The type of this proposal, for
                example, END to end the budget associated with
                this proposal.
            name (str):
                Output only. The name to assign to the
                account-level budget.

                This field is a member of `oneof`_ ``_name``.
            start_date_time (str):
                Output only. The start time in yyyy-MM-dd
                HH:mm:ss format.

                This field is a member of `oneof`_ ``_start_date_time``.
            purchase_order_number (str):
                Output only. A purchase order number is a
                value that helps users reference this budget in
                their monthly invoices.

                This field is a member of `oneof`_ ``_purchase_order_number``.
            notes (str):
                Output only. Notes associated with this
                budget.

                This field is a member of `oneof`_ ``_notes``.
            creation_date_time (str):
                Output only. The time when this account-level
                budget proposal was created. Formatted as
                yyyy-MM-dd HH:mm:ss.

                This field is a member of `oneof`_ ``_creation_date_time``.
            end_date_time (str):
                Output only. The end time in yyyy-MM-dd
                HH:mm:ss format.

                This field is a member of `oneof`_ ``end_time``.
            end_time_type (google.ads.googleads.v12.enums.types.TimeTypeEnum.TimeType):
                Output only. The end time as a well-defined
                type, for example, FOREVER.

                This field is a member of `oneof`_ ``end_time``.
            spending_limit_micros (int):
                Output only. The spending limit in micros.
                One million is equivalent to one unit.

                This field is a member of `oneof`_ ``spending_limit``.
            spending_limit_type (google.ads.googleads.v12.enums.types.SpendingLimitTypeEnum.SpendingLimitType):
                Output only. The spending limit as a
                well-defined type, for example, INFINITE.

                This field is a member of `oneof`_ ``spending_limit``.
        """

        account_budget_proposal = proto.Field(
            proto.STRING, number=12, optional=True,
        )
        proposal_type = proto.Field(
            proto.ENUM,
            number=2,
            enum=account_budget_proposal_type.AccountBudgetProposalTypeEnum.AccountBudgetProposalType,
        )
        name = proto.Field(proto.STRING, number=13, optional=True,)
        start_date_time = proto.Field(proto.STRING, number=14, optional=True,)
        purchase_order_number = proto.Field(
            proto.STRING, number=17, optional=True,
        )
        notes = proto.Field(proto.STRING, number=18, optional=True,)
        creation_date_time = proto.Field(
            proto.STRING, number=19, optional=True,
        )
        end_date_time = proto.Field(proto.STRING, number=15, oneof="end_time",)
        end_time_type = proto.Field(
            proto.ENUM,
            number=6,
            oneof="end_time",
            enum=time_type.TimeTypeEnum.TimeType,
        )
        spending_limit_micros = proto.Field(
            proto.INT64, number=16, oneof="spending_limit",
        )
        spending_limit_type = proto.Field(
            proto.ENUM,
            number=8,
            oneof="spending_limit",
            enum=gage_spending_limit_type.SpendingLimitTypeEnum.SpendingLimitType,
        )

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=23, optional=True,)
    billing_setup = proto.Field(proto.STRING, number=24, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=account_budget_status.AccountBudgetStatusEnum.AccountBudgetStatus,
    )
    name = proto.Field(proto.STRING, number=25, optional=True,)
    proposed_start_date_time = proto.Field(
        proto.STRING, number=26, optional=True,
    )
    approved_start_date_time = proto.Field(
        proto.STRING, number=27, optional=True,
    )
    total_adjustments_micros = proto.Field(proto.INT64, number=33,)
    amount_served_micros = proto.Field(proto.INT64, number=34,)
    purchase_order_number = proto.Field(proto.STRING, number=35, optional=True,)
    notes = proto.Field(proto.STRING, number=36, optional=True,)
    pending_proposal = proto.Field(
        proto.MESSAGE, number=22, message=PendingAccountBudgetProposal,
    )
    proposed_end_date_time = proto.Field(
        proto.STRING, number=28, oneof="proposed_end_time",
    )
    proposed_end_time_type = proto.Field(
        proto.ENUM,
        number=9,
        oneof="proposed_end_time",
        enum=time_type.TimeTypeEnum.TimeType,
    )
    approved_end_date_time = proto.Field(
        proto.STRING, number=29, oneof="approved_end_time",
    )
    approved_end_time_type = proto.Field(
        proto.ENUM,
        number=11,
        oneof="approved_end_time",
        enum=time_type.TimeTypeEnum.TimeType,
    )
    proposed_spending_limit_micros = proto.Field(
        proto.INT64, number=30, oneof="proposed_spending_limit",
    )
    proposed_spending_limit_type = proto.Field(
        proto.ENUM,
        number=13,
        oneof="proposed_spending_limit",
        enum=gage_spending_limit_type.SpendingLimitTypeEnum.SpendingLimitType,
    )
    approved_spending_limit_micros = proto.Field(
        proto.INT64, number=31, oneof="approved_spending_limit",
    )
    approved_spending_limit_type = proto.Field(
        proto.ENUM,
        number=15,
        oneof="approved_spending_limit",
        enum=gage_spending_limit_type.SpendingLimitTypeEnum.SpendingLimitType,
    )
    adjusted_spending_limit_micros = proto.Field(
        proto.INT64, number=32, oneof="adjusted_spending_limit",
    )
    adjusted_spending_limit_type = proto.Field(
        proto.ENUM,
        number=17,
        oneof="adjusted_spending_limit",
        enum=gage_spending_limit_type.SpendingLimitTypeEnum.SpendingLimitType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
