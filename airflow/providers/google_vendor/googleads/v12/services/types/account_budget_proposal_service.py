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

from airflow.providers.google_vendor.googleads.v12.resources.types import account_budget_proposal
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateAccountBudgetProposalRequest",
        "AccountBudgetProposalOperation",
        "MutateAccountBudgetProposalResponse",
        "MutateAccountBudgetProposalResult",
    },
)


class MutateAccountBudgetProposalRequest(proto.Message):
    r"""Request message for
    [AccountBudgetProposalService.MutateAccountBudgetProposal][google.ads.googleads.v12.services.AccountBudgetProposalService.MutateAccountBudgetProposal].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        operation (google.ads.googleads.v12.services.types.AccountBudgetProposalOperation):
            Required. The operation to perform on an
            individual account-level budget proposal.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operation = proto.Field(
        proto.MESSAGE, number=2, message="AccountBudgetProposalOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)


class AccountBudgetProposalOperation(proto.Message):
    r"""A single operation to propose the creation of a new
    account-level budget or edit/end/remove an existing one.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which budget fields
            are modified.  While budgets may be modified,
            proposals that propose such modifications are
            final. Therefore, update operations are not
            supported for proposals.
            Proposals that modify budgets have the 'update'
            proposal type.  Specifying a mask for any other
            proposal type is considered an error.
        create (google.ads.googleads.v12.resources.types.AccountBudgetProposal):
            Create operation: A new proposal to create a
            new budget, edit an existing budget, end an
            actively running budget, or remove an approved
            budget scheduled to start in the future.
            No resource name is expected for the new
            proposal.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed proposal
            is expected, in this format:

            ``customers/{customer_id}/accountBudgetProposals/{account_budget_proposal_id}``
            A request may be cancelled iff it is pending.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=3, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=account_budget_proposal.AccountBudgetProposal,
    )
    remove = proto.Field(proto.STRING, number=1, oneof="operation",)


class MutateAccountBudgetProposalResponse(proto.Message):
    r"""Response message for account-level budget mutate operations.

    Attributes:
        result (google.ads.googleads.v12.services.types.MutateAccountBudgetProposalResult):
            The result of the mutate.
    """

    result = proto.Field(
        proto.MESSAGE, number=2, message="MutateAccountBudgetProposalResult",
    )


class MutateAccountBudgetProposalResult(proto.Message):
    r"""The result for the account budget proposal mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
