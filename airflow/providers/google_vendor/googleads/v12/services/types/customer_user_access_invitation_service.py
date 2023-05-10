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

from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_user_access_invitation,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCustomerUserAccessInvitationRequest",
        "CustomerUserAccessInvitationOperation",
        "MutateCustomerUserAccessInvitationResponse",
        "MutateCustomerUserAccessInvitationResult",
    },
)


class MutateCustomerUserAccessInvitationRequest(proto.Message):
    r"""Request message for
    [CustomerUserAccessInvitation.MutateCustomerUserAccessInvitation][]

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose access
            invitation is being modified.
        operation (google.ads.googleads.v12.services.types.CustomerUserAccessInvitationOperation):
            Required. The operation to perform on the
            access invitation
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operation = proto.Field(
        proto.MESSAGE,
        number=2,
        message="CustomerUserAccessInvitationOperation",
    )


class CustomerUserAccessInvitationOperation(proto.Message):
    r"""A single operation (create or remove) on customer user access
    invitation.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        create (google.ads.googleads.v12.resources.types.CustomerUserAccessInvitation):
            Create operation: No resource name is
            expected for the new access invitation.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the revoke invitation
            is expected, in this format:

            ``customers/{customer_id}/customerUserAccessInvitations/{invitation_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=customer_user_access_invitation.CustomerUserAccessInvitation,
    )
    remove = proto.Field(proto.STRING, number=2, oneof="operation",)


class MutateCustomerUserAccessInvitationResponse(proto.Message):
    r"""Response message for access invitation mutate.

    Attributes:
        result (google.ads.googleads.v12.services.types.MutateCustomerUserAccessInvitationResult):
            Result for the mutate.
    """

    result = proto.Field(
        proto.MESSAGE,
        number=1,
        message="MutateCustomerUserAccessInvitationResult",
    )


class MutateCustomerUserAccessInvitationResult(proto.Message):
    r"""The result for the access invitation mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
