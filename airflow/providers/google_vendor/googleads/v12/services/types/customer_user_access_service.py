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

from airflow.providers.google_vendor.googleads.v12.resources.types import customer_user_access
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCustomerUserAccessRequest",
        "CustomerUserAccessOperation",
        "MutateCustomerUserAccessResponse",
        "MutateCustomerUserAccessResult",
    },
)


class MutateCustomerUserAccessRequest(proto.Message):
    r"""Mutate Request for
    [CustomerUserAccessService.MutateCustomerUserAccess][google.ads.googleads.v12.services.CustomerUserAccessService.MutateCustomerUserAccess].

    Attributes:
        customer_id (str):
            Required. The ID of the customer being
            modified.
        operation (google.ads.googleads.v12.services.types.CustomerUserAccessOperation):
            Required. The operation to perform on the
            customer
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operation = proto.Field(
        proto.MESSAGE, number=2, message="CustomerUserAccessOperation",
    )


class CustomerUserAccessOperation(proto.Message):
    r"""A single operation (update, remove) on customer user access.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        update (google.ads.googleads.v12.resources.types.CustomerUserAccess):
            Update operation: The customer user access is
            expected to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed access is
            expected, in this format:

            ``customers/{customer_id}/customerUserAccesses/{CustomerUserAccess.user_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=3, message=field_mask_pb2.FieldMask,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=customer_user_access.CustomerUserAccess,
    )
    remove = proto.Field(proto.STRING, number=2, oneof="operation",)


class MutateCustomerUserAccessResponse(proto.Message):
    r"""Response message for customer user access mutate.

    Attributes:
        result (google.ads.googleads.v12.services.types.MutateCustomerUserAccessResult):
            Result for the mutate.
    """

    result = proto.Field(
        proto.MESSAGE, number=1, message="MutateCustomerUserAccessResult",
    )


class MutateCustomerUserAccessResult(proto.Message):
    r"""The result for the customer user access mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
