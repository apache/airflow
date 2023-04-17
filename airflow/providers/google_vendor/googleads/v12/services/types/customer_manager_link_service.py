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

from airflow.providers.google_vendor.googleads.v12.resources.types import customer_manager_link
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCustomerManagerLinkRequest",
        "MoveManagerLinkRequest",
        "CustomerManagerLinkOperation",
        "MutateCustomerManagerLinkResponse",
        "MoveManagerLinkResponse",
        "MutateCustomerManagerLinkResult",
    },
)


class MutateCustomerManagerLinkRequest(proto.Message):
    r"""Request message for
    [CustomerManagerLinkService.MutateCustomerManagerLink][google.ads.googleads.v12.services.CustomerManagerLinkService.MutateCustomerManagerLink].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            customer manager links are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.CustomerManagerLinkOperation]):
            Required. The list of operations to perform
            on individual customer manager links.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="CustomerManagerLinkOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)


class MoveManagerLinkRequest(proto.Message):
    r"""Request message for
    [CustomerManagerLinkService.MoveManagerLink][google.ads.googleads.v12.services.CustomerManagerLinkService.MoveManagerLink].

    Attributes:
        customer_id (str):
            Required. The ID of the client customer that
            is being moved.
        previous_customer_manager_link (str):
            Required. The resource name of the previous
            CustomerManagerLink. The resource name has the form:
            ``customers/{customer_id}/customerManagerLinks/{manager_customer_id}~{manager_link_id}``
        new_manager (str):
            Required. The resource name of the new manager customer that
            the client wants to move to. Customer resource names have
            the format: "customers/{customer_id}".
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    previous_customer_manager_link = proto.Field(proto.STRING, number=2,)
    new_manager = proto.Field(proto.STRING, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)


class CustomerManagerLinkOperation(proto.Message):
    r"""Updates the status of a CustomerManagerLink.
    The following actions are possible:
    1. Update operation with status ACTIVE accepts a pending
    invitation. 2. Update operation with status REFUSED declines a
    pending invitation. 3. Update operation with status INACTIVE
    terminates link to manager.


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        update (google.ads.googleads.v12.resources.types.CustomerManagerLink):
            Update operation: The link is expected to
            have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=customer_manager_link.CustomerManagerLink,
    )


class MutateCustomerManagerLinkResponse(proto.Message):
    r"""Response message for a CustomerManagerLink mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateCustomerManagerLinkResult]):
            A result that identifies the resource
            affected by the mutate request.
    """

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="MutateCustomerManagerLinkResult",
    )


class MoveManagerLinkResponse(proto.Message):
    r"""Response message for a CustomerManagerLink moveManagerLink.

    Attributes:
        resource_name (str):
            Returned for successful operations.
            Represents a CustomerManagerLink resource of the
            newly created link between client customer and
            new manager customer.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class MutateCustomerManagerLinkResult(proto.Message):
    r"""The result for the customer manager link mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
