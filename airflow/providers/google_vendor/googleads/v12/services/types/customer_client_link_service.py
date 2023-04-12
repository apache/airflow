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

from airflow.providers.google_vendor.googleads.v12.resources.types import customer_client_link
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCustomerClientLinkRequest",
        "CustomerClientLinkOperation",
        "MutateCustomerClientLinkResponse",
        "MutateCustomerClientLinkResult",
    },
)


class MutateCustomerClientLinkRequest(proto.Message):
    r"""Request message for
    [CustomerClientLinkService.MutateCustomerClientLink][google.ads.googleads.v12.services.CustomerClientLinkService.MutateCustomerClientLink].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            customer link are being modified.
        operation (google.ads.googleads.v12.services.types.CustomerClientLinkOperation):
            Required. The operation to perform on the
            individual CustomerClientLink.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operation = proto.Field(
        proto.MESSAGE, number=2, message="CustomerClientLinkOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)


class CustomerClientLinkOperation(proto.Message):
    r"""A single operation (create, update) on a CustomerClientLink.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v12.resources.types.CustomerClientLink):
            Create operation: No resource name is
            expected for the new link.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v12.resources.types.CustomerClientLink):
            Update operation: The link is expected to
            have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=customer_client_link.CustomerClientLink,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=customer_client_link.CustomerClientLink,
    )


class MutateCustomerClientLinkResponse(proto.Message):
    r"""Response message for a CustomerClientLink mutate.

    Attributes:
        result (google.ads.googleads.v12.services.types.MutateCustomerClientLinkResult):
            A result that identifies the resource
            affected by the mutate request.
    """

    result = proto.Field(
        proto.MESSAGE, number=1, message="MutateCustomerClientLinkResult",
    )


class MutateCustomerClientLinkResult(proto.Message):
    r"""The result for a single customer client link mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
