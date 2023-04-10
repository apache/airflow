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

from airflow.providers.google_vendor.googleads.v12.enums.types import access_role as gage_access_role
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    response_content_type as gage_response_content_type,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import customer as gagr_customer
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCustomerRequest",
        "CreateCustomerClientRequest",
        "CustomerOperation",
        "CreateCustomerClientResponse",
        "MutateCustomerResponse",
        "MutateCustomerResult",
        "ListAccessibleCustomersRequest",
        "ListAccessibleCustomersResponse",
    },
)


class MutateCustomerRequest(proto.Message):
    r"""Request message for
    [CustomerService.MutateCustomer][google.ads.googleads.v12.services.CustomerService.MutateCustomer].

    Attributes:
        customer_id (str):
            Required. The ID of the customer being
            modified.
        operation (google.ads.googleads.v12.services.types.CustomerOperation):
            Required. The operation to perform on the
            customer
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        response_content_type (google.ads.googleads.v12.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operation = proto.Field(
        proto.MESSAGE, number=4, message="CustomerOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=5,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=6,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class CreateCustomerClientRequest(proto.Message):
    r"""Request message for
    [CustomerService.CreateCustomerClient][google.ads.googleads.v12.services.CustomerService.CreateCustomerClient].

    Attributes:
        customer_id (str):
            Required. The ID of the Manager under whom
            client customer is being created.
        customer_client (google.ads.googleads.v12.resources.types.Customer):
            Required. The new client customer to create.
            The resource name on this customer will be
            ignored.
        email_address (str):
            Email address of the user who should be
            invited on the created client customer.
            Accessible only to customers on the allow-list.

            This field is a member of `oneof`_ ``_email_address``.
        access_role (google.ads.googleads.v12.enums.types.AccessRoleEnum.AccessRole):
            The proposed role of user on the created
            client customer. Accessible only to customers on
            the allow-list.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    customer_client = proto.Field(
        proto.MESSAGE, number=2, message=gagr_customer.Customer,
    )
    email_address = proto.Field(proto.STRING, number=5, optional=True,)
    access_role = proto.Field(
        proto.ENUM, number=4, enum=gage_access_role.AccessRoleEnum.AccessRole,
    )
    validate_only = proto.Field(proto.BOOL, number=6,)


class CustomerOperation(proto.Message):
    r"""A single update on a customer.

    Attributes:
        update (google.ads.googleads.v12.resources.types.Customer):
            Mutate operation. Only updates are supported
            for customer.
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
    """

    update = proto.Field(
        proto.MESSAGE, number=1, message=gagr_customer.Customer,
    )
    update_mask = proto.Field(
        proto.MESSAGE, number=2, message=field_mask_pb2.FieldMask,
    )


class CreateCustomerClientResponse(proto.Message):
    r"""Response message for CreateCustomerClient mutate.

    Attributes:
        resource_name (str):
            The resource name of the newly created customer. Customer
            resource names have the form: ``customers/{customer_id}``.
        invitation_link (str):
            Link for inviting user to access the created
            customer. Accessible to allowlisted customers
            only.
    """

    resource_name = proto.Field(proto.STRING, number=2,)
    invitation_link = proto.Field(proto.STRING, number=3,)


class MutateCustomerResponse(proto.Message):
    r"""Response message for customer mutate.

    Attributes:
        result (google.ads.googleads.v12.services.types.MutateCustomerResult):
            Result for the mutate.
    """

    result = proto.Field(
        proto.MESSAGE, number=2, message="MutateCustomerResult",
    )


class MutateCustomerResult(proto.Message):
    r"""The result for the customer mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        customer (google.ads.googleads.v12.resources.types.Customer):
            The mutated customer with only mutable fields after mutate.
            The fields will only be returned when response_content_type
            is set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    customer = proto.Field(
        proto.MESSAGE, number=2, message=gagr_customer.Customer,
    )


class ListAccessibleCustomersRequest(proto.Message):
    r"""Request message for
    [CustomerService.ListAccessibleCustomers][google.ads.googleads.v12.services.CustomerService.ListAccessibleCustomers].

    """


class ListAccessibleCustomersResponse(proto.Message):
    r"""Response message for
    [CustomerService.ListAccessibleCustomers][google.ads.googleads.v12.services.CustomerService.ListAccessibleCustomers].

    Attributes:
        resource_names (Sequence[str]):
            Resource name of customers directly
            accessible by the user authenticating the call.
    """

    resource_names = proto.RepeatedField(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
