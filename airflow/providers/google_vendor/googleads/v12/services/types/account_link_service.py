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
    account_link as gagr_account_link,
)
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "CreateAccountLinkRequest",
        "CreateAccountLinkResponse",
        "MutateAccountLinkRequest",
        "AccountLinkOperation",
        "MutateAccountLinkResponse",
        "MutateAccountLinkResult",
    },
)


class CreateAccountLinkRequest(proto.Message):
    r"""Request message for
    [AccountLinkService.CreateAccountLink][google.ads.googleads.v12.services.AccountLinkService.CreateAccountLink].

    Attributes:
        customer_id (str):
            Required. The ID of the customer for which
            the account link is created.
        account_link (google.ads.googleads.v12.resources.types.AccountLink):
            Required. The account link to be created.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    account_link = proto.Field(
        proto.MESSAGE, number=2, message=gagr_account_link.AccountLink,
    )


class CreateAccountLinkResponse(proto.Message):
    r"""Response message for
    [AccountLinkService.CreateAccountLink][google.ads.googleads.v12.services.AccountLinkService.CreateAccountLink].

    Attributes:
        resource_name (str):
            Returned for successful operations. Resource
            name of the account link.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class MutateAccountLinkRequest(proto.Message):
    r"""Request message for
    [AccountLinkService.MutateAccountLink][google.ads.googleads.v12.services.AccountLinkService.MutateAccountLink].

    Attributes:
        customer_id (str):
            Required. The ID of the customer being
            modified.
        operation (google.ads.googleads.v12.services.types.AccountLinkOperation):
            Required. The operation to perform on the
            link.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operation = proto.Field(
        proto.MESSAGE, number=2, message="AccountLinkOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)


class AccountLinkOperation(proto.Message):
    r"""A single update on an account link.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        update (google.ads.googleads.v12.resources.types.AccountLink):
            Update operation: The account link is
            expected to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the account link to
            remove is expected, in this format:

            ``customers/{customer_id}/accountLinks/{account_link_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_account_link.AccountLink,
    )
    remove = proto.Field(proto.STRING, number=3, oneof="operation",)


class MutateAccountLinkResponse(proto.Message):
    r"""Response message for account link mutate.

    Attributes:
        result (google.ads.googleads.v12.services.types.MutateAccountLinkResult):
            Result for the mutate.
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
    """

    result = proto.Field(
        proto.MESSAGE, number=1, message="MutateAccountLinkResult",
    )
    partial_failure_error = proto.Field(
        proto.MESSAGE, number=2, message=status_pb2.Status,
    )


class MutateAccountLinkResult(proto.Message):
    r"""The result for the account link mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
