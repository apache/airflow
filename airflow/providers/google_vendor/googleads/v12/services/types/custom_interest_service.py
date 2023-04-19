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

from airflow.providers.google_vendor.googleads.v12.resources.types import custom_interest
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateCustomInterestsRequest",
        "CustomInterestOperation",
        "MutateCustomInterestsResponse",
        "MutateCustomInterestResult",
    },
)


class MutateCustomInterestsRequest(proto.Message):
    r"""Request message for
    [CustomInterestService.MutateCustomInterests][google.ads.googleads.v12.services.CustomInterestService.MutateCustomInterests].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose custom
            interests are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.CustomInterestOperation]):
            Required. The list of operations to perform
            on individual custom interests.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="CustomInterestOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=4,)


class CustomInterestOperation(proto.Message):
    r"""A single operation (create, update) on a custom interest.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v12.resources.types.CustomInterest):
            Create operation: No resource name is
            expected for the new custom interest.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v12.resources.types.CustomInterest):
            Update operation: The custom interest is
            expected to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=custom_interest.CustomInterest,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=custom_interest.CustomInterest,
    )


class MutateCustomInterestsResponse(proto.Message):
    r"""Response message for custom interest mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateCustomInterestResult]):
            All results for the mutate.
    """

    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="MutateCustomInterestResult",
    )


class MutateCustomInterestResult(proto.Message):
    r"""The result for the custom interest mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
