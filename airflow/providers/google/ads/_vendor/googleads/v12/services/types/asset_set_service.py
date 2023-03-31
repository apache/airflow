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

from google.ads.googleads.v12.enums.types import (
    response_content_type as gage_response_content_type,
)
from google.ads.googleads.v12.resources.types import asset_set as gagr_asset_set
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="google.ads.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateAssetSetsRequest",
        "AssetSetOperation",
        "MutateAssetSetsResponse",
        "MutateAssetSetResult",
    },
)


class MutateAssetSetsRequest(proto.Message):
    r"""Request message for
    [AssetSetService.MutateAssetSets][google.ads.googleads.v12.services.AssetSetService.MutateAssetSets].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose asset
            sets are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.AssetSetOperation]):
            Required. The list of operations to perform
            on individual asset sets.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        response_content_type (google.ads.googleads.v12.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="AssetSetOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class AssetSetOperation(proto.Message):
    r"""A single operation (create, remove) on an asset set.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v12.resources.types.AssetSet):
            Create operation: No resource name is
            expected for the new asset set

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v12.resources.types.AssetSet):
            Update operation: The asset set is expected
            to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed asset set
            is expected, in this format:
            ``customers/{customer_id}/assetSets/{asset_set_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_asset_set.AssetSet,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_asset_set.AssetSet,
    )
    remove = proto.Field(proto.STRING, number=3, oneof="operation",)


class MutateAssetSetsResponse(proto.Message):
    r"""Response message for an asset set mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateAssetSetResult]):
            All results for the mutate.
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
    """

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="MutateAssetSetResult",
    )
    partial_failure_error = proto.Field(
        proto.MESSAGE, number=2, message=status_pb2.Status,
    )


class MutateAssetSetResult(proto.Message):
    r"""The result for the asset set mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        asset_set (google.ads.googleads.v12.resources.types.AssetSet):
            The mutated asset set with only mutable fields after mutate.
            The field will only be returned when response_content_type
            is set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_set = proto.Field(
        proto.MESSAGE, number=2, message=gagr_asset_set.AssetSet,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
