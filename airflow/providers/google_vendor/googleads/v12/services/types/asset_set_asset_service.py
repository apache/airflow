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

from airflow.providers.google_vendor.googleads.v12.enums.types import (
    response_content_type as gage_response_content_type,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_set_asset as gagr_asset_set_asset,
)
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateAssetSetAssetsRequest",
        "AssetSetAssetOperation",
        "MutateAssetSetAssetsResponse",
        "MutateAssetSetAssetResult",
    },
)


class MutateAssetSetAssetsRequest(proto.Message):
    r"""Request message for
    [AssetSetAssetService.MutateAssetSetAssets][google.ads.googleads.v12.services.AssetSetAssetService.MutateAssetSetAssets].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose asset
            set assets are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.AssetSetAssetOperation]):
            Required. The list of operations to perform
            on individual asset set assets.
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
        proto.MESSAGE, number=2, message="AssetSetAssetOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class AssetSetAssetOperation(proto.Message):
    r"""A single operation (create, remove) on an asset set asset.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        create (google.ads.googleads.v12.resources.types.AssetSetAsset):
            Create operation: No resource name is
            expected for the new asset set asset

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed asset set
            asset is expected, in this format:
            ``customers/{customer_id}/assetSetAssets/{asset_set_id}~{asset_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_asset_set_asset.AssetSetAsset,
    )
    remove = proto.Field(proto.STRING, number=2, oneof="operation",)


class MutateAssetSetAssetsResponse(proto.Message):
    r"""Response message for an asset set asset mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateAssetSetAssetResult]):
            All results for the mutate.
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
    """

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="MutateAssetSetAssetResult",
    )
    partial_failure_error = proto.Field(
        proto.MESSAGE, number=2, message=status_pb2.Status,
    )


class MutateAssetSetAssetResult(proto.Message):
    r"""The result for the asset set asset mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        asset_set_asset (google.ads.googleads.v12.resources.types.AssetSetAsset):
            The mutated asset set asset with only mutable fields after
            mutate. The field will only be returned when
            response_content_type is set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_set_asset = proto.Field(
        proto.MESSAGE, number=2, message=gagr_asset_set_asset.AssetSetAsset,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
