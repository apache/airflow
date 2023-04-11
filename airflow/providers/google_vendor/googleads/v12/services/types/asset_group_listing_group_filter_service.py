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
    asset_group_listing_group_filter as gagr_asset_group_listing_group_filter,
)
from google.protobuf import field_mask_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateAssetGroupListingGroupFiltersRequest",
        "AssetGroupListingGroupFilterOperation",
        "MutateAssetGroupListingGroupFiltersResponse",
        "MutateAssetGroupListingGroupFilterResult",
    },
)


class MutateAssetGroupListingGroupFiltersRequest(proto.Message):
    r"""Request message for
    [AssetGroupListingGroupFilterService.MutateAssetGroupListingGroupFilters][google.ads.googleads.v12.services.AssetGroupListingGroupFilterService.MutateAssetGroupListingGroupFilters].
    partial_failure is not supported because the tree needs to be
    validated together.

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose asset
            group listing group filters are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.AssetGroupListingGroupFilterOperation]):
            Required. The list of operations to perform
            on individual asset group listing group filters.
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
        proto.MESSAGE,
        number=2,
        message="AssetGroupListingGroupFilterOperation",
    )
    validate_only = proto.Field(proto.BOOL, number=3,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class AssetGroupListingGroupFilterOperation(proto.Message):
    r"""A single operation (create, remove) on an asset group listing
    group filter.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v12.resources.types.AssetGroupListingGroupFilter):
            Create operation: No resource name is
            expected for the new asset group listing group
            filter.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v12.resources.types.AssetGroupListingGroupFilter):
            Update operation: The asset group listing
            group filter is expected to have a valid
            resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed asset
            group listing group filter is expected, in this format:
            ``customers/{customer_id}/assetGroupListingGroupFilters/{asset_group_id}~{listing_group_filter_id}``
            An entity can be removed only if it's not referenced by
            other parent_listing_group_id. If multiple entities are
            being deleted, the mutates must be in the correct order.

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_asset_group_listing_group_filter.AssetGroupListingGroupFilter,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_asset_group_listing_group_filter.AssetGroupListingGroupFilter,
    )
    remove = proto.Field(proto.STRING, number=3, oneof="operation",)


class MutateAssetGroupListingGroupFiltersResponse(proto.Message):
    r"""Response message for an asset group listing group filter
    mutate.

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.MutateAssetGroupListingGroupFilterResult]):
            All results for the mutate.
    """

    results = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="MutateAssetGroupListingGroupFilterResult",
    )


class MutateAssetGroupListingGroupFilterResult(proto.Message):
    r"""The result for the asset group listing group filter mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
        asset_group_listing_group_filter (google.ads.googleads.v12.resources.types.AssetGroupListingGroupFilter):
            The mutated AssetGroupListingGroupFilter with only mutable
            fields after mutate. The field will only be returned when
            response_content_type is set to "MUTABLE_RESOURCE".
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_group_listing_group_filter = proto.Field(
        proto.MESSAGE,
        number=2,
        message=gagr_asset_group_listing_group_filter.AssetGroupListingGroupFilter,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
