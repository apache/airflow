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

from airflow.providers.google_vendor.googleads.v12.enums.types import asset_field_type
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_link_status
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_source


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomerAsset",},
)


class CustomerAsset(proto.Message):
    r"""A link between a customer and an asset.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customer asset.
            CustomerAsset resource names have the form:

            ``customers/{customer_id}/customerAssets/{asset_id}~{field_type}``
        asset (str):
            Required. Immutable. The asset which is
            linked to the customer.
        field_type (google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Required. Immutable. Role that the asset
            takes for the customer link.
        source (google.ads.googleads.v12.enums.types.AssetSourceEnum.AssetSource):
            Output only. Source of the customer asset
            link.
        status (google.ads.googleads.v12.enums.types.AssetLinkStatusEnum.AssetLinkStatus):
            Status of the customer asset.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset = proto.Field(proto.STRING, number=2,)
    field_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    source = proto.Field(
        proto.ENUM, number=5, enum=asset_source.AssetSourceEnum.AssetSource,
    )
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=asset_link_status.AssetLinkStatusEnum.AssetLinkStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
