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
    manifest={"AdGroupAsset",},
)


class AdGroupAsset(proto.Message):
    r"""A link between an ad group and an asset.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the ad group asset.
            AdGroupAsset resource names have the form:

            ``customers/{customer_id}/adGroupAssets/{ad_group_id}~{asset_id}~{field_type}``
        ad_group (str):
            Required. Immutable. The ad group to which
            the asset is linked.
        asset (str):
            Required. Immutable. The asset which is
            linked to the ad group.
        field_type (google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Required. Immutable. Role that the asset
            takes under the linked ad group.
        source (google.ads.googleads.v12.enums.types.AssetSourceEnum.AssetSource):
            Output only. Source of the adgroup asset
            link.
        status (google.ads.googleads.v12.enums.types.AssetLinkStatusEnum.AssetLinkStatus):
            Status of the ad group asset.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group = proto.Field(proto.STRING, number=2,)
    asset = proto.Field(proto.STRING, number=3,)
    field_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    source = proto.Field(
        proto.ENUM, number=6, enum=asset_source.AssetSourceEnum.AssetSource,
    )
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=asset_link_status.AssetLinkStatusEnum.AssetLinkStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
