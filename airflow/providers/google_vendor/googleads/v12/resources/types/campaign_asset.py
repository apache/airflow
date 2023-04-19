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
    manifest={"CampaignAsset",},
)


class CampaignAsset(proto.Message):
    r"""A link between a Campaign and an Asset.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign asset.
            CampaignAsset resource names have the form:

            ``customers/{customer_id}/campaignAssets/{campaign_id}~{asset_id}~{field_type}``
        campaign (str):
            Immutable. The campaign to which the asset is
            linked.

            This field is a member of `oneof`_ ``_campaign``.
        asset (str):
            Immutable. The asset which is linked to the
            campaign.

            This field is a member of `oneof`_ ``_asset``.
        field_type (google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Immutable. Role that the asset takes under
            the linked campaign. Required.
        source (google.ads.googleads.v12.enums.types.AssetSourceEnum.AssetSource):
            Output only. Source of the campaign asset
            link.
        status (google.ads.googleads.v12.enums.types.AssetLinkStatusEnum.AssetLinkStatus):
            Status of the campaign asset.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    campaign = proto.Field(proto.STRING, number=6, optional=True,)
    asset = proto.Field(proto.STRING, number=7, optional=True,)
    field_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    source = proto.Field(
        proto.ENUM, number=8, enum=asset_source.AssetSourceEnum.AssetSource,
    )
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=asset_link_status.AssetLinkStatusEnum.AssetLinkStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
