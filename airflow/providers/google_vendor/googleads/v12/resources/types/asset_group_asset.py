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

from airflow.providers.google_vendor.googleads.v12.common.types import (
    policy_summary as gagc_policy_summary,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_field_type
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_link_status
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_performance_label


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AssetGroupAsset",},
)


class AssetGroupAsset(proto.Message):
    r"""AssetGroupAsset is the link between an asset and an asset
    group. Adding an AssetGroupAsset links an asset with an asset
    group.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the asset group asset. Asset
            group asset resource name have the form:

            ``customers/{customer_id}/assetGroupAssets/{asset_group_id}~{asset_id}~{field_type}``
        asset_group (str):
            Immutable. The asset group which this asset
            group asset is linking.
        asset (str):
            Immutable. The asset which this asset group
            asset is linking.
        field_type (google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType):
            The description of the placement of the asset within the
            asset group. For example: HEADLINE, YOUTUBE_VIDEO etc
        status (google.ads.googleads.v12.enums.types.AssetLinkStatusEnum.AssetLinkStatus):
            The status of the link between an asset and
            asset group.
        performance_label (google.ads.googleads.v12.enums.types.AssetPerformanceLabelEnum.AssetPerformanceLabel):
            Output only. The performance of this asset
            group asset.
        policy_summary (google.ads.googleads.v12.common.types.PolicySummary):
            Output only. The policy information for this
            asset group asset.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_group = proto.Field(proto.STRING, number=2,)
    asset = proto.Field(proto.STRING, number=3,)
    field_type = proto.Field(
        proto.ENUM,
        number=4,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=asset_link_status.AssetLinkStatusEnum.AssetLinkStatus,
    )
    performance_label = proto.Field(
        proto.ENUM,
        number=6,
        enum=asset_performance_label.AssetPerformanceLabelEnum.AssetPerformanceLabel,
    )
    policy_summary = proto.Field(
        proto.MESSAGE, number=7, message=gagc_policy_summary.PolicySummary,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
