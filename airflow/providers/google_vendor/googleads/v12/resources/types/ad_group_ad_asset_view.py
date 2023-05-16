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

from airflow.providers.google_vendor.googleads.v12.common.types import policy
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_field_type
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_performance_label
from airflow.providers.google_vendor.googleads.v12.enums.types import policy_approval_status
from airflow.providers.google_vendor.googleads.v12.enums.types import policy_review_status
from airflow.providers.google_vendor.googleads.v12.enums.types import served_asset_field_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupAdAssetView", "AdGroupAdAssetPolicySummary",},
)


class AdGroupAdAssetView(proto.Message):
    r"""A link between an AdGroupAd and an Asset.
    Currently we only support AdGroupAdAssetView for AppAds and
    Responsive Search Ads.

    Attributes:
        resource_name (str):
            Output only. The resource name of the ad group ad asset
            view. Ad group ad asset view resource names have the form
            (Before V4):

            ``customers/{customer_id}/adGroupAdAssets/{AdGroupAdAsset.ad_group_id}~{AdGroupAdAsset.ad.ad_id}~{AdGroupAdAsset.asset_id}~{AdGroupAdAsset.field_type}``

            Ad group ad asset view resource names have the form
            (Beginning from V4):

            ``customers/{customer_id}/adGroupAdAssetViews/{AdGroupAdAsset.ad_group_id}~{AdGroupAdAsset.ad_id}~{AdGroupAdAsset.asset_id}~{AdGroupAdAsset.field_type}``
        ad_group_ad (str):
            Output only. The ad group ad to which the
            asset is linked.

            This field is a member of `oneof`_ ``_ad_group_ad``.
        asset (str):
            Output only. The asset which is linked to the
            ad group ad.

            This field is a member of `oneof`_ ``_asset``.
        field_type (google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Output only. Role that the asset takes in the
            ad.
        enabled (bool):
            Output only. The status between the asset and
            the latest version of the ad. If true, the asset
            is linked to the latest version of the ad. If
            false, it means the link once existed but has
            been removed and is no longer present in the
            latest version of the ad.

            This field is a member of `oneof`_ ``_enabled``.
        policy_summary (google.ads.googleads.v12.resources.types.AdGroupAdAssetPolicySummary):
            Output only. Policy information for the ad
            group ad asset.
        performance_label (google.ads.googleads.v12.enums.types.AssetPerformanceLabelEnum.AssetPerformanceLabel):
            Output only. Performance of an asset linkage.
        pinned_field (google.ads.googleads.v12.enums.types.ServedAssetFieldTypeEnum.ServedAssetFieldType):
            Output only. Pinned field.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group_ad = proto.Field(proto.STRING, number=9, optional=True,)
    asset = proto.Field(proto.STRING, number=10, optional=True,)
    field_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    enabled = proto.Field(proto.BOOL, number=8, optional=True,)
    policy_summary = proto.Field(
        proto.MESSAGE, number=3, message="AdGroupAdAssetPolicySummary",
    )
    performance_label = proto.Field(
        proto.ENUM,
        number=4,
        enum=asset_performance_label.AssetPerformanceLabelEnum.AssetPerformanceLabel,
    )
    pinned_field = proto.Field(
        proto.ENUM,
        number=11,
        enum=served_asset_field_type.ServedAssetFieldTypeEnum.ServedAssetFieldType,
    )


class AdGroupAdAssetPolicySummary(proto.Message):
    r"""Contains policy information for an ad group ad asset.

    Attributes:
        policy_topic_entries (Sequence[google.ads.googleads.v12.common.types.PolicyTopicEntry]):
            Output only. The list of policy findings for
            the ad group ad asset.
        review_status (google.ads.googleads.v12.enums.types.PolicyReviewStatusEnum.PolicyReviewStatus):
            Output only. Where in the review process this
            ad group ad asset is.
        approval_status (google.ads.googleads.v12.enums.types.PolicyApprovalStatusEnum.PolicyApprovalStatus):
            Output only. The overall approval status of
            this ad group ad asset, calculated based on the
            status of its individual policy topic entries.
    """

    policy_topic_entries = proto.RepeatedField(
        proto.MESSAGE, number=1, message=policy.PolicyTopicEntry,
    )
    review_status = proto.Field(
        proto.ENUM,
        number=2,
        enum=policy_review_status.PolicyReviewStatusEnum.PolicyReviewStatus,
    )
    approval_status = proto.Field(
        proto.ENUM,
        number=3,
        enum=policy_approval_status.PolicyApprovalStatusEnum.PolicyApprovalStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
