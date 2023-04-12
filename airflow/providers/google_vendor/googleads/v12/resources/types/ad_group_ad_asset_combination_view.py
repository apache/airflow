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

from airflow.providers.google_vendor.googleads.v12.common.types import asset_usage


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupAdAssetCombinationView",},
)


class AdGroupAdAssetCombinationView(proto.Message):
    r"""A view on the usage of ad group ad asset combination.
    Now we only support AdGroupAdAssetCombinationView for Responsive
    Search Ads, with more ad types planned for the future.

    Attributes:
        resource_name (str):
            Output only. The resource name of the ad group ad asset
            combination view. The combination ID is 128 bits long, where
            the upper 64 bits are stored in asset_combination_id_high,
            and the lower 64 bits are stored in
            asset_combination_id_low. AdGroupAd Asset Combination view
            resource names have the form:
            ``customers/{customer_id}/adGroupAdAssetCombinationViews/{AdGroupAd.ad_group_id}~{AdGroupAd.ad.ad_id}~{AssetCombination.asset_combination_id_low}~{AssetCombination.asset_combination_id_high}``
        served_assets (Sequence[google.ads.googleads.v12.common.types.AssetUsage]):
            Output only. Served assets.
        enabled (bool):
            Output only. The status between the asset
            combination and the latest version of the ad. If
            true, the asset combination is linked to the
            latest version of the ad. If false, it means the
            link once existed but has been removed and is no
            longer present in the latest version of the ad.

            This field is a member of `oneof`_ ``_enabled``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    served_assets = proto.RepeatedField(
        proto.MESSAGE, number=2, message=asset_usage.AssetUsage,
    )
    enabled = proto.Field(proto.BOOL, number=3, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
