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

from airflow.providers.google_vendor.googleads.v12.enums.types import ad_strength as gage_ad_strength
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_group_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AssetGroup",},
)


class AssetGroup(proto.Message):
    r"""An asset group.
    AssetGroupAsset is used to link an asset to the asset group.
    AssetGroupSignal is used to associate a signal to an asset
    group.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the asset group. Asset group
            resource names have the form:

            ``customers/{customer_id}/assetGroups/{asset_group_id}``
        id (int):
            Output only. The ID of the asset group.
        campaign (str):
            Immutable. The campaign with which this asset
            group is associated. The asset which is linked
            to the asset group.
        name (str):
            Required. Name of the asset group. Required.
            It must have a minimum length of 1 and maximum
            length of 128. It must be unique under a
            campaign.
        final_urls (Sequence[str]):
            A list of final URLs after all cross domain
            redirects. In performance max, by default, the
            urls are eligible for expansion unless opted
            out.
        final_mobile_urls (Sequence[str]):
            A list of final mobile URLs after all cross
            domain redirects. In performance max, by
            default, the urls are eligible for expansion
            unless opted out.
        status (google.ads.googleads.v12.enums.types.AssetGroupStatusEnum.AssetGroupStatus):
            The status of the asset group.
        path1 (str):
            First part of text that may appear appended
            to the url displayed in the ad.
        path2 (str):
            Second part of text that may appear appended
            to the url displayed in the ad. This field can
            only be set when path1 is set.
        ad_strength (google.ads.googleads.v12.enums.types.AdStrengthEnum.AdStrength):
            Output only. Overall ad strength of this
            asset group.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=9,)
    campaign = proto.Field(proto.STRING, number=2,)
    name = proto.Field(proto.STRING, number=3,)
    final_urls = proto.RepeatedField(proto.STRING, number=4,)
    final_mobile_urls = proto.RepeatedField(proto.STRING, number=5,)
    status = proto.Field(
        proto.ENUM,
        number=6,
        enum=asset_group_status.AssetGroupStatusEnum.AssetGroupStatus,
    )
    path1 = proto.Field(proto.STRING, number=7,)
    path2 = proto.Field(proto.STRING, number=8,)
    ad_strength = proto.Field(
        proto.ENUM, number=10, enum=gage_ad_strength.AdStrengthEnum.AdStrength,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
