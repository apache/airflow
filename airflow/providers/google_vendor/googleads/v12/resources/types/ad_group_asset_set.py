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

from airflow.providers.google_vendor.googleads.v12.enums.types import asset_set_link_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupAssetSet",},
)


class AdGroupAssetSet(proto.Message):
    r"""AdGroupAssetSet is the linkage between an ad group and an
    asset set. Creating an AdGroupAssetSet links an asset set with
    an ad group.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the ad group asset set. Ad
            group asset set resource names have the form:

            ``customers/{customer_id}/adGroupAssetSets/{ad_group_id}~{asset_set_id}``
        ad_group (str):
            Immutable. The ad group to which this asset
            set is linked.
        asset_set (str):
            Immutable. The asset set which is linked to
            the ad group.
        status (google.ads.googleads.v12.enums.types.AssetSetLinkStatusEnum.AssetSetLinkStatus):
            Output only. The status of the ad group asset
            set. Read-only.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    ad_group = proto.Field(proto.STRING, number=2,)
    asset_set = proto.Field(proto.STRING, number=3,)
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=asset_set_link_status.AssetSetLinkStatusEnum.AssetSetLinkStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
