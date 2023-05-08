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


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AssetGroupProductGroupView",},
)


class AssetGroupProductGroupView(proto.Message):
    r"""An asset group product group view.

    Attributes:
        resource_name (str):
            Output only. The resource name of the asset group product
            group view. Asset group product group view resource names
            have the form:

            ``customers/{customer_id}/assetGroupProductGroupViews/{asset_group_id}~{listing_group_filter_id}``
        asset_group (str):
            Output only. The asset group associated with
            the listing group filter.
        asset_group_listing_group_filter (str):
            Output only. The resource name of the asset
            group listing group filter.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_group = proto.Field(proto.STRING, number=2,)
    asset_group_listing_group_filter = proto.Field(proto.STRING, number=4,)


__all__ = tuple(sorted(__protobuf__.manifest))
