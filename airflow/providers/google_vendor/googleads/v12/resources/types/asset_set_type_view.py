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
    asset_set_type as gage_asset_set_type,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AssetSetTypeView",},
)


class AssetSetTypeView(proto.Message):
    r"""An asset set type view.
    This view reports non-overcounted metrics for each asset set
    type. Child asset set types are not included in this report.
    Their stats are aggregated under the parent asset set type.

    Attributes:
        resource_name (str):
            Output only. The resource name of the asset set type view.
            Asset set type view resource names have the form:

            ``customers/{customer_id}/assetSetTypeViews/{asset_set_type}``
        asset_set_type (google.ads.googleads.v12.enums.types.AssetSetTypeEnum.AssetSetType):
            Output only. The asset set type of the asset
            set type view.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_set_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_asset_set_type.AssetSetTypeEnum.AssetSetType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
