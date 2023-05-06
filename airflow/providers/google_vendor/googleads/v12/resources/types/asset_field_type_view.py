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


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AssetFieldTypeView",},
)


class AssetFieldTypeView(proto.Message):
    r"""An asset field type view.
    This view reports non-overcounted metrics for each asset field
    type when the asset is used as extension.

    Attributes:
        resource_name (str):
            Output only. The resource name of the asset field type view.
            Asset field type view resource names have the form:

            ``customers/{customer_id}/assetFieldTypeViews/{field_type}``
        field_type (google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType):
            Output only. The asset field type of the
            asset field type view.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    field_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
