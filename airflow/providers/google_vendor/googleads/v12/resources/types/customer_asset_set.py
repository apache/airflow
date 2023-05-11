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
    manifest={"CustomerAssetSet",},
)


class CustomerAssetSet(proto.Message):
    r"""CustomerAssetSet is the linkage between a customer and an
    asset set. Adding a CustomerAssetSet links an asset set with a
    customer.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customer asset set.
            Asset set asset resource names have the form:

            ``customers/{customer_id}/customerAssetSets/{asset_set_id}``
        asset_set (str):
            Immutable. The asset set which is linked to
            the customer.
        customer (str):
            Immutable. The customer to which this asset
            set is linked.
        status (google.ads.googleads.v12.enums.types.AssetSetLinkStatusEnum.AssetSetLinkStatus):
            Output only. The status of the customer asset
            set asset. Read-only.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_set = proto.Field(proto.STRING, number=2,)
    customer = proto.Field(proto.STRING, number=3,)
    status = proto.Field(
        proto.ENUM,
        number=4,
        enum=asset_set_link_status.AssetSetLinkStatusEnum.AssetSetLinkStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
