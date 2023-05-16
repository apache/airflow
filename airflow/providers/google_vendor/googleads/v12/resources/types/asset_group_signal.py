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

from airflow.providers.google_vendor.googleads.v12.common.types import criteria


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AssetGroupSignal",},
)


class AssetGroupSignal(proto.Message):
    r"""AssetGroupSignal represents a signal in an asset group. The
    existence of a signal tells the performance max campaign who's
    most likely to convert. Performance Max uses the signal to look
    for new people with similar or stronger intent to find
    conversions across Search, Display, Video, and more.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the asset group signal.
            Asset group signal resource name have the form:

            ``customers/{customer_id}/assetGroupSignals/{asset_group_id}~{signal_id}``
        asset_group (str):
            Immutable. The asset group which this asset
            group signal belongs to.
        audience (google.ads.googleads.v12.common.types.AudienceInfo):
            Immutable. The signal(audience criterion) to
            be used by the performance max campaign.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    asset_group = proto.Field(proto.STRING, number=2,)
    audience = proto.Field(
        proto.MESSAGE, number=3, message=criteria.AudienceInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
