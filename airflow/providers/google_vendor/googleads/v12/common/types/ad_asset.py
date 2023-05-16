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

from airflow.providers.google_vendor.googleads.v12.common.types import asset_policy
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    asset_performance_label as gage_asset_performance_label,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import served_asset_field_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "AdTextAsset",
        "AdImageAsset",
        "AdVideoAsset",
        "AdMediaBundleAsset",
        "AdDiscoveryCarouselCardAsset",
    },
)


class AdTextAsset(proto.Message):
    r"""A text asset used inside an ad.

    Attributes:
        text (str):
            Asset text.

            This field is a member of `oneof`_ ``_text``.
        pinned_field (google.ads.googleads.v12.enums.types.ServedAssetFieldTypeEnum.ServedAssetFieldType):
            The pinned field of the asset. This restricts
            the asset to only serve within this field.
            Multiple assets can be pinned to the same field.
            An asset that is unpinned or pinned to a
            different field will not serve in a field where
            some other asset has been pinned.
        asset_performance_label (google.ads.googleads.v12.enums.types.AssetPerformanceLabelEnum.AssetPerformanceLabel):
            The performance label of this text asset.
        policy_summary_info (google.ads.googleads.v12.common.types.AdAssetPolicySummary):
            The policy summary of this text asset.
    """

    text = proto.Field(proto.STRING, number=4, optional=True,)
    pinned_field = proto.Field(
        proto.ENUM,
        number=2,
        enum=served_asset_field_type.ServedAssetFieldTypeEnum.ServedAssetFieldType,
    )
    asset_performance_label = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_asset_performance_label.AssetPerformanceLabelEnum.AssetPerformanceLabel,
    )
    policy_summary_info = proto.Field(
        proto.MESSAGE, number=6, message=asset_policy.AdAssetPolicySummary,
    )


class AdImageAsset(proto.Message):
    r"""An image asset used inside an ad.

    Attributes:
        asset (str):
            The Asset resource name of this image.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset = proto.Field(proto.STRING, number=2, optional=True,)


class AdVideoAsset(proto.Message):
    r"""A video asset used inside an ad.

    Attributes:
        asset (str):
            The Asset resource name of this video.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset = proto.Field(proto.STRING, number=2, optional=True,)


class AdMediaBundleAsset(proto.Message):
    r"""A media bundle asset used inside an ad.

    Attributes:
        asset (str):
            The Asset resource name of this media bundle.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset = proto.Field(proto.STRING, number=2, optional=True,)


class AdDiscoveryCarouselCardAsset(proto.Message):
    r"""A discovery carousel card asset used inside an ad.

    Attributes:
        asset (str):
            The Asset resource name of this discovery
            carousel card.

            This field is a member of `oneof`_ ``_asset``.
    """

    asset = proto.Field(proto.STRING, number=1, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
