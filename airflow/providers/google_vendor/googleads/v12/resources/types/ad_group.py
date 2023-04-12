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

from airflow.providers.google_vendor.googleads.v12.common.types import custom_parameter
from airflow.providers.google_vendor.googleads.v12.common.types import (
    explorer_auto_optimizer_setting as gagc_explorer_auto_optimizer_setting,
)
from airflow.providers.google_vendor.googleads.v12.common.types import (
    targeting_setting as gagc_targeting_setting,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import ad_group_ad_rotation_mode
from airflow.providers.google_vendor.googleads.v12.enums.types import ad_group_status
from airflow.providers.google_vendor.googleads.v12.enums.types import ad_group_type
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_field_type
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_set_type
from airflow.providers.google_vendor.googleads.v12.enums.types import bidding_source
from airflow.providers.google_vendor.googleads.v12.enums.types import targeting_dimension


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroup",},
)


class AdGroup(proto.Message):
    r"""An ad group.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the ad group. Ad group
            resource names have the form:

            ``customers/{customer_id}/adGroups/{ad_group_id}``
        id (int):
            Output only. The ID of the ad group.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the ad group.
            This field is required and should not be empty
            when creating new ad groups.

            It must contain fewer than 255 UTF-8 full-width
            characters.
            It must not contain any null (code point 0x0),
            NL line feed (code point 0xA) or carriage return
            (code point 0xD) characters.

            This field is a member of `oneof`_ ``_name``.
        status (google.ads.googleads.v12.enums.types.AdGroupStatusEnum.AdGroupStatus):
            The status of the ad group.
        type_ (google.ads.googleads.v12.enums.types.AdGroupTypeEnum.AdGroupType):
            Immutable. The type of the ad group.
        ad_rotation_mode (google.ads.googleads.v12.enums.types.AdGroupAdRotationModeEnum.AdGroupAdRotationMode):
            The ad rotation mode of the ad group.
        base_ad_group (str):
            Output only. For draft or experiment ad
            groups, this field is the resource name of the
            base ad group from which this ad group was
            created. If a draft or experiment ad group does
            not have a base ad group, then this field is
            null.
            For base ad groups, this field equals the ad
            group resource name.
            This field is read-only.

            This field is a member of `oneof`_ ``_base_ad_group``.
        tracking_url_template (str):
            The URL template for constructing a tracking
            URL.

            This field is a member of `oneof`_ ``_tracking_url_template``.
        url_custom_parameters (Sequence[google.ads.googleads.v12.common.types.CustomParameter]):
            The list of mappings used to substitute custom parameter
            tags in a ``tracking_url_template``, ``final_urls``, or
            ``mobile_final_urls``.
        campaign (str):
            Immutable. The campaign to which the ad group
            belongs.

            This field is a member of `oneof`_ ``_campaign``.
        cpc_bid_micros (int):
            The maximum CPC (cost-per-click) bid.

            This field is a member of `oneof`_ ``_cpc_bid_micros``.
        effective_cpc_bid_micros (int):
            Output only. Value will be same as that of
            the CPC (cost-per-click) bid value when the
            bidding strategy is one of manual cpc, enhanced
            cpc, page one promoted or target outrank share,
            otherwise the value will be null.

            This field is a member of `oneof`_ ``_effective_cpc_bid_micros``.
        cpm_bid_micros (int):
            The maximum CPM (cost-per-thousand viewable
            impressions) bid.

            This field is a member of `oneof`_ ``_cpm_bid_micros``.
        target_cpa_micros (int):
            The target CPA (cost-per-acquisition). If the ad group's
            campaign bidding strategy is TargetCpa or
            MaximizeConversions (with its target_cpa field set), then
            this field overrides the target CPA specified in the
            campaign's bidding strategy. Otherwise, this value is
            ignored.

            This field is a member of `oneof`_ ``_target_cpa_micros``.
        cpv_bid_micros (int):
            The CPV (cost-per-view) bid.

            This field is a member of `oneof`_ ``_cpv_bid_micros``.
        target_cpm_micros (int):
            Average amount in micros that the advertiser
            is willing to pay for every thousand times the
            ad is shown.

            This field is a member of `oneof`_ ``_target_cpm_micros``.
        target_roas (float):
            The target ROAS (return-on-ad-spend) override. If the ad
            group's campaign bidding strategy is TargetRoas or
            MaximizeConversionValue (with its target_roas field set),
            then this field overrides the target ROAS specified in the
            campaign's bidding strategy. Otherwise, this value is
            ignored.

            This field is a member of `oneof`_ ``_target_roas``.
        percent_cpc_bid_micros (int):
            The percent cpc bid amount, expressed as a fraction of the
            advertised price for some good or service. The valid range
            for the fraction is [0,1) and the value stored here is
            1,000,000 \* [fraction].

            This field is a member of `oneof`_ ``_percent_cpc_bid_micros``.
        explorer_auto_optimizer_setting (google.ads.googleads.v12.common.types.ExplorerAutoOptimizerSetting):
            Settings for the Display Campaign Optimizer,
            initially termed "Explorer".
        display_custom_bid_dimension (google.ads.googleads.v12.enums.types.TargetingDimensionEnum.TargetingDimension):
            Allows advertisers to specify a targeting
            dimension on which to place absolute bids. This
            is only applicable for campaigns that target
            only the display network and not search.
        final_url_suffix (str):
            URL template for appending params to Final
            URL.

            This field is a member of `oneof`_ ``_final_url_suffix``.
        targeting_setting (google.ads.googleads.v12.common.types.TargetingSetting):
            Setting for targeting related features.
        audience_setting (google.ads.googleads.v12.resources.types.AdGroup.AudienceSetting):
            Immutable. Setting for audience related
            features.
        effective_target_cpa_micros (int):
            Output only. The effective target CPA
            (cost-per-acquisition). This field is read-only.

            This field is a member of `oneof`_ ``_effective_target_cpa_micros``.
        effective_target_cpa_source (google.ads.googleads.v12.enums.types.BiddingSourceEnum.BiddingSource):
            Output only. Source of the effective target
            CPA. This field is read-only.
        effective_target_roas (float):
            Output only. The effective target ROAS
            (return-on-ad-spend). This field is read-only.

            This field is a member of `oneof`_ ``_effective_target_roas``.
        effective_target_roas_source (google.ads.googleads.v12.enums.types.BiddingSourceEnum.BiddingSource):
            Output only. Source of the effective target
            ROAS. This field is read-only.
        labels (Sequence[str]):
            Output only. The resource names of labels
            attached to this ad group.
        excluded_parent_asset_field_types (Sequence[google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType]):
            The asset field types that should be excluded
            from this ad group. Asset links with these field
            types will not be inherited by this ad group
            from the upper levels.
        excluded_parent_asset_set_types (Sequence[google.ads.googleads.v12.enums.types.AssetSetTypeEnum.AssetSetType]):
            The asset set types that should be excluded from this ad
            group. Asset set links with these types will not be
            inherited by this ad group from the upper levels. Location
            group types (GMB_DYNAMIC_LOCATION_GROUP,
            CHAIN_DYNAMIC_LOCATION_GROUP, and STATIC_LOCATION_GROUP) are
            child types of LOCATION_SYNC. Therefore, if LOCATION_SYNC is
            set for this field, all location group asset sets are not
            allowed to be linked to this ad group, and all Location
            Extension (LE) and Affiliate Location Extensions (ALE) will
            not be served under this ad group. Only LOCATION_SYNC is
            currently supported.
    """

    class AudienceSetting(proto.Message):
        r"""Settings for the audience targeting.

        Attributes:
            use_audience_grouped (bool):
                Immutable. If true, this ad group uses an
                Audience resource for audience targeting. If
                false, this ad group may use audience segment
                criteria instead.
        """

        use_audience_grouped = proto.Field(proto.BOOL, number=1,)

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=34, optional=True,)
    name = proto.Field(proto.STRING, number=35, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=ad_group_status.AdGroupStatusEnum.AdGroupStatus,
    )
    type_ = proto.Field(
        proto.ENUM, number=12, enum=ad_group_type.AdGroupTypeEnum.AdGroupType,
    )
    ad_rotation_mode = proto.Field(
        proto.ENUM,
        number=22,
        enum=ad_group_ad_rotation_mode.AdGroupAdRotationModeEnum.AdGroupAdRotationMode,
    )
    base_ad_group = proto.Field(proto.STRING, number=36, optional=True,)
    tracking_url_template = proto.Field(proto.STRING, number=37, optional=True,)
    url_custom_parameters = proto.RepeatedField(
        proto.MESSAGE, number=6, message=custom_parameter.CustomParameter,
    )
    campaign = proto.Field(proto.STRING, number=38, optional=True,)
    cpc_bid_micros = proto.Field(proto.INT64, number=39, optional=True,)
    effective_cpc_bid_micros = proto.Field(
        proto.INT64, number=57, optional=True,
    )
    cpm_bid_micros = proto.Field(proto.INT64, number=40, optional=True,)
    target_cpa_micros = proto.Field(proto.INT64, number=41, optional=True,)
    cpv_bid_micros = proto.Field(proto.INT64, number=42, optional=True,)
    target_cpm_micros = proto.Field(proto.INT64, number=43, optional=True,)
    target_roas = proto.Field(proto.DOUBLE, number=44, optional=True,)
    percent_cpc_bid_micros = proto.Field(proto.INT64, number=45, optional=True,)
    explorer_auto_optimizer_setting = proto.Field(
        proto.MESSAGE,
        number=21,
        message=gagc_explorer_auto_optimizer_setting.ExplorerAutoOptimizerSetting,
    )
    display_custom_bid_dimension = proto.Field(
        proto.ENUM,
        number=23,
        enum=targeting_dimension.TargetingDimensionEnum.TargetingDimension,
    )
    final_url_suffix = proto.Field(proto.STRING, number=46, optional=True,)
    targeting_setting = proto.Field(
        proto.MESSAGE,
        number=25,
        message=gagc_targeting_setting.TargetingSetting,
    )
    audience_setting = proto.Field(
        proto.MESSAGE, number=56, message=AudienceSetting,
    )
    effective_target_cpa_micros = proto.Field(
        proto.INT64, number=47, optional=True,
    )
    effective_target_cpa_source = proto.Field(
        proto.ENUM,
        number=29,
        enum=bidding_source.BiddingSourceEnum.BiddingSource,
    )
    effective_target_roas = proto.Field(proto.DOUBLE, number=48, optional=True,)
    effective_target_roas_source = proto.Field(
        proto.ENUM,
        number=32,
        enum=bidding_source.BiddingSourceEnum.BiddingSource,
    )
    labels = proto.RepeatedField(proto.STRING, number=49,)
    excluded_parent_asset_field_types = proto.RepeatedField(
        proto.ENUM,
        number=54,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    excluded_parent_asset_set_types = proto.RepeatedField(
        proto.ENUM,
        number=58,
        enum=asset_set_type.AssetSetTypeEnum.AssetSetType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
