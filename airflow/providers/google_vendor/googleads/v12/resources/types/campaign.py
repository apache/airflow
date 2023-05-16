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

from airflow.providers.google_vendor.googleads.v12.common.types import bidding
from airflow.providers.google_vendor.googleads.v12.common.types import custom_parameter
from airflow.providers.google_vendor.googleads.v12.common.types import frequency_cap
from airflow.providers.google_vendor.googleads.v12.common.types import (
    real_time_bidding_setting as gagc_real_time_bidding_setting,
)
from airflow.providers.google_vendor.googleads.v12.common.types import (
    targeting_setting as gagc_targeting_setting,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    ad_serving_optimization_status as gage_ad_serving_optimization_status,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    advertising_channel_sub_type as gage_advertising_channel_sub_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    advertising_channel_type as gage_advertising_channel_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import app_campaign_app_store
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    app_campaign_bidding_strategy_goal_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_field_type
from airflow.providers.google_vendor.googleads.v12.enums.types import asset_set_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    bidding_strategy_system_status as gage_bidding_strategy_system_status,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    bidding_strategy_type as gage_bidding_strategy_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import brand_safety_suitability
from airflow.providers.google_vendor.googleads.v12.enums.types import campaign_experiment_type
from airflow.providers.google_vendor.googleads.v12.enums.types import campaign_primary_status
from airflow.providers.google_vendor.googleads.v12.enums.types import campaign_primary_status_reason
from airflow.providers.google_vendor.googleads.v12.enums.types import campaign_serving_status
from airflow.providers.google_vendor.googleads.v12.enums.types import campaign_status
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    location_source_type as gage_location_source_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    negative_geo_target_type as gage_negative_geo_target_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import optimization_goal_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    payment_mode as gage_payment_mode,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import performance_max_upgrade_status
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    positive_geo_target_type as gage_positive_geo_target_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    vanity_pharma_display_url_mode as gage_vanity_pharma_display_url_mode,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    vanity_pharma_text as gage_vanity_pharma_text,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"Campaign",},
)


class Campaign(proto.Message):
    r"""A campaign.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the campaign. Campaign
            resource names have the form:

            ``customers/{customer_id}/campaigns/{campaign_id}``
        id (int):
            Output only. The ID of the campaign.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The name of the campaign.
            This field is required and should not be empty
            when creating new campaigns.

            It must not contain any null (code point 0x0),
            NL line feed (code point 0xA) or carriage return
            (code point 0xD) characters.

            This field is a member of `oneof`_ ``_name``.
        primary_status (google.ads.googleads.v12.enums.types.CampaignPrimaryStatusEnum.CampaignPrimaryStatus):
            Output only. The primary status of the
            campaign.
            Provides insight into why a campaign is not
            serving or not serving optimally. Modification
            to the campaign and its related entities might
            take a while to be reflected in this status.
        primary_status_reasons (Sequence[google.ads.googleads.v12.enums.types.CampaignPrimaryStatusReasonEnum.CampaignPrimaryStatusReason]):
            Output only. The primary status reasons of
            the campaign.
            Provides insight into why a campaign is not
            serving or not serving optimally. These reasons
            are aggregated to determine an overall
            CampaignPrimaryStatus.
        status (google.ads.googleads.v12.enums.types.CampaignStatusEnum.CampaignStatus):
            The status of the campaign.
            When a new campaign is added, the status
            defaults to ENABLED.
        serving_status (google.ads.googleads.v12.enums.types.CampaignServingStatusEnum.CampaignServingStatus):
            Output only. The ad serving status of the
            campaign.
        bidding_strategy_system_status (google.ads.googleads.v12.enums.types.BiddingStrategySystemStatusEnum.BiddingStrategySystemStatus):
            Output only. The system status of the
            campaign's bidding strategy.
        ad_serving_optimization_status (google.ads.googleads.v12.enums.types.AdServingOptimizationStatusEnum.AdServingOptimizationStatus):
            The ad serving optimization status of the
            campaign.
        advertising_channel_type (google.ads.googleads.v12.enums.types.AdvertisingChannelTypeEnum.AdvertisingChannelType):
            Immutable. The primary serving target for ads within the
            campaign. The targeting options can be refined in
            ``network_settings``.

            This field is required and should not be empty when creating
            new campaigns.

            Can be set only when creating campaigns. After the campaign
            is created, the field can not be changed.
        advertising_channel_sub_type (google.ads.googleads.v12.enums.types.AdvertisingChannelSubTypeEnum.AdvertisingChannelSubType):
            Immutable. Optional refinement to
            ``advertising_channel_type``. Must be a valid sub-type of
            the parent channel type.

            Can be set only when creating campaigns. After campaign is
            created, the field can not be changed.
        tracking_url_template (str):
            The URL template for constructing a tracking
            URL.

            This field is a member of `oneof`_ ``_tracking_url_template``.
        url_custom_parameters (Sequence[google.ads.googleads.v12.common.types.CustomParameter]):
            The list of mappings used to substitute custom parameter
            tags in a ``tracking_url_template``, ``final_urls``, or
            ``mobile_final_urls``.
        local_services_campaign_settings (google.ads.googleads.v12.resources.types.Campaign.LocalServicesCampaignSettings):
            The Local Services Campaign related settings.
        real_time_bidding_setting (google.ads.googleads.v12.common.types.RealTimeBiddingSetting):
            Settings for Real-Time Bidding, a feature
            only available for campaigns targeting the Ad
            Exchange network.
        network_settings (google.ads.googleads.v12.resources.types.Campaign.NetworkSettings):
            The network settings for the campaign.
        hotel_setting (google.ads.googleads.v12.resources.types.Campaign.HotelSettingInfo):
            Immutable. The hotel setting for the
            campaign.
        dynamic_search_ads_setting (google.ads.googleads.v12.resources.types.Campaign.DynamicSearchAdsSetting):
            The setting for controlling Dynamic Search
            Ads (DSA).
        shopping_setting (google.ads.googleads.v12.resources.types.Campaign.ShoppingSetting):
            The setting for controlling Shopping
            campaigns.
        targeting_setting (google.ads.googleads.v12.common.types.TargetingSetting):
            Setting for targeting related features.
        audience_setting (google.ads.googleads.v12.resources.types.Campaign.AudienceSetting):
            Immutable. Setting for audience related
            features.

            This field is a member of `oneof`_ ``_audience_setting``.
        geo_target_type_setting (google.ads.googleads.v12.resources.types.Campaign.GeoTargetTypeSetting):
            The setting for ads geotargeting.
        local_campaign_setting (google.ads.googleads.v12.resources.types.Campaign.LocalCampaignSetting):
            The setting for local campaign.
        app_campaign_setting (google.ads.googleads.v12.resources.types.Campaign.AppCampaignSetting):
            The setting related to App Campaign.
        labels (Sequence[str]):
            Output only. The resource names of labels
            attached to this campaign.
        experiment_type (google.ads.googleads.v12.enums.types.CampaignExperimentTypeEnum.CampaignExperimentType):
            Output only. The type of campaign: normal,
            draft, or experiment.
        base_campaign (str):
            Output only. The resource name of the base campaign of a
            draft or experiment campaign. For base campaigns, this is
            equal to ``resource_name``.

            This field is read-only.

            This field is a member of `oneof`_ ``_base_campaign``.
        campaign_budget (str):
            The budget of the campaign.

            This field is a member of `oneof`_ ``_campaign_budget``.
        bidding_strategy_type (google.ads.googleads.v12.enums.types.BiddingStrategyTypeEnum.BiddingStrategyType):
            Output only. The type of bidding strategy.

            A bidding strategy can be created by setting either the
            bidding scheme to create a standard bidding strategy or the
            ``bidding_strategy`` field to create a portfolio bidding
            strategy.

            This field is read-only.
        accessible_bidding_strategy (str):
            Output only. Resource name of AccessibleBiddingStrategy, a
            read-only view of the unrestricted attributes of the
            attached portfolio bidding strategy identified by
            'bidding_strategy'. Empty, if the campaign does not use a
            portfolio strategy. Unrestricted strategy attributes are
            available to all customers with whom the strategy is shared
            and are read from the AccessibleBiddingStrategy resource. In
            contrast, restricted attributes are only available to the
            owner customer of the strategy and their managers.
            Restricted attributes can only be read from the
            BiddingStrategy resource.
        start_date (str):
            The date when campaign started in serving
            customer's timezone in YYYY-MM-DD format.

            This field is a member of `oneof`_ ``_start_date``.
        campaign_group (str):
            The campaign group this campaign belongs to.

            This field is a member of `oneof`_ ``_campaign_group``.
        end_date (str):
            The last day of the campaign in serving
            customer's timezone in YYYY-MM-DD format. On
            create, defaults to 2037-12-30, which means the
            campaign will run indefinitely. To set an
            existing campaign to run indefinitely, set this
            field to 2037-12-30.

            This field is a member of `oneof`_ ``_end_date``.
        final_url_suffix (str):
            Suffix used to append query parameters to
            landing pages that are served with parallel
            tracking.

            This field is a member of `oneof`_ ``_final_url_suffix``.
        frequency_caps (Sequence[google.ads.googleads.v12.common.types.FrequencyCapEntry]):
            A list that limits how often each user will
            see this campaign's ads.
        video_brand_safety_suitability (google.ads.googleads.v12.enums.types.BrandSafetySuitabilityEnum.BrandSafetySuitability):
            Output only. 3-Tier Brand Safety setting for
            the campaign.
        vanity_pharma (google.ads.googleads.v12.resources.types.Campaign.VanityPharma):
            Describes how unbranded pharma ads will be
            displayed.
        selective_optimization (google.ads.googleads.v12.resources.types.Campaign.SelectiveOptimization):
            Selective optimization setting for this
            campaign, which includes a set of conversion
            actions to optimize this campaign towards.
        optimization_goal_setting (google.ads.googleads.v12.resources.types.Campaign.OptimizationGoalSetting):
            Optimization goal setting for this campaign,
            which includes a set of optimization goal types.
        tracking_setting (google.ads.googleads.v12.resources.types.Campaign.TrackingSetting):
            Output only. Campaign-level settings for
            tracking information.
        payment_mode (google.ads.googleads.v12.enums.types.PaymentModeEnum.PaymentMode):
            Payment mode for the campaign.
        optimization_score (float):
            Output only. Optimization score of the
            campaign.
            Optimization score is an estimate of how well a
            campaign is set to perform. It ranges from 0%
            (0.0) to 100% (1.0), with 100% indicating that
            the campaign is performing at full potential.
            This field is null for unscored campaigns.

            See "About optimization score" at
            https://support.google.com/google-ads/answer/9061546.
            This field is read-only.

            This field is a member of `oneof`_ ``_optimization_score``.
        excluded_parent_asset_field_types (Sequence[google.ads.googleads.v12.enums.types.AssetFieldTypeEnum.AssetFieldType]):
            The asset field types that should be excluded
            from this campaign. Asset links with these field
            types will not be inherited by this campaign
            from the upper level.
        excluded_parent_asset_set_types (Sequence[google.ads.googleads.v12.enums.types.AssetSetTypeEnum.AssetSetType]):
            The asset set types that should be excluded from this
            campaign. Asset set links with these types will not be
            inherited by this campaign from the upper level. Location
            group types (GMB_DYNAMIC_LOCATION_GROUP,
            CHAIN_DYNAMIC_LOCATION_GROUP, and STATIC_LOCATION_GROUP) are
            child types of LOCATION_SYNC. Therefore, if LOCATION_SYNC is
            set for this field, all location group asset sets are not
            allowed to be linked to this campaign, and all Location
            Extension (LE) and Affiliate Location Extensions (ALE) will
            not be served under this campaign. Only LOCATION_SYNC is
            currently supported.
        url_expansion_opt_out (bool):
            Represents opting out of URL expansion to
            more targeted URLs. If opted out (true), only
            the final URLs in the asset group or URLs
            specified in the advertiser's Google Merchant
            Center or business data feeds are targeted. If
            opted in (false), the entire domain will be
            targeted. This field can only be set for
            Performance Max campaigns, where the default
            value is false.

            This field is a member of `oneof`_ ``_url_expansion_opt_out``.
        performance_max_upgrade (google.ads.googleads.v12.resources.types.Campaign.PerformanceMaxUpgrade):
            Output only. Information about campaigns
            being upgraded to Performance Max.
        bidding_strategy (str):
            Portfolio bidding strategy used by campaign.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        commission (google.ads.googleads.v12.common.types.Commission):
            Commission is an automatic bidding strategy
            in which the advertiser pays a certain portion
            of the conversion value.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpa (google.ads.googleads.v12.common.types.ManualCpa):
            Standard Manual CPA bidding strategy.
            Manual bidding strategy that allows advertiser
            to set the bid per advertiser-specified action.
            Supported only for Local Services campaigns.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpc (google.ads.googleads.v12.common.types.ManualCpc):
            Standard Manual CPC bidding strategy.
            Manual click-based bidding where user pays per
            click.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpm (google.ads.googleads.v12.common.types.ManualCpm):
            Standard Manual CPM bidding strategy.
            Manual impression-based bidding where user pays
            per thousand impressions.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        manual_cpv (google.ads.googleads.v12.common.types.ManualCpv):
            Output only. A bidding strategy that pays a
            configurable amount per video view.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        maximize_conversions (google.ads.googleads.v12.common.types.MaximizeConversions):
            Standard Maximize Conversions bidding
            strategy that automatically maximizes number of
            conversions while spending your budget.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        maximize_conversion_value (google.ads.googleads.v12.common.types.MaximizeConversionValue):
            Standard Maximize Conversion Value bidding
            strategy that automatically sets bids to
            maximize revenue while spending your budget.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_cpa (google.ads.googleads.v12.common.types.TargetCpa):
            Standard Target CPA bidding strategy that
            automatically sets bids to help get as many
            conversions as possible at the target
            cost-per-acquisition (CPA) you set.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_impression_share (google.ads.googleads.v12.common.types.TargetImpressionShare):
            Target Impression Share bidding strategy. An
            automated bidding strategy that sets bids to
            achieve a chosen percentage of impressions.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_roas (google.ads.googleads.v12.common.types.TargetRoas):
            Standard Target ROAS bidding strategy that
            automatically maximizes revenue while averaging
            a specific target return on ad spend (ROAS).

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_spend (google.ads.googleads.v12.common.types.TargetSpend):
            Standard Target Spend bidding strategy that
            automatically sets your bids to help get as many
            clicks as possible within your budget.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        percent_cpc (google.ads.googleads.v12.common.types.PercentCpc):
            Standard Percent Cpc bidding strategy where
            bids are a fraction of the advertised price for
            some good or service.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
        target_cpm (google.ads.googleads.v12.common.types.TargetCpm):
            A bidding strategy that automatically
            optimizes cost per thousand impressions.

            This field is a member of `oneof`_ ``campaign_bidding_strategy``.
    """

    class PerformanceMaxUpgrade(proto.Message):
        r"""Information about a campaign being upgraded to Performance
        Max.

        Attributes:
            performance_max_campaign (str):
                Output only. Indicates which Performance Max
                campaign the campaign is upgraded to.
            pre_upgrade_campaign (str):
                Output only. Indicates legacy campaign
                upgraded to Performance Max.
            status (google.ads.googleads.v12.enums.types.PerformanceMaxUpgradeStatusEnum.PerformanceMaxUpgradeStatus):
                Output only. The upgrade status of a campaign
                requested to be upgraded to Performance Max.
        """

        performance_max_campaign = proto.Field(proto.STRING, number=1,)
        pre_upgrade_campaign = proto.Field(proto.STRING, number=2,)
        status = proto.Field(
            proto.ENUM,
            number=3,
            enum=performance_max_upgrade_status.PerformanceMaxUpgradeStatusEnum.PerformanceMaxUpgradeStatus,
        )

    class NetworkSettings(proto.Message):
        r"""The network settings for the campaign.

        Attributes:
            target_google_search (bool):
                Whether ads will be served with google.com
                search results.

                This field is a member of `oneof`_ ``_target_google_search``.
            target_search_network (bool):
                Whether ads will be served on partner sites in the Google
                Search Network (requires ``target_google_search`` to also be
                ``true``).

                This field is a member of `oneof`_ ``_target_search_network``.
            target_content_network (bool):
                Whether ads will be served on specified
                placements in the Google Display Network.
                Placements are specified using the Placement
                criterion.

                This field is a member of `oneof`_ ``_target_content_network``.
            target_partner_search_network (bool):
                Whether ads will be served on the Google
                Partner Network. This is available only to some
                select Google partner accounts.

                This field is a member of `oneof`_ ``_target_partner_search_network``.
        """

        target_google_search = proto.Field(proto.BOOL, number=5, optional=True,)
        target_search_network = proto.Field(
            proto.BOOL, number=6, optional=True,
        )
        target_content_network = proto.Field(
            proto.BOOL, number=7, optional=True,
        )
        target_partner_search_network = proto.Field(
            proto.BOOL, number=8, optional=True,
        )

    class HotelSettingInfo(proto.Message):
        r"""Campaign-level settings for hotel ads.

        Attributes:
            hotel_center_id (int):
                Immutable. The linked Hotel Center account.

                This field is a member of `oneof`_ ``_hotel_center_id``.
        """

        hotel_center_id = proto.Field(proto.INT64, number=2, optional=True,)

    class DynamicSearchAdsSetting(proto.Message):
        r"""The setting for controlling Dynamic Search Ads (DSA).

        Attributes:
            domain_name (str):
                Required. The Internet domain name that this
                setting represents, for example, "google.com" or
                "www.google.com".
            language_code (str):
                Required. The language code specifying the
                language of the domain, for example, "en".
            use_supplied_urls_only (bool):
                Whether the campaign uses advertiser supplied
                URLs exclusively.

                This field is a member of `oneof`_ ``_use_supplied_urls_only``.
            feeds (Sequence[str]):
                The list of page feeds associated with the
                campaign.
        """

        domain_name = proto.Field(proto.STRING, number=6,)
        language_code = proto.Field(proto.STRING, number=7,)
        use_supplied_urls_only = proto.Field(
            proto.BOOL, number=8, optional=True,
        )
        feeds = proto.RepeatedField(proto.STRING, number=9,)

    class ShoppingSetting(proto.Message):
        r"""The setting for Shopping campaigns. Defines the universe of
        products that can be advertised by the campaign, and how this
        campaign interacts with other Shopping campaigns.

        Attributes:
            merchant_id (int):
                Immutable. ID of the Merchant Center account.
                This field is required for create operations.
                This field is immutable for Shopping campaigns.

                This field is a member of `oneof`_ ``_merchant_id``.
            sales_country (str):
                Sales country of products to include in the campaign. Only
                one of feed_label or sales_country can be set. Field is
                immutable except for clearing. Once this field is cleared,
                you must use feed_label if you want to set the sales
                country.

                This field is a member of `oneof`_ ``_sales_country``.
            feed_label (str):
                Feed label of products to include in the campaign. Only one
                of feed_label or sales_country can be set. If used instead
                of sales_country, the feed_label field accepts country codes
                in the same format for example: 'XX'. Otherwise can be any
                string used for feed label in Google Merchant Center.
            campaign_priority (int):
                Priority of the campaign. Campaigns with
                numerically higher priorities take precedence
                over those with lower priorities. This field is
                required for Shopping campaigns, with values
                between 0 and 2, inclusive.
                This field is optional for Smart Shopping
                campaigns, but must be equal to 3 if set.

                This field is a member of `oneof`_ ``_campaign_priority``.
            enable_local (bool):
                Whether to include local products.

                This field is a member of `oneof`_ ``_enable_local``.
            use_vehicle_inventory (bool):
                Immutable. Whether to target Vehicle Listing
                inventory.
        """

        merchant_id = proto.Field(proto.INT64, number=5, optional=True,)
        sales_country = proto.Field(proto.STRING, number=6, optional=True,)
        feed_label = proto.Field(proto.STRING, number=10,)
        campaign_priority = proto.Field(proto.INT32, number=7, optional=True,)
        enable_local = proto.Field(proto.BOOL, number=8, optional=True,)
        use_vehicle_inventory = proto.Field(proto.BOOL, number=9,)

    class TrackingSetting(proto.Message):
        r"""Campaign-level settings for tracking information.

        Attributes:
            tracking_url (str):
                Output only. The url used for dynamic
                tracking.

                This field is a member of `oneof`_ ``_tracking_url``.
        """

        tracking_url = proto.Field(proto.STRING, number=2, optional=True,)

    class GeoTargetTypeSetting(proto.Message):
        r"""Represents a collection of settings related to ads
        geotargeting.

        Attributes:
            positive_geo_target_type (google.ads.googleads.v12.enums.types.PositiveGeoTargetTypeEnum.PositiveGeoTargetType):
                The setting used for positive geotargeting in
                this particular campaign.
            negative_geo_target_type (google.ads.googleads.v12.enums.types.NegativeGeoTargetTypeEnum.NegativeGeoTargetType):
                The setting used for negative geotargeting in
                this particular campaign.
        """

        positive_geo_target_type = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_positive_geo_target_type.PositiveGeoTargetTypeEnum.PositiveGeoTargetType,
        )
        negative_geo_target_type = proto.Field(
            proto.ENUM,
            number=2,
            enum=gage_negative_geo_target_type.NegativeGeoTargetTypeEnum.NegativeGeoTargetType,
        )

    class LocalCampaignSetting(proto.Message):
        r"""Campaign setting for local campaigns.

        Attributes:
            location_source_type (google.ads.googleads.v12.enums.types.LocationSourceTypeEnum.LocationSourceType):
                The location source type for this local
                campaign.
        """

        location_source_type = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_location_source_type.LocationSourceTypeEnum.LocationSourceType,
        )

    class AppCampaignSetting(proto.Message):
        r"""Campaign-level settings for App Campaigns.

        Attributes:
            bidding_strategy_goal_type (google.ads.googleads.v12.enums.types.AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType):
                Represents the goal which the bidding
                strategy of this app campaign should optimize
                towards.
            app_id (str):
                Immutable. A string that uniquely identifies
                a mobile application.

                This field is a member of `oneof`_ ``_app_id``.
            app_store (google.ads.googleads.v12.enums.types.AppCampaignAppStoreEnum.AppCampaignAppStore):
                Immutable. The application store that
                distributes this specific app.
        """

        bidding_strategy_goal_type = proto.Field(
            proto.ENUM,
            number=1,
            enum=app_campaign_bidding_strategy_goal_type.AppCampaignBiddingStrategyGoalTypeEnum.AppCampaignBiddingStrategyGoalType,
        )
        app_id = proto.Field(proto.STRING, number=4, optional=True,)
        app_store = proto.Field(
            proto.ENUM,
            number=3,
            enum=app_campaign_app_store.AppCampaignAppStoreEnum.AppCampaignAppStore,
        )

    class VanityPharma(proto.Message):
        r"""Describes how unbranded pharma ads will be displayed.

        Attributes:
            vanity_pharma_display_url_mode (google.ads.googleads.v12.enums.types.VanityPharmaDisplayUrlModeEnum.VanityPharmaDisplayUrlMode):
                The display mode for vanity pharma URLs.
            vanity_pharma_text (google.ads.googleads.v12.enums.types.VanityPharmaTextEnum.VanityPharmaText):
                The text that will be displayed in display
                URL of the text ad when website description is
                the selected display mode for vanity pharma
                URLs.
        """

        vanity_pharma_display_url_mode = proto.Field(
            proto.ENUM,
            number=1,
            enum=gage_vanity_pharma_display_url_mode.VanityPharmaDisplayUrlModeEnum.VanityPharmaDisplayUrlMode,
        )
        vanity_pharma_text = proto.Field(
            proto.ENUM,
            number=2,
            enum=gage_vanity_pharma_text.VanityPharmaTextEnum.VanityPharmaText,
        )

    class SelectiveOptimization(proto.Message):
        r"""Selective optimization setting for this campaign, which
        includes a set of conversion actions to optimize this campaign
        towards.

        Attributes:
            conversion_actions (Sequence[str]):
                The selected set of conversion actions for
                optimizing this campaign.
        """

        conversion_actions = proto.RepeatedField(proto.STRING, number=2,)

    class OptimizationGoalSetting(proto.Message):
        r"""Optimization goal setting for this campaign, which includes a
        set of optimization goal types.

        Attributes:
            optimization_goal_types (Sequence[google.ads.googleads.v12.enums.types.OptimizationGoalTypeEnum.OptimizationGoalType]):
                The list of optimization goal types.
        """

        optimization_goal_types = proto.RepeatedField(
            proto.ENUM,
            number=1,
            enum=optimization_goal_type.OptimizationGoalTypeEnum.OptimizationGoalType,
        )

    class AudienceSetting(proto.Message):
        r"""Settings for the audience targeting.

        Attributes:
            use_audience_grouped (bool):
                Immutable. If true, this campaign uses an
                Audience resource for audience targeting. If
                false, this campaign may use audience segment
                criteria instead.

                This field is a member of `oneof`_ ``_use_audience_grouped``.
        """

        use_audience_grouped = proto.Field(proto.BOOL, number=1, optional=True,)

    class LocalServicesCampaignSettings(proto.Message):
        r"""Settings for LocalServicesCampaign subresource.

        Attributes:
            category_bids (Sequence[google.ads.googleads.v12.resources.types.Campaign.CategoryBid]):
                Categorical level bids associated with MANUAL_CPA bidding
                strategy.
        """

        category_bids = proto.RepeatedField(
            proto.MESSAGE, number=1, message="Campaign.CategoryBid",
        )

    class CategoryBid(proto.Message):
        r"""Category bids in LocalServicesReportingCampaignSettings.

        Attributes:
            category_id (str):
                Category for which the bid will be associated with. For
                example, xcat:service_area_business_plumber.

                This field is a member of `oneof`_ ``_category_id``.
            manual_cpa_bid_micros (int):
                Manual CPA bid for the category. Bid must be
                greater than the reserve price associated for
                that category. Value is in micros and in the
                advertiser's currency.

                This field is a member of `oneof`_ ``_manual_cpa_bid_micros``.
        """

        category_id = proto.Field(proto.STRING, number=1, optional=True,)
        manual_cpa_bid_micros = proto.Field(
            proto.INT64, number=2, optional=True,
        )

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=59, optional=True,)
    name = proto.Field(proto.STRING, number=58, optional=True,)
    primary_status = proto.Field(
        proto.ENUM,
        number=81,
        enum=campaign_primary_status.CampaignPrimaryStatusEnum.CampaignPrimaryStatus,
    )
    primary_status_reasons = proto.RepeatedField(
        proto.ENUM,
        number=82,
        enum=campaign_primary_status_reason.CampaignPrimaryStatusReasonEnum.CampaignPrimaryStatusReason,
    )
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=campaign_status.CampaignStatusEnum.CampaignStatus,
    )
    serving_status = proto.Field(
        proto.ENUM,
        number=21,
        enum=campaign_serving_status.CampaignServingStatusEnum.CampaignServingStatus,
    )
    bidding_strategy_system_status = proto.Field(
        proto.ENUM,
        number=78,
        enum=gage_bidding_strategy_system_status.BiddingStrategySystemStatusEnum.BiddingStrategySystemStatus,
    )
    ad_serving_optimization_status = proto.Field(
        proto.ENUM,
        number=8,
        enum=gage_ad_serving_optimization_status.AdServingOptimizationStatusEnum.AdServingOptimizationStatus,
    )
    advertising_channel_type = proto.Field(
        proto.ENUM,
        number=9,
        enum=gage_advertising_channel_type.AdvertisingChannelTypeEnum.AdvertisingChannelType,
    )
    advertising_channel_sub_type = proto.Field(
        proto.ENUM,
        number=10,
        enum=gage_advertising_channel_sub_type.AdvertisingChannelSubTypeEnum.AdvertisingChannelSubType,
    )
    tracking_url_template = proto.Field(proto.STRING, number=60, optional=True,)
    url_custom_parameters = proto.RepeatedField(
        proto.MESSAGE, number=12, message=custom_parameter.CustomParameter,
    )
    local_services_campaign_settings = proto.Field(
        proto.MESSAGE, number=75, message=LocalServicesCampaignSettings,
    )
    real_time_bidding_setting = proto.Field(
        proto.MESSAGE,
        number=39,
        message=gagc_real_time_bidding_setting.RealTimeBiddingSetting,
    )
    network_settings = proto.Field(
        proto.MESSAGE, number=14, message=NetworkSettings,
    )
    hotel_setting = proto.Field(
        proto.MESSAGE, number=32, message=HotelSettingInfo,
    )
    dynamic_search_ads_setting = proto.Field(
        proto.MESSAGE, number=33, message=DynamicSearchAdsSetting,
    )
    shopping_setting = proto.Field(
        proto.MESSAGE, number=36, message=ShoppingSetting,
    )
    targeting_setting = proto.Field(
        proto.MESSAGE,
        number=43,
        message=gagc_targeting_setting.TargetingSetting,
    )
    audience_setting = proto.Field(
        proto.MESSAGE, number=73, optional=True, message=AudienceSetting,
    )
    geo_target_type_setting = proto.Field(
        proto.MESSAGE, number=47, message=GeoTargetTypeSetting,
    )
    local_campaign_setting = proto.Field(
        proto.MESSAGE, number=50, message=LocalCampaignSetting,
    )
    app_campaign_setting = proto.Field(
        proto.MESSAGE, number=51, message=AppCampaignSetting,
    )
    labels = proto.RepeatedField(proto.STRING, number=61,)
    experiment_type = proto.Field(
        proto.ENUM,
        number=17,
        enum=campaign_experiment_type.CampaignExperimentTypeEnum.CampaignExperimentType,
    )
    base_campaign = proto.Field(proto.STRING, number=56, optional=True,)
    campaign_budget = proto.Field(proto.STRING, number=62, optional=True,)
    bidding_strategy_type = proto.Field(
        proto.ENUM,
        number=22,
        enum=gage_bidding_strategy_type.BiddingStrategyTypeEnum.BiddingStrategyType,
    )
    accessible_bidding_strategy = proto.Field(proto.STRING, number=71,)
    start_date = proto.Field(proto.STRING, number=63, optional=True,)
    campaign_group = proto.Field(proto.STRING, number=76, optional=True,)
    end_date = proto.Field(proto.STRING, number=64, optional=True,)
    final_url_suffix = proto.Field(proto.STRING, number=65, optional=True,)
    frequency_caps = proto.RepeatedField(
        proto.MESSAGE, number=40, message=frequency_cap.FrequencyCapEntry,
    )
    video_brand_safety_suitability = proto.Field(
        proto.ENUM,
        number=42,
        enum=brand_safety_suitability.BrandSafetySuitabilityEnum.BrandSafetySuitability,
    )
    vanity_pharma = proto.Field(proto.MESSAGE, number=44, message=VanityPharma,)
    selective_optimization = proto.Field(
        proto.MESSAGE, number=45, message=SelectiveOptimization,
    )
    optimization_goal_setting = proto.Field(
        proto.MESSAGE, number=54, message=OptimizationGoalSetting,
    )
    tracking_setting = proto.Field(
        proto.MESSAGE, number=46, message=TrackingSetting,
    )
    payment_mode = proto.Field(
        proto.ENUM,
        number=52,
        enum=gage_payment_mode.PaymentModeEnum.PaymentMode,
    )
    optimization_score = proto.Field(proto.DOUBLE, number=66, optional=True,)
    excluded_parent_asset_field_types = proto.RepeatedField(
        proto.ENUM,
        number=69,
        enum=asset_field_type.AssetFieldTypeEnum.AssetFieldType,
    )
    excluded_parent_asset_set_types = proto.RepeatedField(
        proto.ENUM,
        number=80,
        enum=asset_set_type.AssetSetTypeEnum.AssetSetType,
    )
    url_expansion_opt_out = proto.Field(proto.BOOL, number=72, optional=True,)
    performance_max_upgrade = proto.Field(
        proto.MESSAGE, number=77, message=PerformanceMaxUpgrade,
    )
    bidding_strategy = proto.Field(
        proto.STRING, number=67, oneof="campaign_bidding_strategy",
    )
    commission = proto.Field(
        proto.MESSAGE,
        number=49,
        oneof="campaign_bidding_strategy",
        message=bidding.Commission,
    )
    manual_cpa = proto.Field(
        proto.MESSAGE,
        number=74,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpa,
    )
    manual_cpc = proto.Field(
        proto.MESSAGE,
        number=24,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpc,
    )
    manual_cpm = proto.Field(
        proto.MESSAGE,
        number=25,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpm,
    )
    manual_cpv = proto.Field(
        proto.MESSAGE,
        number=37,
        oneof="campaign_bidding_strategy",
        message=bidding.ManualCpv,
    )
    maximize_conversions = proto.Field(
        proto.MESSAGE,
        number=30,
        oneof="campaign_bidding_strategy",
        message=bidding.MaximizeConversions,
    )
    maximize_conversion_value = proto.Field(
        proto.MESSAGE,
        number=31,
        oneof="campaign_bidding_strategy",
        message=bidding.MaximizeConversionValue,
    )
    target_cpa = proto.Field(
        proto.MESSAGE,
        number=26,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetCpa,
    )
    target_impression_share = proto.Field(
        proto.MESSAGE,
        number=48,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetImpressionShare,
    )
    target_roas = proto.Field(
        proto.MESSAGE,
        number=29,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetRoas,
    )
    target_spend = proto.Field(
        proto.MESSAGE,
        number=27,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetSpend,
    )
    percent_cpc = proto.Field(
        proto.MESSAGE,
        number=34,
        oneof="campaign_bidding_strategy",
        message=bidding.PercentCpc,
    )
    target_cpm = proto.Field(
        proto.MESSAGE,
        number=41,
        oneof="campaign_bidding_strategy",
        message=bidding.TargetCpm,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
