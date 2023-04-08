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
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    ad_destination_type as gage_ad_destination_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    ad_network_type as gage_ad_network_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    budget_campaign_association_status as gage_budget_campaign_association_status,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import click_type as gage_click_type
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    conversion_action_category as gage_conversion_action_category,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    conversion_attribution_event_type as gage_conversion_attribution_event_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    conversion_lag_bucket as gage_conversion_lag_bucket,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    conversion_or_adjustment_lag_bucket as gage_conversion_or_adjustment_lag_bucket,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    conversion_value_rule_primary_dimension as gage_conversion_value_rule_primary_dimension,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import day_of_week as gage_day_of_week
from airflow.providers.google_vendor.googleads.v12.enums.types import device as gage_device
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    external_conversion_source as gage_external_conversion_source,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    hotel_date_selection_type as gage_hotel_date_selection_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    hotel_price_bucket as gage_hotel_price_bucket,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    hotel_rate_type as gage_hotel_rate_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    month_of_year as gage_month_of_year,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    placeholder_type as gage_placeholder_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    product_channel as gage_product_channel,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    product_channel_exclusivity as gage_product_channel_exclusivity,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    product_condition as gage_product_condition,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    recommendation_type as gage_recommendation_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    search_engine_results_page_type as gage_search_engine_results_page_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    search_term_match_type as gage_search_term_match_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    sk_ad_network_ad_event_type as gage_sk_ad_network_ad_event_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    sk_ad_network_attribution_credit as gage_sk_ad_network_attribution_credit,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    sk_ad_network_user_type as gage_sk_ad_network_user_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import slot as gage_slot


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "Segments",
        "Keyword",
        "BudgetCampaignAssociationStatus",
        "AssetInteractionTarget",
        "SkAdNetworkSourceApp",
    },
)


class Segments(proto.Message):
    r"""Segment only fields.

    Attributes:
        ad_destination_type (google.ads.googleads.v12.enums.types.AdDestinationTypeEnum.AdDestinationType):
            Ad Destination type.
        ad_network_type (google.ads.googleads.v12.enums.types.AdNetworkTypeEnum.AdNetworkType):
            Ad network type.
        auction_insight_domain (str):
            Domain (visible URL) of a participant in the
            Auction Insights report.

            This field is a member of `oneof`_ ``_auction_insight_domain``.
        budget_campaign_association_status (google.ads.googleads.v12.common.types.BudgetCampaignAssociationStatus):
            Budget campaign association status.
        click_type (google.ads.googleads.v12.enums.types.ClickTypeEnum.ClickType):
            Click type.
        conversion_action (str):
            Resource name of the conversion action.

            This field is a member of `oneof`_ ``_conversion_action``.
        conversion_action_category (google.ads.googleads.v12.enums.types.ConversionActionCategoryEnum.ConversionActionCategory):
            Conversion action category.
        conversion_action_name (str):
            Conversion action name.

            This field is a member of `oneof`_ ``_conversion_action_name``.
        conversion_adjustment (bool):
            This segments your conversion columns by the
            original conversion and conversion value versus
            the delta if conversions were adjusted. False
            row has the data as originally stated; While
            true row has the delta between data now and the
            data as originally stated. Summing the two
            together results post-adjustment data.

            This field is a member of `oneof`_ ``_conversion_adjustment``.
        conversion_attribution_event_type (google.ads.googleads.v12.enums.types.ConversionAttributionEventTypeEnum.ConversionAttributionEventType):
            Conversion attribution event type.
        conversion_lag_bucket (google.ads.googleads.v12.enums.types.ConversionLagBucketEnum.ConversionLagBucket):
            An enum value representing the number of days
            between the impression and the conversion.
        conversion_or_adjustment_lag_bucket (google.ads.googleads.v12.enums.types.ConversionOrAdjustmentLagBucketEnum.ConversionOrAdjustmentLagBucket):
            An enum value representing the number of days
            between the impression and the conversion or
            between the impression and adjustments to the
            conversion.
        date (str):
            Date to which metrics apply.
            yyyy-MM-dd format, for example, 2018-04-17.

            This field is a member of `oneof`_ ``_date``.
        day_of_week (google.ads.googleads.v12.enums.types.DayOfWeekEnum.DayOfWeek):
            Day of the week, for example, MONDAY.
        device (google.ads.googleads.v12.enums.types.DeviceEnum.Device):
            Device to which metrics apply.
        external_conversion_source (google.ads.googleads.v12.enums.types.ExternalConversionSourceEnum.ExternalConversionSource):
            External conversion source.
        geo_target_airport (str):
            Resource name of the geo target constant that
            represents an airport.

            This field is a member of `oneof`_ ``_geo_target_airport``.
        geo_target_canton (str):
            Resource name of the geo target constant that
            represents a canton.

            This field is a member of `oneof`_ ``_geo_target_canton``.
        geo_target_city (str):
            Resource name of the geo target constant that
            represents a city.

            This field is a member of `oneof`_ ``_geo_target_city``.
        geo_target_country (str):
            Resource name of the geo target constant that
            represents a country.

            This field is a member of `oneof`_ ``_geo_target_country``.
        geo_target_county (str):
            Resource name of the geo target constant that
            represents a county.

            This field is a member of `oneof`_ ``_geo_target_county``.
        geo_target_district (str):
            Resource name of the geo target constant that
            represents a district.

            This field is a member of `oneof`_ ``_geo_target_district``.
        geo_target_metro (str):
            Resource name of the geo target constant that
            represents a metro.

            This field is a member of `oneof`_ ``_geo_target_metro``.
        geo_target_most_specific_location (str):
            Resource name of the geo target constant that
            represents the most specific location.

            This field is a member of `oneof`_ ``_geo_target_most_specific_location``.
        geo_target_postal_code (str):
            Resource name of the geo target constant that
            represents a postal code.

            This field is a member of `oneof`_ ``_geo_target_postal_code``.
        geo_target_province (str):
            Resource name of the geo target constant that
            represents a province.

            This field is a member of `oneof`_ ``_geo_target_province``.
        geo_target_region (str):
            Resource name of the geo target constant that
            represents a region.

            This field is a member of `oneof`_ ``_geo_target_region``.
        geo_target_state (str):
            Resource name of the geo target constant that
            represents a state.

            This field is a member of `oneof`_ ``_geo_target_state``.
        hotel_booking_window_days (int):
            Hotel booking window in days.

            This field is a member of `oneof`_ ``_hotel_booking_window_days``.
        hotel_center_id (int):
            Hotel center ID.

            This field is a member of `oneof`_ ``_hotel_center_id``.
        hotel_check_in_date (str):
            Hotel check-in date. Formatted as yyyy-MM-dd.

            This field is a member of `oneof`_ ``_hotel_check_in_date``.
        hotel_check_in_day_of_week (google.ads.googleads.v12.enums.types.DayOfWeekEnum.DayOfWeek):
            Hotel check-in day of week.
        hotel_city (str):
            Hotel city.

            This field is a member of `oneof`_ ``_hotel_city``.
        hotel_class (int):
            Hotel class.

            This field is a member of `oneof`_ ``_hotel_class``.
        hotel_country (str):
            Hotel country.

            This field is a member of `oneof`_ ``_hotel_country``.
        hotel_date_selection_type (google.ads.googleads.v12.enums.types.HotelDateSelectionTypeEnum.HotelDateSelectionType):
            Hotel date selection type.
        hotel_length_of_stay (int):
            Hotel length of stay.

            This field is a member of `oneof`_ ``_hotel_length_of_stay``.
        hotel_rate_rule_id (str):
            Hotel rate rule ID.

            This field is a member of `oneof`_ ``_hotel_rate_rule_id``.
        hotel_rate_type (google.ads.googleads.v12.enums.types.HotelRateTypeEnum.HotelRateType):
            Hotel rate type.
        hotel_price_bucket (google.ads.googleads.v12.enums.types.HotelPriceBucketEnum.HotelPriceBucket):
            Hotel price bucket.
        hotel_state (str):
            Hotel state.

            This field is a member of `oneof`_ ``_hotel_state``.
        hour (int):
            Hour of day as a number between 0 and 23,
            inclusive.

            This field is a member of `oneof`_ ``_hour``.
        interaction_on_this_extension (bool):
            Only used with feed item metrics.
            Indicates whether the interaction metrics
            occurred on the feed item itself or a different
            extension or ad unit.

            This field is a member of `oneof`_ ``_interaction_on_this_extension``.
        keyword (google.ads.googleads.v12.common.types.Keyword):
            Keyword criterion.
        month (str):
            Month as represented by the date of the first
            day of a month. Formatted as yyyy-MM-dd.

            This field is a member of `oneof`_ ``_month``.
        month_of_year (google.ads.googleads.v12.enums.types.MonthOfYearEnum.MonthOfYear):
            Month of the year, for example, January.
        partner_hotel_id (str):
            Partner hotel ID.

            This field is a member of `oneof`_ ``_partner_hotel_id``.
        placeholder_type (google.ads.googleads.v12.enums.types.PlaceholderTypeEnum.PlaceholderType):
            Placeholder type. This is only used with feed
            item metrics.
        product_aggregator_id (int):
            Aggregator ID of the product.

            This field is a member of `oneof`_ ``_product_aggregator_id``.
        product_bidding_category_level1 (str):
            Bidding category (level 1) of the product.

            This field is a member of `oneof`_ ``_product_bidding_category_level1``.
        product_bidding_category_level2 (str):
            Bidding category (level 2) of the product.

            This field is a member of `oneof`_ ``_product_bidding_category_level2``.
        product_bidding_category_level3 (str):
            Bidding category (level 3) of the product.

            This field is a member of `oneof`_ ``_product_bidding_category_level3``.
        product_bidding_category_level4 (str):
            Bidding category (level 4) of the product.

            This field is a member of `oneof`_ ``_product_bidding_category_level4``.
        product_bidding_category_level5 (str):
            Bidding category (level 5) of the product.

            This field is a member of `oneof`_ ``_product_bidding_category_level5``.
        product_brand (str):
            Brand of the product.

            This field is a member of `oneof`_ ``_product_brand``.
        product_channel (google.ads.googleads.v12.enums.types.ProductChannelEnum.ProductChannel):
            Channel of the product.
        product_channel_exclusivity (google.ads.googleads.v12.enums.types.ProductChannelExclusivityEnum.ProductChannelExclusivity):
            Channel exclusivity of the product.
        product_condition (google.ads.googleads.v12.enums.types.ProductConditionEnum.ProductCondition):
            Condition of the product.
        product_country (str):
            Resource name of the geo target constant for
            the country of sale of the product.

            This field is a member of `oneof`_ ``_product_country``.
        product_custom_attribute0 (str):
            Custom attribute 0 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute0``.
        product_custom_attribute1 (str):
            Custom attribute 1 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute1``.
        product_custom_attribute2 (str):
            Custom attribute 2 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute2``.
        product_custom_attribute3 (str):
            Custom attribute 3 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute3``.
        product_custom_attribute4 (str):
            Custom attribute 4 of the product.

            This field is a member of `oneof`_ ``_product_custom_attribute4``.
        product_item_id (str):
            Item ID of the product.

            This field is a member of `oneof`_ ``_product_item_id``.
        product_language (str):
            Resource name of the language constant for
            the language of the product.

            This field is a member of `oneof`_ ``_product_language``.
        product_merchant_id (int):
            Merchant ID of the product.

            This field is a member of `oneof`_ ``_product_merchant_id``.
        product_store_id (str):
            Store ID of the product.

            This field is a member of `oneof`_ ``_product_store_id``.
        product_title (str):
            Title of the product.

            This field is a member of `oneof`_ ``_product_title``.
        product_type_l1 (str):
            Type (level 1) of the product.

            This field is a member of `oneof`_ ``_product_type_l1``.
        product_type_l2 (str):
            Type (level 2) of the product.

            This field is a member of `oneof`_ ``_product_type_l2``.
        product_type_l3 (str):
            Type (level 3) of the product.

            This field is a member of `oneof`_ ``_product_type_l3``.
        product_type_l4 (str):
            Type (level 4) of the product.

            This field is a member of `oneof`_ ``_product_type_l4``.
        product_type_l5 (str):
            Type (level 5) of the product.

            This field is a member of `oneof`_ ``_product_type_l5``.
        quarter (str):
            Quarter as represented by the date of the
            first day of a quarter. Uses the calendar year
            for quarters, for example, the second quarter of
            2018 starts on 2018-04-01. Formatted as
            yyyy-MM-dd.

            This field is a member of `oneof`_ ``_quarter``.
        recommendation_type (google.ads.googleads.v12.enums.types.RecommendationTypeEnum.RecommendationType):
            Recommendation type.
        search_engine_results_page_type (google.ads.googleads.v12.enums.types.SearchEngineResultsPageTypeEnum.SearchEngineResultsPageType):
            Type of the search engine results page.
        search_term_match_type (google.ads.googleads.v12.enums.types.SearchTermMatchTypeEnum.SearchTermMatchType):
            Match type of the keyword that triggered the
            ad, including variants.
        slot (google.ads.googleads.v12.enums.types.SlotEnum.Slot):
            Position of the ad.
        conversion_value_rule_primary_dimension (google.ads.googleads.v12.enums.types.ConversionValueRulePrimaryDimensionEnum.ConversionValueRulePrimaryDimension):
            Primary dimension of applied conversion value rules.
            NO_RULE_APPLIED shows the total recorded value of
            conversions that do not have a value rule applied. ORIGINAL
            shows the original value of conversions to which a value
            rule has been applied. GEO_LOCATION, DEVICE, AUDIENCE show
            the net adjustment after value rules were applied.
        webpage (str):
            Resource name of the ad group criterion that
            represents webpage criterion.

            This field is a member of `oneof`_ ``_webpage``.
        week (str):
            Week as defined as Monday through Sunday, and
            represented by the date of Monday. Formatted as
            yyyy-MM-dd.

            This field is a member of `oneof`_ ``_week``.
        year (int):
            Year, formatted as yyyy.

            This field is a member of `oneof`_ ``_year``.
        sk_ad_network_conversion_value (int):
            iOS Store Kit Ad Network conversion value.
            Null value means this segment is not applicable,
            for example, non-iOS campaign.

            This field is a member of `oneof`_ ``_sk_ad_network_conversion_value``.
        sk_ad_network_user_type (google.ads.googleads.v12.enums.types.SkAdNetworkUserTypeEnum.SkAdNetworkUserType):
            iOS Store Kit Ad Network user type.
        sk_ad_network_ad_event_type (google.ads.googleads.v12.enums.types.SkAdNetworkAdEventTypeEnum.SkAdNetworkAdEventType):
            iOS Store Kit Ad Network ad event type.
        sk_ad_network_source_app (google.ads.googleads.v12.common.types.SkAdNetworkSourceApp):
            App where the ad that drove the iOS Store Kit
            Ad Network install was shown. Null value means
            this segment is not applicable, for example,
            non-iOS campaign, or was not present in any
            postbacks sent by Apple.

            This field is a member of `oneof`_ ``_sk_ad_network_source_app``.
        sk_ad_network_attribution_credit (google.ads.googleads.v12.enums.types.SkAdNetworkAttributionCreditEnum.SkAdNetworkAttributionCredit):
            iOS Store Kit Ad Network attribution credit
        asset_interaction_target (google.ads.googleads.v12.common.types.AssetInteractionTarget):
            Only used with CustomerAsset, CampaignAsset and AdGroupAsset
            metrics. Indicates whether the interaction metrics occurred
            on the asset itself or a different asset or ad unit.
            Interactions (for example, clicks) are counted across all
            the parts of the served ad (for example, Ad itself and other
            components like Sitelinks) when they are served together.
            When interaction_on_this_asset is true, it means the
            interactions are on this specific asset and when
            interaction_on_this_asset is false, it means the
            interactions is not on this specific asset but on other
            parts of the served ad this asset is served with.

            This field is a member of `oneof`_ ``_asset_interaction_target``.
    """

    ad_destination_type = proto.Field(
        proto.ENUM,
        number=136,
        enum=gage_ad_destination_type.AdDestinationTypeEnum.AdDestinationType,
    )
    ad_network_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_ad_network_type.AdNetworkTypeEnum.AdNetworkType,
    )
    auction_insight_domain = proto.Field(
        proto.STRING, number=145, optional=True,
    )
    budget_campaign_association_status = proto.Field(
        proto.MESSAGE, number=134, message="BudgetCampaignAssociationStatus",
    )
    click_type = proto.Field(
        proto.ENUM, number=26, enum=gage_click_type.ClickTypeEnum.ClickType,
    )
    conversion_action = proto.Field(proto.STRING, number=113, optional=True,)
    conversion_action_category = proto.Field(
        proto.ENUM,
        number=53,
        enum=gage_conversion_action_category.ConversionActionCategoryEnum.ConversionActionCategory,
    )
    conversion_action_name = proto.Field(
        proto.STRING, number=114, optional=True,
    )
    conversion_adjustment = proto.Field(proto.BOOL, number=115, optional=True,)
    conversion_attribution_event_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_conversion_attribution_event_type.ConversionAttributionEventTypeEnum.ConversionAttributionEventType,
    )
    conversion_lag_bucket = proto.Field(
        proto.ENUM,
        number=50,
        enum=gage_conversion_lag_bucket.ConversionLagBucketEnum.ConversionLagBucket,
    )
    conversion_or_adjustment_lag_bucket = proto.Field(
        proto.ENUM,
        number=51,
        enum=gage_conversion_or_adjustment_lag_bucket.ConversionOrAdjustmentLagBucketEnum.ConversionOrAdjustmentLagBucket,
    )
    date = proto.Field(proto.STRING, number=79, optional=True,)
    day_of_week = proto.Field(
        proto.ENUM, number=5, enum=gage_day_of_week.DayOfWeekEnum.DayOfWeek,
    )
    device = proto.Field(
        proto.ENUM, number=1, enum=gage_device.DeviceEnum.Device,
    )
    external_conversion_source = proto.Field(
        proto.ENUM,
        number=55,
        enum=gage_external_conversion_source.ExternalConversionSourceEnum.ExternalConversionSource,
    )
    geo_target_airport = proto.Field(proto.STRING, number=116, optional=True,)
    geo_target_canton = proto.Field(proto.STRING, number=117, optional=True,)
    geo_target_city = proto.Field(proto.STRING, number=118, optional=True,)
    geo_target_country = proto.Field(proto.STRING, number=119, optional=True,)
    geo_target_county = proto.Field(proto.STRING, number=120, optional=True,)
    geo_target_district = proto.Field(proto.STRING, number=121, optional=True,)
    geo_target_metro = proto.Field(proto.STRING, number=122, optional=True,)
    geo_target_most_specific_location = proto.Field(
        proto.STRING, number=123, optional=True,
    )
    geo_target_postal_code = proto.Field(
        proto.STRING, number=124, optional=True,
    )
    geo_target_province = proto.Field(proto.STRING, number=125, optional=True,)
    geo_target_region = proto.Field(proto.STRING, number=126, optional=True,)
    geo_target_state = proto.Field(proto.STRING, number=127, optional=True,)
    hotel_booking_window_days = proto.Field(
        proto.INT64, number=135, optional=True,
    )
    hotel_center_id = proto.Field(proto.INT64, number=80, optional=True,)
    hotel_check_in_date = proto.Field(proto.STRING, number=81, optional=True,)
    hotel_check_in_day_of_week = proto.Field(
        proto.ENUM, number=9, enum=gage_day_of_week.DayOfWeekEnum.DayOfWeek,
    )
    hotel_city = proto.Field(proto.STRING, number=82, optional=True,)
    hotel_class = proto.Field(proto.INT32, number=83, optional=True,)
    hotel_country = proto.Field(proto.STRING, number=84, optional=True,)
    hotel_date_selection_type = proto.Field(
        proto.ENUM,
        number=13,
        enum=gage_hotel_date_selection_type.HotelDateSelectionTypeEnum.HotelDateSelectionType,
    )
    hotel_length_of_stay = proto.Field(proto.INT32, number=85, optional=True,)
    hotel_rate_rule_id = proto.Field(proto.STRING, number=86, optional=True,)
    hotel_rate_type = proto.Field(
        proto.ENUM,
        number=74,
        enum=gage_hotel_rate_type.HotelRateTypeEnum.HotelRateType,
    )
    hotel_price_bucket = proto.Field(
        proto.ENUM,
        number=78,
        enum=gage_hotel_price_bucket.HotelPriceBucketEnum.HotelPriceBucket,
    )
    hotel_state = proto.Field(proto.STRING, number=87, optional=True,)
    hour = proto.Field(proto.INT32, number=88, optional=True,)
    interaction_on_this_extension = proto.Field(
        proto.BOOL, number=89, optional=True,
    )
    keyword = proto.Field(proto.MESSAGE, number=61, message="Keyword",)
    month = proto.Field(proto.STRING, number=90, optional=True,)
    month_of_year = proto.Field(
        proto.ENUM,
        number=18,
        enum=gage_month_of_year.MonthOfYearEnum.MonthOfYear,
    )
    partner_hotel_id = proto.Field(proto.STRING, number=91, optional=True,)
    placeholder_type = proto.Field(
        proto.ENUM,
        number=20,
        enum=gage_placeholder_type.PlaceholderTypeEnum.PlaceholderType,
    )
    product_aggregator_id = proto.Field(proto.INT64, number=132, optional=True,)
    product_bidding_category_level1 = proto.Field(
        proto.STRING, number=92, optional=True,
    )
    product_bidding_category_level2 = proto.Field(
        proto.STRING, number=93, optional=True,
    )
    product_bidding_category_level3 = proto.Field(
        proto.STRING, number=94, optional=True,
    )
    product_bidding_category_level4 = proto.Field(
        proto.STRING, number=95, optional=True,
    )
    product_bidding_category_level5 = proto.Field(
        proto.STRING, number=96, optional=True,
    )
    product_brand = proto.Field(proto.STRING, number=97, optional=True,)
    product_channel = proto.Field(
        proto.ENUM,
        number=30,
        enum=gage_product_channel.ProductChannelEnum.ProductChannel,
    )
    product_channel_exclusivity = proto.Field(
        proto.ENUM,
        number=31,
        enum=gage_product_channel_exclusivity.ProductChannelExclusivityEnum.ProductChannelExclusivity,
    )
    product_condition = proto.Field(
        proto.ENUM,
        number=32,
        enum=gage_product_condition.ProductConditionEnum.ProductCondition,
    )
    product_country = proto.Field(proto.STRING, number=98, optional=True,)
    product_custom_attribute0 = proto.Field(
        proto.STRING, number=99, optional=True,
    )
    product_custom_attribute1 = proto.Field(
        proto.STRING, number=100, optional=True,
    )
    product_custom_attribute2 = proto.Field(
        proto.STRING, number=101, optional=True,
    )
    product_custom_attribute3 = proto.Field(
        proto.STRING, number=102, optional=True,
    )
    product_custom_attribute4 = proto.Field(
        proto.STRING, number=103, optional=True,
    )
    product_item_id = proto.Field(proto.STRING, number=104, optional=True,)
    product_language = proto.Field(proto.STRING, number=105, optional=True,)
    product_merchant_id = proto.Field(proto.INT64, number=133, optional=True,)
    product_store_id = proto.Field(proto.STRING, number=106, optional=True,)
    product_title = proto.Field(proto.STRING, number=107, optional=True,)
    product_type_l1 = proto.Field(proto.STRING, number=108, optional=True,)
    product_type_l2 = proto.Field(proto.STRING, number=109, optional=True,)
    product_type_l3 = proto.Field(proto.STRING, number=110, optional=True,)
    product_type_l4 = proto.Field(proto.STRING, number=111, optional=True,)
    product_type_l5 = proto.Field(proto.STRING, number=112, optional=True,)
    quarter = proto.Field(proto.STRING, number=128, optional=True,)
    recommendation_type = proto.Field(
        proto.ENUM,
        number=140,
        enum=gage_recommendation_type.RecommendationTypeEnum.RecommendationType,
    )
    search_engine_results_page_type = proto.Field(
        proto.ENUM,
        number=70,
        enum=gage_search_engine_results_page_type.SearchEngineResultsPageTypeEnum.SearchEngineResultsPageType,
    )
    search_term_match_type = proto.Field(
        proto.ENUM,
        number=22,
        enum=gage_search_term_match_type.SearchTermMatchTypeEnum.SearchTermMatchType,
    )
    slot = proto.Field(proto.ENUM, number=23, enum=gage_slot.SlotEnum.Slot,)
    conversion_value_rule_primary_dimension = proto.Field(
        proto.ENUM,
        number=138,
        enum=gage_conversion_value_rule_primary_dimension.ConversionValueRulePrimaryDimensionEnum.ConversionValueRulePrimaryDimension,
    )
    webpage = proto.Field(proto.STRING, number=129, optional=True,)
    week = proto.Field(proto.STRING, number=130, optional=True,)
    year = proto.Field(proto.INT32, number=131, optional=True,)
    sk_ad_network_conversion_value = proto.Field(
        proto.INT64, number=137, optional=True,
    )
    sk_ad_network_user_type = proto.Field(
        proto.ENUM,
        number=141,
        enum=gage_sk_ad_network_user_type.SkAdNetworkUserTypeEnum.SkAdNetworkUserType,
    )
    sk_ad_network_ad_event_type = proto.Field(
        proto.ENUM,
        number=142,
        enum=gage_sk_ad_network_ad_event_type.SkAdNetworkAdEventTypeEnum.SkAdNetworkAdEventType,
    )
    sk_ad_network_source_app = proto.Field(
        proto.MESSAGE,
        number=143,
        optional=True,
        message="SkAdNetworkSourceApp",
    )
    sk_ad_network_attribution_credit = proto.Field(
        proto.ENUM,
        number=144,
        enum=gage_sk_ad_network_attribution_credit.SkAdNetworkAttributionCreditEnum.SkAdNetworkAttributionCredit,
    )
    asset_interaction_target = proto.Field(
        proto.MESSAGE,
        number=139,
        optional=True,
        message="AssetInteractionTarget",
    )


class Keyword(proto.Message):
    r"""A Keyword criterion segment.

    Attributes:
        ad_group_criterion (str):
            The AdGroupCriterion resource name.

            This field is a member of `oneof`_ ``_ad_group_criterion``.
        info (google.ads.googleads.v12.common.types.KeywordInfo):
            Keyword info.
    """

    ad_group_criterion = proto.Field(proto.STRING, number=3, optional=True,)
    info = proto.Field(proto.MESSAGE, number=2, message=criteria.KeywordInfo,)


class BudgetCampaignAssociationStatus(proto.Message):
    r"""A BudgetCampaignAssociationStatus segment.

    Attributes:
        campaign (str):
            The campaign resource name.

            This field is a member of `oneof`_ ``_campaign``.
        status (google.ads.googleads.v12.enums.types.BudgetCampaignAssociationStatusEnum.BudgetCampaignAssociationStatus):
            Budget campaign association status.
    """

    campaign = proto.Field(proto.STRING, number=1, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_budget_campaign_association_status.BudgetCampaignAssociationStatusEnum.BudgetCampaignAssociationStatus,
    )


class AssetInteractionTarget(proto.Message):
    r"""An AssetInteractionTarget segment.

    Attributes:
        asset (str):
            The asset resource name.
        interaction_on_this_asset (bool):
            Only used with CustomerAsset, CampaignAsset
            and AdGroupAsset metrics. Indicates whether the
            interaction metrics occurred on the asset itself
            or a different asset or ad unit.
    """

    asset = proto.Field(proto.STRING, number=1,)
    interaction_on_this_asset = proto.Field(proto.BOOL, number=2,)


class SkAdNetworkSourceApp(proto.Message):
    r"""A SkAdNetworkSourceApp segment.

    Attributes:
        sk_ad_network_source_app_id (str):
            App id where the ad that drove the iOS Store
            Kit Ad Network install was shown.

            This field is a member of `oneof`_ ``_sk_ad_network_source_app_id``.
    """

    sk_ad_network_source_app_id = proto.Field(
        proto.STRING, number=1, optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
