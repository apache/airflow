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

from airflow.providers.google_vendor.googleads.v12.common.types import metrics as gagc_metrics
from airflow.providers.google_vendor.googleads.v12.common.types import segments as gagc_segments
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    response_content_type as gage_response_content_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    summary_row_setting as gage_summary_row_setting,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    accessible_bidding_strategy as gagr_accessible_bidding_strategy,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    account_budget as gagr_account_budget,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    account_budget_proposal as gagr_account_budget_proposal,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    account_link as gagr_account_link,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import ad_group as gagr_ad_group
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_ad as gagr_ad_group_ad,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_ad_asset_combination_view as gagr_ad_group_ad_asset_combination_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_ad_asset_view as gagr_ad_group_ad_asset_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_ad_label as gagr_ad_group_ad_label,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_asset as gagr_ad_group_asset,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_asset_set as gagr_ad_group_asset_set,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_audience_view as gagr_ad_group_audience_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_bid_modifier as gagr_ad_group_bid_modifier,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_criterion as gagr_ad_group_criterion,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_criterion_customizer as gagr_ad_group_criterion_customizer,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_criterion_label as gagr_ad_group_criterion_label,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_criterion_simulation as gagr_ad_group_criterion_simulation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_customizer as gagr_ad_group_customizer,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_extension_setting as gagr_ad_group_extension_setting,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_feed as gagr_ad_group_feed,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_label as gagr_ad_group_label,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_group_simulation as gagr_ad_group_simulation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_parameter as gagr_ad_parameter,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    ad_schedule_view as gagr_ad_schedule_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    age_range_view as gagr_age_range_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import asset as gagr_asset
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_field_type_view as gagr_asset_field_type_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_group as gagr_asset_group,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_group_asset as gagr_asset_group_asset,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_group_listing_group_filter as gagr_asset_group_listing_group_filter,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_group_product_group_view as gagr_asset_group_product_group_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_group_signal as gagr_asset_group_signal,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import asset_set as gagr_asset_set
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_set_asset as gagr_asset_set_asset,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    asset_set_type_view as gagr_asset_set_type_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import audience as gagr_audience
from airflow.providers.google_vendor.googleads.v12.resources.types import batch_job as gagr_batch_job
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    bidding_data_exclusion as gagr_bidding_data_exclusion,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    bidding_seasonality_adjustment as gagr_bidding_seasonality_adjustment,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    bidding_strategy as gagr_bidding_strategy,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    bidding_strategy_simulation as gagr_bidding_strategy_simulation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    billing_setup as gagr_billing_setup,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import call_view as gagr_call_view
from airflow.providers.google_vendor.googleads.v12.resources.types import campaign as gagr_campaign
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_asset as gagr_campaign_asset,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_asset_set as gagr_campaign_asset_set,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_audience_view as gagr_campaign_audience_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_bid_modifier as gagr_campaign_bid_modifier,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_budget as gagr_campaign_budget,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_conversion_goal as gagr_campaign_conversion_goal,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_criterion as gagr_campaign_criterion,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_criterion_simulation as gagr_campaign_criterion_simulation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_customizer as gagr_campaign_customizer,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_draft as gagr_campaign_draft,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_extension_setting as gagr_campaign_extension_setting,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_feed as gagr_campaign_feed,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_group as gagr_campaign_group,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_label as gagr_campaign_label,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_shared_set as gagr_campaign_shared_set,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    campaign_simulation as gagr_campaign_simulation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    carrier_constant as gagr_carrier_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    change_event as gagr_change_event,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    change_status as gagr_change_status,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    click_view as gagr_click_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    combined_audience as gagr_combined_audience,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    conversion_action as gagr_conversion_action,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    conversion_custom_variable as gagr_conversion_custom_variable,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    conversion_goal_campaign_config as gagr_conversion_goal_campaign_config,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    conversion_value_rule as gagr_conversion_value_rule,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    conversion_value_rule_set as gagr_conversion_value_rule_set,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    currency_constant as gagr_currency_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    custom_audience as gagr_custom_audience,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    custom_conversion_goal as gagr_custom_conversion_goal,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    custom_interest as gagr_custom_interest,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import customer as gagr_customer
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_asset as gagr_customer_asset,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_asset_set as gagr_customer_asset_set,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_client as gagr_customer_client,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_client_link as gagr_customer_client_link,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_conversion_goal as gagr_customer_conversion_goal,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_customizer as gagr_customer_customizer,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_extension_setting as gagr_customer_extension_setting,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_feed as gagr_customer_feed,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_label as gagr_customer_label,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_manager_link as gagr_customer_manager_link,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_negative_criterion as gagr_customer_negative_criterion,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_user_access as gagr_customer_user_access,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customer_user_access_invitation as gagr_customer_user_access_invitation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    customizer_attribute as gagr_customizer_attribute,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    detail_placement_view as gagr_detail_placement_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    detailed_demographic as gagr_detailed_demographic,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    display_keyword_view as gagr_display_keyword_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    distance_view as gagr_distance_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    domain_category as gagr_domain_category,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    dynamic_search_ads_search_term_view as gagr_dynamic_search_ads_search_term_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    expanded_landing_page_view as gagr_expanded_landing_page_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    experiment as gagr_experiment,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    experiment_arm as gagr_experiment_arm,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    extension_feed_item as gagr_extension_feed_item,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import feed as gagr_feed
from airflow.providers.google_vendor.googleads.v12.resources.types import feed_item as gagr_feed_item
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    feed_item_set as gagr_feed_item_set,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    feed_item_set_link as gagr_feed_item_set_link,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    feed_item_target as gagr_feed_item_target,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    feed_mapping as gagr_feed_mapping,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    feed_placeholder_view as gagr_feed_placeholder_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    gender_view as gagr_gender_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    geo_target_constant as gagr_geo_target_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    geographic_view as gagr_geographic_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    group_placement_view as gagr_group_placement_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    hotel_group_view as gagr_hotel_group_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    hotel_performance_view as gagr_hotel_performance_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    hotel_reconciliation as gagr_hotel_reconciliation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    income_range_view as gagr_income_range_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_plan as gagr_keyword_plan,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_plan_ad_group as gagr_keyword_plan_ad_group,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_plan_ad_group_keyword as gagr_keyword_plan_ad_group_keyword,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_plan_campaign as gagr_keyword_plan_campaign,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_plan_campaign_keyword as gagr_keyword_plan_campaign_keyword,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_theme_constant as gagr_keyword_theme_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_view as gagr_keyword_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import label as gagr_label
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    landing_page_view as gagr_landing_page_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    language_constant as gagr_language_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    lead_form_submission_data as gagr_lead_form_submission_data,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    life_event as gagr_life_event,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    location_view as gagr_location_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    managed_placement_view as gagr_managed_placement_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    media_file as gagr_media_file,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    mobile_app_category_constant as gagr_mobile_app_category_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    mobile_device_constant as gagr_mobile_device_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    offline_user_data_job as gagr_offline_user_data_job,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    operating_system_version_constant as gagr_operating_system_version_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    paid_organic_search_term_view as gagr_paid_organic_search_term_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    parental_status_view as gagr_parental_status_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    per_store_view as gagr_per_store_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    product_bidding_category_constant as gagr_product_bidding_category_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    product_group_view as gagr_product_group_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    recommendation as gagr_recommendation,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    remarketing_action as gagr_remarketing_action,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    search_term_view as gagr_search_term_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    shared_criterion as gagr_shared_criterion,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    shared_set as gagr_shared_set,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    shopping_performance_view as gagr_shopping_performance_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    smart_campaign_search_term_view as gagr_smart_campaign_search_term_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    smart_campaign_setting as gagr_smart_campaign_setting,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    third_party_app_analytics_link as gagr_third_party_app_analytics_link,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    topic_constant as gagr_topic_constant,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    topic_view as gagr_topic_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    user_interest as gagr_user_interest,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import user_list as gagr_user_list
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    user_location_view as gagr_user_location_view,
)
from airflow.providers.google_vendor.googleads.v12.resources.types import video as gagr_video
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    webpage_view as gagr_webpage_view,
)
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_ad_label_service
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_ad_service
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_asset_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    ad_group_bid_modifier_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    ad_group_criterion_customizer_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    ad_group_criterion_label_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_criterion_service
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_customizer_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    ad_group_extension_setting_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_feed_service
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_label_service
from airflow.providers.google_vendor.googleads.v12.services.types import ad_group_service
from airflow.providers.google_vendor.googleads.v12.services.types import ad_parameter_service
from airflow.providers.google_vendor.googleads.v12.services.types import ad_service
from airflow.providers.google_vendor.googleads.v12.services.types import asset_group_asset_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    asset_group_listing_group_filter_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import asset_group_service
from airflow.providers.google_vendor.googleads.v12.services.types import asset_group_signal_service
from airflow.providers.google_vendor.googleads.v12.services.types import asset_service
from airflow.providers.google_vendor.googleads.v12.services.types import asset_set_asset_service
from airflow.providers.google_vendor.googleads.v12.services.types import asset_set_service
from airflow.providers.google_vendor.googleads.v12.services.types import audience_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    bidding_data_exclusion_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    bidding_seasonality_adjustment_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import bidding_strategy_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_asset_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_asset_set_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    campaign_bid_modifier_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_budget_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    campaign_conversion_goal_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_criterion_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_customizer_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_draft_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    campaign_extension_setting_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_feed_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_group_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_label_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_service
from airflow.providers.google_vendor.googleads.v12.services.types import campaign_shared_set_service
from airflow.providers.google_vendor.googleads.v12.services.types import conversion_action_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    conversion_custom_variable_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    conversion_goal_campaign_config_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    conversion_value_rule_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    conversion_value_rule_set_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    custom_conversion_goal_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import customer_asset_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    customer_conversion_goal_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import customer_customizer_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    customer_extension_setting_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import customer_feed_service
from airflow.providers.google_vendor.googleads.v12.services.types import customer_label_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    customer_negative_criterion_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import customer_service
from airflow.providers.google_vendor.googleads.v12.services.types import customizer_attribute_service
from airflow.providers.google_vendor.googleads.v12.services.types import experiment_arm_service
from airflow.providers.google_vendor.googleads.v12.services.types import experiment_service
from airflow.providers.google_vendor.googleads.v12.services.types import extension_feed_item_service
from airflow.providers.google_vendor.googleads.v12.services.types import feed_item_service
from airflow.providers.google_vendor.googleads.v12.services.types import feed_item_set_link_service
from airflow.providers.google_vendor.googleads.v12.services.types import feed_item_set_service
from airflow.providers.google_vendor.googleads.v12.services.types import feed_item_target_service
from airflow.providers.google_vendor.googleads.v12.services.types import feed_mapping_service
from airflow.providers.google_vendor.googleads.v12.services.types import feed_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    keyword_plan_ad_group_keyword_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    keyword_plan_ad_group_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    keyword_plan_campaign_keyword_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import (
    keyword_plan_campaign_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import keyword_plan_service
from airflow.providers.google_vendor.googleads.v12.services.types import label_service
from airflow.providers.google_vendor.googleads.v12.services.types import media_file_service
from airflow.providers.google_vendor.googleads.v12.services.types import remarketing_action_service
from airflow.providers.google_vendor.googleads.v12.services.types import shared_criterion_service
from airflow.providers.google_vendor.googleads.v12.services.types import shared_set_service
from airflow.providers.google_vendor.googleads.v12.services.types import (
    smart_campaign_setting_service,
)
from airflow.providers.google_vendor.googleads.v12.services.types import user_list_service
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "SearchGoogleAdsRequest",
        "SearchGoogleAdsResponse",
        "SearchGoogleAdsStreamRequest",
        "SearchGoogleAdsStreamResponse",
        "GoogleAdsRow",
        "MutateGoogleAdsRequest",
        "MutateGoogleAdsResponse",
        "MutateOperation",
        "MutateOperationResponse",
    },
)


class SearchGoogleAdsRequest(proto.Message):
    r"""Request message for
    [GoogleAdsService.Search][google.ads.googleads.v12.services.GoogleAdsService.Search].

    Attributes:
        customer_id (str):
            Required. The ID of the customer being
            queried.
        query (str):
            Required. The query string.
        page_token (str):
            Token of the page to retrieve. If not specified, the first
            page of results will be returned. Use the value obtained
            from ``next_page_token`` in the previous response in order
            to request the next page of results.
        page_size (int):
            Number of elements to retrieve in a single
            page. When too large a page is requested, the
            server may decide to further limit the number of
            returned resources.
        validate_only (bool):
            If true, the request is validated but not
            executed.
        return_total_results_count (bool):
            If true, the total number of results that
            match the query ignoring the LIMIT clause will
            be included in the response. Default is false.
        summary_row_setting (google.ads.googleads.v12.enums.types.SummaryRowSettingEnum.SummaryRowSetting):
            Determines whether a summary row will be
            returned. By default, summary row is not
            returned. If requested, the summary row will be
            sent in a response by itself after all other
            query results are returned.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    query = proto.Field(proto.STRING, number=2,)
    page_token = proto.Field(proto.STRING, number=3,)
    page_size = proto.Field(proto.INT32, number=4,)
    validate_only = proto.Field(proto.BOOL, number=5,)
    return_total_results_count = proto.Field(proto.BOOL, number=7,)
    summary_row_setting = proto.Field(
        proto.ENUM,
        number=8,
        enum=gage_summary_row_setting.SummaryRowSettingEnum.SummaryRowSetting,
    )


class SearchGoogleAdsResponse(proto.Message):
    r"""Response message for
    [GoogleAdsService.Search][google.ads.googleads.v12.services.GoogleAdsService.Search].

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.GoogleAdsRow]):
            The list of rows that matched the query.
        next_page_token (str):
            Pagination token used to retrieve the next page of results.
            Pass the content of this string as the ``page_token``
            attribute of the next request. ``next_page_token`` is not
            returned for the last page.
        total_results_count (int):
            Total number of results that match the query
            ignoring the LIMIT clause.
        field_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that represents what fields were
            requested by the user.
        summary_row (google.ads.googleads.v12.services.types.GoogleAdsRow):
            Summary row that contains summary of metrics
            in results. Summary of metrics means aggregation
            of metrics across all results, here aggregation
            could be sum, average, rate, etc.
    """

    @property
    def raw_page(self):
        return self

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="GoogleAdsRow",
    )
    next_page_token = proto.Field(proto.STRING, number=2,)
    total_results_count = proto.Field(proto.INT64, number=3,)
    field_mask = proto.Field(
        proto.MESSAGE, number=5, message=field_mask_pb2.FieldMask,
    )
    summary_row = proto.Field(proto.MESSAGE, number=6, message="GoogleAdsRow",)


class SearchGoogleAdsStreamRequest(proto.Message):
    r"""Request message for
    [GoogleAdsService.SearchStream][google.ads.googleads.v12.services.GoogleAdsService.SearchStream].

    Attributes:
        customer_id (str):
            Required. The ID of the customer being
            queried.
        query (str):
            Required. The query string.
        summary_row_setting (google.ads.googleads.v12.enums.types.SummaryRowSettingEnum.SummaryRowSetting):
            Determines whether a summary row will be
            returned. By default, summary row is not
            returned. If requested, the summary row will be
            sent in a response by itself after all other
            query results are returned.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    query = proto.Field(proto.STRING, number=2,)
    summary_row_setting = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_summary_row_setting.SummaryRowSettingEnum.SummaryRowSetting,
    )


class SearchGoogleAdsStreamResponse(proto.Message):
    r"""Response message for
    [GoogleAdsService.SearchStream][google.ads.googleads.v12.services.GoogleAdsService.SearchStream].

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.GoogleAdsRow]):
            The list of rows that matched the query.
        field_mask (google.protobuf.field_mask_pb2.FieldMask):
            FieldMask that represents what fields were
            requested by the user.
        summary_row (google.ads.googleads.v12.services.types.GoogleAdsRow):
            Summary row that contains summary of metrics
            in results. Summary of metrics means aggregation
            of metrics across all results, here aggregation
            could be sum, average, rate, etc.
        request_id (str):
            The unique id of the request that is used for
            debugging purposes.
    """

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="GoogleAdsRow",
    )
    field_mask = proto.Field(
        proto.MESSAGE, number=2, message=field_mask_pb2.FieldMask,
    )
    summary_row = proto.Field(proto.MESSAGE, number=3, message="GoogleAdsRow",)
    request_id = proto.Field(proto.STRING, number=4,)


class GoogleAdsRow(proto.Message):
    r"""A returned row from the query.

    Attributes:
        account_budget (google.ads.googleads.v12.resources.types.AccountBudget):
            The account budget in the query.
        account_budget_proposal (google.ads.googleads.v12.resources.types.AccountBudgetProposal):
            The account budget proposal referenced in the
            query.
        account_link (google.ads.googleads.v12.resources.types.AccountLink):
            The AccountLink referenced in the query.
        ad_group (google.ads.googleads.v12.resources.types.AdGroup):
            The ad group referenced in the query.
        ad_group_ad (google.ads.googleads.v12.resources.types.AdGroupAd):
            The ad referenced in the query.
        ad_group_ad_asset_combination_view (google.ads.googleads.v12.resources.types.AdGroupAdAssetCombinationView):
            The ad group ad asset combination view in the
            query.
        ad_group_ad_asset_view (google.ads.googleads.v12.resources.types.AdGroupAdAssetView):
            The ad group ad asset view in the query.
        ad_group_ad_label (google.ads.googleads.v12.resources.types.AdGroupAdLabel):
            The ad group ad label referenced in the
            query.
        ad_group_asset (google.ads.googleads.v12.resources.types.AdGroupAsset):
            The ad group asset referenced in the query.
        ad_group_asset_set (google.ads.googleads.v12.resources.types.AdGroupAssetSet):
            The ad group asset set referenced in the
            query.
        ad_group_audience_view (google.ads.googleads.v12.resources.types.AdGroupAudienceView):
            The ad group audience view referenced in the
            query.
        ad_group_bid_modifier (google.ads.googleads.v12.resources.types.AdGroupBidModifier):
            The bid modifier referenced in the query.
        ad_group_criterion (google.ads.googleads.v12.resources.types.AdGroupCriterion):
            The criterion referenced in the query.
        ad_group_criterion_customizer (google.ads.googleads.v12.resources.types.AdGroupCriterionCustomizer):
            The ad group criterion customizer referenced
            in the query.
        ad_group_criterion_label (google.ads.googleads.v12.resources.types.AdGroupCriterionLabel):
            The ad group criterion label referenced in
            the query.
        ad_group_criterion_simulation (google.ads.googleads.v12.resources.types.AdGroupCriterionSimulation):
            The ad group criterion simulation referenced
            in the query.
        ad_group_customizer (google.ads.googleads.v12.resources.types.AdGroupCustomizer):
            The ad group customizer referenced in the
            query.
        ad_group_extension_setting (google.ads.googleads.v12.resources.types.AdGroupExtensionSetting):
            The ad group extension setting referenced in
            the query.
        ad_group_feed (google.ads.googleads.v12.resources.types.AdGroupFeed):
            The ad group feed referenced in the query.
        ad_group_label (google.ads.googleads.v12.resources.types.AdGroupLabel):
            The ad group label referenced in the query.
        ad_group_simulation (google.ads.googleads.v12.resources.types.AdGroupSimulation):
            The ad group simulation referenced in the
            query.
        ad_parameter (google.ads.googleads.v12.resources.types.AdParameter):
            The ad parameter referenced in the query.
        age_range_view (google.ads.googleads.v12.resources.types.AgeRangeView):
            The age range view referenced in the query.
        ad_schedule_view (google.ads.googleads.v12.resources.types.AdScheduleView):
            The ad schedule view referenced in the query.
        domain_category (google.ads.googleads.v12.resources.types.DomainCategory):
            The domain category referenced in the query.
        asset (google.ads.googleads.v12.resources.types.Asset):
            The asset referenced in the query.
        asset_field_type_view (google.ads.googleads.v12.resources.types.AssetFieldTypeView):
            The asset field type view referenced in the
            query.
        asset_group_asset (google.ads.googleads.v12.resources.types.AssetGroupAsset):
            The asset group asset referenced in the
            query.
        asset_group_signal (google.ads.googleads.v12.resources.types.AssetGroupSignal):
            The asset group signal referenced in the
            query.
        asset_group_listing_group_filter (google.ads.googleads.v12.resources.types.AssetGroupListingGroupFilter):
            The asset group listing group filter
            referenced in the query.
        asset_group_product_group_view (google.ads.googleads.v12.resources.types.AssetGroupProductGroupView):
            The asset group product group view referenced
            in the query.
        asset_group (google.ads.googleads.v12.resources.types.AssetGroup):
            The asset group referenced in the query.
        asset_set_asset (google.ads.googleads.v12.resources.types.AssetSetAsset):
            The asset set asset referenced in the query.
        asset_set (google.ads.googleads.v12.resources.types.AssetSet):
            The asset set referenced in the query.
        asset_set_type_view (google.ads.googleads.v12.resources.types.AssetSetTypeView):
            The asset set type view referenced in the
            query.
        batch_job (google.ads.googleads.v12.resources.types.BatchJob):
            The batch job referenced in the query.
        bidding_data_exclusion (google.ads.googleads.v12.resources.types.BiddingDataExclusion):
            The bidding data exclusion referenced in the
            query.
        bidding_seasonality_adjustment (google.ads.googleads.v12.resources.types.BiddingSeasonalityAdjustment):
            The bidding seasonality adjustment referenced
            in the query.
        bidding_strategy (google.ads.googleads.v12.resources.types.BiddingStrategy):
            The bidding strategy referenced in the query.
        bidding_strategy_simulation (google.ads.googleads.v12.resources.types.BiddingStrategySimulation):
            The bidding strategy simulation referenced in
            the query.
        billing_setup (google.ads.googleads.v12.resources.types.BillingSetup):
            The billing setup referenced in the query.
        call_view (google.ads.googleads.v12.resources.types.CallView):
            The call view referenced in the query.
        campaign_budget (google.ads.googleads.v12.resources.types.CampaignBudget):
            The campaign budget referenced in the query.
        campaign (google.ads.googleads.v12.resources.types.Campaign):
            The campaign referenced in the query.
        campaign_asset (google.ads.googleads.v12.resources.types.CampaignAsset):
            The campaign asset referenced in the query.
        campaign_asset_set (google.ads.googleads.v12.resources.types.CampaignAssetSet):
            The campaign asset set referenced in the
            query.
        campaign_audience_view (google.ads.googleads.v12.resources.types.CampaignAudienceView):
            The campaign audience view referenced in the
            query.
        campaign_bid_modifier (google.ads.googleads.v12.resources.types.CampaignBidModifier):
            The campaign bid modifier referenced in the
            query.
        campaign_conversion_goal (google.ads.googleads.v12.resources.types.CampaignConversionGoal):
            The CampaignConversionGoal referenced in the
            query.
        campaign_criterion (google.ads.googleads.v12.resources.types.CampaignCriterion):
            The campaign criterion referenced in the
            query.
        campaign_criterion_simulation (google.ads.googleads.v12.resources.types.CampaignCriterionSimulation):
            The campaign criterion simulation referenced
            in the query.
        campaign_customizer (google.ads.googleads.v12.resources.types.CampaignCustomizer):
            The campaign customizer referenced in the
            query.
        campaign_draft (google.ads.googleads.v12.resources.types.CampaignDraft):
            The campaign draft referenced in the query.
        campaign_extension_setting (google.ads.googleads.v12.resources.types.CampaignExtensionSetting):
            The campaign extension setting referenced in
            the query.
        campaign_feed (google.ads.googleads.v12.resources.types.CampaignFeed):
            The campaign feed referenced in the query.
        campaign_group (google.ads.googleads.v12.resources.types.CampaignGroup):
            Campaign Group referenced in AWQL query.
        campaign_label (google.ads.googleads.v12.resources.types.CampaignLabel):
            The campaign label referenced in the query.
        campaign_shared_set (google.ads.googleads.v12.resources.types.CampaignSharedSet):
            Campaign Shared Set referenced in AWQL query.
        campaign_simulation (google.ads.googleads.v12.resources.types.CampaignSimulation):
            The campaign simulation referenced in the
            query.
        carrier_constant (google.ads.googleads.v12.resources.types.CarrierConstant):
            The carrier constant referenced in the query.
        change_event (google.ads.googleads.v12.resources.types.ChangeEvent):
            The ChangeEvent referenced in the query.
        change_status (google.ads.googleads.v12.resources.types.ChangeStatus):
            The ChangeStatus referenced in the query.
        combined_audience (google.ads.googleads.v12.resources.types.CombinedAudience):
            The CombinedAudience referenced in the query.
        audience (google.ads.googleads.v12.resources.types.Audience):
            The Audience referenced in the query.
        conversion_action (google.ads.googleads.v12.resources.types.ConversionAction):
            The conversion action referenced in the
            query.
        conversion_custom_variable (google.ads.googleads.v12.resources.types.ConversionCustomVariable):
            The conversion custom variable referenced in
            the query.
        conversion_goal_campaign_config (google.ads.googleads.v12.resources.types.ConversionGoalCampaignConfig):
            The ConversionGoalCampaignConfig referenced
            in the query.
        conversion_value_rule (google.ads.googleads.v12.resources.types.ConversionValueRule):
            The conversion value rule referenced in the
            query.
        conversion_value_rule_set (google.ads.googleads.v12.resources.types.ConversionValueRuleSet):
            The conversion value rule set referenced in
            the query.
        click_view (google.ads.googleads.v12.resources.types.ClickView):
            The ClickView referenced in the query.
        currency_constant (google.ads.googleads.v12.resources.types.CurrencyConstant):
            The currency constant referenced in the
            query.
        custom_audience (google.ads.googleads.v12.resources.types.CustomAudience):
            The CustomAudience referenced in the query.
        custom_conversion_goal (google.ads.googleads.v12.resources.types.CustomConversionGoal):
            The CustomConversionGoal referenced in the
            query.
        custom_interest (google.ads.googleads.v12.resources.types.CustomInterest):
            The CustomInterest referenced in the query.
        customer (google.ads.googleads.v12.resources.types.Customer):
            The customer referenced in the query.
        customer_asset (google.ads.googleads.v12.resources.types.CustomerAsset):
            The customer asset referenced in the query.
        customer_asset_set (google.ads.googleads.v12.resources.types.CustomerAssetSet):
            The customer asset set referenced in the
            query.
        accessible_bidding_strategy (google.ads.googleads.v12.resources.types.AccessibleBiddingStrategy):
            The accessible bidding strategy referenced in
            the query.
        customer_customizer (google.ads.googleads.v12.resources.types.CustomerCustomizer):
            The customer customizer referenced in the
            query.
        customer_manager_link (google.ads.googleads.v12.resources.types.CustomerManagerLink):
            The CustomerManagerLink referenced in the
            query.
        customer_client_link (google.ads.googleads.v12.resources.types.CustomerClientLink):
            The CustomerClientLink referenced in the
            query.
        customer_client (google.ads.googleads.v12.resources.types.CustomerClient):
            The CustomerClient referenced in the query.
        customer_conversion_goal (google.ads.googleads.v12.resources.types.CustomerConversionGoal):
            The CustomerConversionGoal referenced in the
            query.
        customer_extension_setting (google.ads.googleads.v12.resources.types.CustomerExtensionSetting):
            The customer extension setting referenced in
            the query.
        customer_feed (google.ads.googleads.v12.resources.types.CustomerFeed):
            The customer feed referenced in the query.
        customer_label (google.ads.googleads.v12.resources.types.CustomerLabel):
            The customer label referenced in the query.
        customer_negative_criterion (google.ads.googleads.v12.resources.types.CustomerNegativeCriterion):
            The customer negative criterion referenced in
            the query.
        customer_user_access (google.ads.googleads.v12.resources.types.CustomerUserAccess):
            The CustomerUserAccess referenced in the
            query.
        customer_user_access_invitation (google.ads.googleads.v12.resources.types.CustomerUserAccessInvitation):
            The CustomerUserAccessInvitation referenced
            in the query.
        customizer_attribute (google.ads.googleads.v12.resources.types.CustomizerAttribute):
            The customizer attribute referenced in the
            query.
        detail_placement_view (google.ads.googleads.v12.resources.types.DetailPlacementView):
            The detail placement view referenced in the
            query.
        detailed_demographic (google.ads.googleads.v12.resources.types.DetailedDemographic):
            The detailed demographic referenced in the
            query.
        display_keyword_view (google.ads.googleads.v12.resources.types.DisplayKeywordView):
            The display keyword view referenced in the
            query.
        distance_view (google.ads.googleads.v12.resources.types.DistanceView):
            The distance view referenced in the query.
        dynamic_search_ads_search_term_view (google.ads.googleads.v12.resources.types.DynamicSearchAdsSearchTermView):
            The dynamic search ads search term view
            referenced in the query.
        expanded_landing_page_view (google.ads.googleads.v12.resources.types.ExpandedLandingPageView):
            The expanded landing page view referenced in
            the query.
        extension_feed_item (google.ads.googleads.v12.resources.types.ExtensionFeedItem):
            The extension feed item referenced in the
            query.
        feed (google.ads.googleads.v12.resources.types.Feed):
            The feed referenced in the query.
        feed_item (google.ads.googleads.v12.resources.types.FeedItem):
            The feed item referenced in the query.
        feed_item_set (google.ads.googleads.v12.resources.types.FeedItemSet):
            The feed item set referenced in the query.
        feed_item_set_link (google.ads.googleads.v12.resources.types.FeedItemSetLink):
            The feed item set link referenced in the
            query.
        feed_item_target (google.ads.googleads.v12.resources.types.FeedItemTarget):
            The feed item target referenced in the query.
        feed_mapping (google.ads.googleads.v12.resources.types.FeedMapping):
            The feed mapping referenced in the query.
        feed_placeholder_view (google.ads.googleads.v12.resources.types.FeedPlaceholderView):
            The feed placeholder view referenced in the
            query.
        gender_view (google.ads.googleads.v12.resources.types.GenderView):
            The gender view referenced in the query.
        geo_target_constant (google.ads.googleads.v12.resources.types.GeoTargetConstant):
            The geo target constant referenced in the
            query.
        geographic_view (google.ads.googleads.v12.resources.types.GeographicView):
            The geographic view referenced in the query.
        group_placement_view (google.ads.googleads.v12.resources.types.GroupPlacementView):
            The group placement view referenced in the
            query.
        hotel_group_view (google.ads.googleads.v12.resources.types.HotelGroupView):
            The hotel group view referenced in the query.
        hotel_performance_view (google.ads.googleads.v12.resources.types.HotelPerformanceView):
            The hotel performance view referenced in the
            query.
        hotel_reconciliation (google.ads.googleads.v12.resources.types.HotelReconciliation):
            The hotel reconciliation referenced in the
            query.
        income_range_view (google.ads.googleads.v12.resources.types.IncomeRangeView):
            The income range view referenced in the
            query.
        keyword_view (google.ads.googleads.v12.resources.types.KeywordView):
            The keyword view referenced in the query.
        keyword_plan (google.ads.googleads.v12.resources.types.KeywordPlan):
            The keyword plan referenced in the query.
        keyword_plan_campaign (google.ads.googleads.v12.resources.types.KeywordPlanCampaign):
            The keyword plan campaign referenced in the
            query.
        keyword_plan_campaign_keyword (google.ads.googleads.v12.resources.types.KeywordPlanCampaignKeyword):
            The keyword plan campaign keyword referenced
            in the query.
        keyword_plan_ad_group (google.ads.googleads.v12.resources.types.KeywordPlanAdGroup):
            The keyword plan ad group referenced in the
            query.
        keyword_plan_ad_group_keyword (google.ads.googleads.v12.resources.types.KeywordPlanAdGroupKeyword):
            The keyword plan ad group referenced in the
            query.
        keyword_theme_constant (google.ads.googleads.v12.resources.types.KeywordThemeConstant):
            The keyword theme constant referenced in the
            query.
        label (google.ads.googleads.v12.resources.types.Label):
            The label referenced in the query.
        landing_page_view (google.ads.googleads.v12.resources.types.LandingPageView):
            The landing page view referenced in the
            query.
        language_constant (google.ads.googleads.v12.resources.types.LanguageConstant):
            The language constant referenced in the
            query.
        location_view (google.ads.googleads.v12.resources.types.LocationView):
            The location view referenced in the query.
        managed_placement_view (google.ads.googleads.v12.resources.types.ManagedPlacementView):
            The managed placement view referenced in the
            query.
        media_file (google.ads.googleads.v12.resources.types.MediaFile):
            The media file referenced in the query.
        mobile_app_category_constant (google.ads.googleads.v12.resources.types.MobileAppCategoryConstant):
            The mobile app category constant referenced
            in the query.
        mobile_device_constant (google.ads.googleads.v12.resources.types.MobileDeviceConstant):
            The mobile device constant referenced in the
            query.
        offline_user_data_job (google.ads.googleads.v12.resources.types.OfflineUserDataJob):
            The offline user data job referenced in the
            query.
        operating_system_version_constant (google.ads.googleads.v12.resources.types.OperatingSystemVersionConstant):
            The operating system version constant
            referenced in the query.
        paid_organic_search_term_view (google.ads.googleads.v12.resources.types.PaidOrganicSearchTermView):
            The paid organic search term view referenced
            in the query.
        parental_status_view (google.ads.googleads.v12.resources.types.ParentalStatusView):
            The parental status view referenced in the
            query.
        per_store_view (google.ads.googleads.v12.resources.types.PerStoreView):
            The per store view referenced in the query.
        product_bidding_category_constant (google.ads.googleads.v12.resources.types.ProductBiddingCategoryConstant):
            The Product Bidding Category referenced in
            the query.
        product_group_view (google.ads.googleads.v12.resources.types.ProductGroupView):
            The product group view referenced in the
            query.
        recommendation (google.ads.googleads.v12.resources.types.Recommendation):
            The recommendation referenced in the query.
        search_term_view (google.ads.googleads.v12.resources.types.SearchTermView):
            The search term view referenced in the query.
        shared_criterion (google.ads.googleads.v12.resources.types.SharedCriterion):
            The shared set referenced in the query.
        shared_set (google.ads.googleads.v12.resources.types.SharedSet):
            The shared set referenced in the query.
        smart_campaign_setting (google.ads.googleads.v12.resources.types.SmartCampaignSetting):
            The Smart campaign setting referenced in the
            query.
        shopping_performance_view (google.ads.googleads.v12.resources.types.ShoppingPerformanceView):
            The shopping performance view referenced in
            the query.
        smart_campaign_search_term_view (google.ads.googleads.v12.resources.types.SmartCampaignSearchTermView):
            The Smart campaign search term view
            referenced in the query.
        third_party_app_analytics_link (google.ads.googleads.v12.resources.types.ThirdPartyAppAnalyticsLink):
            The AccountLink referenced in the query.
        topic_view (google.ads.googleads.v12.resources.types.TopicView):
            The topic view referenced in the query.
        experiment (google.ads.googleads.v12.resources.types.Experiment):
            The experiment referenced in the query.
        experiment_arm (google.ads.googleads.v12.resources.types.ExperimentArm):
            The experiment arm referenced in the query.
        user_interest (google.ads.googleads.v12.resources.types.UserInterest):
            The user interest referenced in the query.
        life_event (google.ads.googleads.v12.resources.types.LifeEvent):
            The life event referenced in the query.
        user_list (google.ads.googleads.v12.resources.types.UserList):
            The user list referenced in the query.
        user_location_view (google.ads.googleads.v12.resources.types.UserLocationView):
            The user location view referenced in the
            query.
        remarketing_action (google.ads.googleads.v12.resources.types.RemarketingAction):
            The remarketing action referenced in the
            query.
        topic_constant (google.ads.googleads.v12.resources.types.TopicConstant):
            The topic constant referenced in the query.
        video (google.ads.googleads.v12.resources.types.Video):
            The video referenced in the query.
        webpage_view (google.ads.googleads.v12.resources.types.WebpageView):
            The webpage view referenced in the query.
        lead_form_submission_data (google.ads.googleads.v12.resources.types.LeadFormSubmissionData):
            The lead form user submission referenced in
            the query.
        metrics (google.ads.googleads.v12.common.types.Metrics):
            The metrics.
        segments (google.ads.googleads.v12.common.types.Segments):
            The segments.
    """

    account_budget = proto.Field(
        proto.MESSAGE, number=42, message=gagr_account_budget.AccountBudget,
    )
    account_budget_proposal = proto.Field(
        proto.MESSAGE,
        number=43,
        message=gagr_account_budget_proposal.AccountBudgetProposal,
    )
    account_link = proto.Field(
        proto.MESSAGE, number=143, message=gagr_account_link.AccountLink,
    )
    ad_group = proto.Field(
        proto.MESSAGE, number=3, message=gagr_ad_group.AdGroup,
    )
    ad_group_ad = proto.Field(
        proto.MESSAGE, number=16, message=gagr_ad_group_ad.AdGroupAd,
    )
    ad_group_ad_asset_combination_view = proto.Field(
        proto.MESSAGE,
        number=193,
        message=gagr_ad_group_ad_asset_combination_view.AdGroupAdAssetCombinationView,
    )
    ad_group_ad_asset_view = proto.Field(
        proto.MESSAGE,
        number=131,
        message=gagr_ad_group_ad_asset_view.AdGroupAdAssetView,
    )
    ad_group_ad_label = proto.Field(
        proto.MESSAGE,
        number=120,
        message=gagr_ad_group_ad_label.AdGroupAdLabel,
    )
    ad_group_asset = proto.Field(
        proto.MESSAGE, number=154, message=gagr_ad_group_asset.AdGroupAsset,
    )
    ad_group_asset_set = proto.Field(
        proto.MESSAGE,
        number=196,
        message=gagr_ad_group_asset_set.AdGroupAssetSet,
    )
    ad_group_audience_view = proto.Field(
        proto.MESSAGE,
        number=57,
        message=gagr_ad_group_audience_view.AdGroupAudienceView,
    )
    ad_group_bid_modifier = proto.Field(
        proto.MESSAGE,
        number=24,
        message=gagr_ad_group_bid_modifier.AdGroupBidModifier,
    )
    ad_group_criterion = proto.Field(
        proto.MESSAGE,
        number=17,
        message=gagr_ad_group_criterion.AdGroupCriterion,
    )
    ad_group_criterion_customizer = proto.Field(
        proto.MESSAGE,
        number=187,
        message=gagr_ad_group_criterion_customizer.AdGroupCriterionCustomizer,
    )
    ad_group_criterion_label = proto.Field(
        proto.MESSAGE,
        number=121,
        message=gagr_ad_group_criterion_label.AdGroupCriterionLabel,
    )
    ad_group_criterion_simulation = proto.Field(
        proto.MESSAGE,
        number=110,
        message=gagr_ad_group_criterion_simulation.AdGroupCriterionSimulation,
    )
    ad_group_customizer = proto.Field(
        proto.MESSAGE,
        number=185,
        message=gagr_ad_group_customizer.AdGroupCustomizer,
    )
    ad_group_extension_setting = proto.Field(
        proto.MESSAGE,
        number=112,
        message=gagr_ad_group_extension_setting.AdGroupExtensionSetting,
    )
    ad_group_feed = proto.Field(
        proto.MESSAGE, number=67, message=gagr_ad_group_feed.AdGroupFeed,
    )
    ad_group_label = proto.Field(
        proto.MESSAGE, number=115, message=gagr_ad_group_label.AdGroupLabel,
    )
    ad_group_simulation = proto.Field(
        proto.MESSAGE,
        number=107,
        message=gagr_ad_group_simulation.AdGroupSimulation,
    )
    ad_parameter = proto.Field(
        proto.MESSAGE, number=130, message=gagr_ad_parameter.AdParameter,
    )
    age_range_view = proto.Field(
        proto.MESSAGE, number=48, message=gagr_age_range_view.AgeRangeView,
    )
    ad_schedule_view = proto.Field(
        proto.MESSAGE, number=89, message=gagr_ad_schedule_view.AdScheduleView,
    )
    domain_category = proto.Field(
        proto.MESSAGE, number=91, message=gagr_domain_category.DomainCategory,
    )
    asset = proto.Field(proto.MESSAGE, number=105, message=gagr_asset.Asset,)
    asset_field_type_view = proto.Field(
        proto.MESSAGE,
        number=168,
        message=gagr_asset_field_type_view.AssetFieldTypeView,
    )
    asset_group_asset = proto.Field(
        proto.MESSAGE,
        number=173,
        message=gagr_asset_group_asset.AssetGroupAsset,
    )
    asset_group_signal = proto.Field(
        proto.MESSAGE,
        number=191,
        message=gagr_asset_group_signal.AssetGroupSignal,
    )
    asset_group_listing_group_filter = proto.Field(
        proto.MESSAGE,
        number=182,
        message=gagr_asset_group_listing_group_filter.AssetGroupListingGroupFilter,
    )
    asset_group_product_group_view = proto.Field(
        proto.MESSAGE,
        number=189,
        message=gagr_asset_group_product_group_view.AssetGroupProductGroupView,
    )
    asset_group = proto.Field(
        proto.MESSAGE, number=172, message=gagr_asset_group.AssetGroup,
    )
    asset_set_asset = proto.Field(
        proto.MESSAGE, number=180, message=gagr_asset_set_asset.AssetSetAsset,
    )
    asset_set = proto.Field(
        proto.MESSAGE, number=179, message=gagr_asset_set.AssetSet,
    )
    asset_set_type_view = proto.Field(
        proto.MESSAGE,
        number=197,
        message=gagr_asset_set_type_view.AssetSetTypeView,
    )
    batch_job = proto.Field(
        proto.MESSAGE, number=139, message=gagr_batch_job.BatchJob,
    )
    bidding_data_exclusion = proto.Field(
        proto.MESSAGE,
        number=159,
        message=gagr_bidding_data_exclusion.BiddingDataExclusion,
    )
    bidding_seasonality_adjustment = proto.Field(
        proto.MESSAGE,
        number=160,
        message=gagr_bidding_seasonality_adjustment.BiddingSeasonalityAdjustment,
    )
    bidding_strategy = proto.Field(
        proto.MESSAGE, number=18, message=gagr_bidding_strategy.BiddingStrategy,
    )
    bidding_strategy_simulation = proto.Field(
        proto.MESSAGE,
        number=158,
        message=gagr_bidding_strategy_simulation.BiddingStrategySimulation,
    )
    billing_setup = proto.Field(
        proto.MESSAGE, number=41, message=gagr_billing_setup.BillingSetup,
    )
    call_view = proto.Field(
        proto.MESSAGE, number=152, message=gagr_call_view.CallView,
    )
    campaign_budget = proto.Field(
        proto.MESSAGE, number=19, message=gagr_campaign_budget.CampaignBudget,
    )
    campaign = proto.Field(
        proto.MESSAGE, number=2, message=gagr_campaign.Campaign,
    )
    campaign_asset = proto.Field(
        proto.MESSAGE, number=142, message=gagr_campaign_asset.CampaignAsset,
    )
    campaign_asset_set = proto.Field(
        proto.MESSAGE,
        number=181,
        message=gagr_campaign_asset_set.CampaignAssetSet,
    )
    campaign_audience_view = proto.Field(
        proto.MESSAGE,
        number=69,
        message=gagr_campaign_audience_view.CampaignAudienceView,
    )
    campaign_bid_modifier = proto.Field(
        proto.MESSAGE,
        number=26,
        message=gagr_campaign_bid_modifier.CampaignBidModifier,
    )
    campaign_conversion_goal = proto.Field(
        proto.MESSAGE,
        number=175,
        message=gagr_campaign_conversion_goal.CampaignConversionGoal,
    )
    campaign_criterion = proto.Field(
        proto.MESSAGE,
        number=20,
        message=gagr_campaign_criterion.CampaignCriterion,
    )
    campaign_criterion_simulation = proto.Field(
        proto.MESSAGE,
        number=111,
        message=gagr_campaign_criterion_simulation.CampaignCriterionSimulation,
    )
    campaign_customizer = proto.Field(
        proto.MESSAGE,
        number=186,
        message=gagr_campaign_customizer.CampaignCustomizer,
    )
    campaign_draft = proto.Field(
        proto.MESSAGE, number=49, message=gagr_campaign_draft.CampaignDraft,
    )
    campaign_extension_setting = proto.Field(
        proto.MESSAGE,
        number=113,
        message=gagr_campaign_extension_setting.CampaignExtensionSetting,
    )
    campaign_feed = proto.Field(
        proto.MESSAGE, number=63, message=gagr_campaign_feed.CampaignFeed,
    )
    campaign_group = proto.Field(
        proto.MESSAGE, number=25, message=gagr_campaign_group.CampaignGroup,
    )
    campaign_label = proto.Field(
        proto.MESSAGE, number=108, message=gagr_campaign_label.CampaignLabel,
    )
    campaign_shared_set = proto.Field(
        proto.MESSAGE,
        number=30,
        message=gagr_campaign_shared_set.CampaignSharedSet,
    )
    campaign_simulation = proto.Field(
        proto.MESSAGE,
        number=157,
        message=gagr_campaign_simulation.CampaignSimulation,
    )
    carrier_constant = proto.Field(
        proto.MESSAGE, number=66, message=gagr_carrier_constant.CarrierConstant,
    )
    change_event = proto.Field(
        proto.MESSAGE, number=145, message=gagr_change_event.ChangeEvent,
    )
    change_status = proto.Field(
        proto.MESSAGE, number=37, message=gagr_change_status.ChangeStatus,
    )
    combined_audience = proto.Field(
        proto.MESSAGE,
        number=148,
        message=gagr_combined_audience.CombinedAudience,
    )
    audience = proto.Field(
        proto.MESSAGE, number=190, message=gagr_audience.Audience,
    )
    conversion_action = proto.Field(
        proto.MESSAGE,
        number=103,
        message=gagr_conversion_action.ConversionAction,
    )
    conversion_custom_variable = proto.Field(
        proto.MESSAGE,
        number=153,
        message=gagr_conversion_custom_variable.ConversionCustomVariable,
    )
    conversion_goal_campaign_config = proto.Field(
        proto.MESSAGE,
        number=177,
        message=gagr_conversion_goal_campaign_config.ConversionGoalCampaignConfig,
    )
    conversion_value_rule = proto.Field(
        proto.MESSAGE,
        number=164,
        message=gagr_conversion_value_rule.ConversionValueRule,
    )
    conversion_value_rule_set = proto.Field(
        proto.MESSAGE,
        number=165,
        message=gagr_conversion_value_rule_set.ConversionValueRuleSet,
    )
    click_view = proto.Field(
        proto.MESSAGE, number=122, message=gagr_click_view.ClickView,
    )
    currency_constant = proto.Field(
        proto.MESSAGE,
        number=134,
        message=gagr_currency_constant.CurrencyConstant,
    )
    custom_audience = proto.Field(
        proto.MESSAGE, number=147, message=gagr_custom_audience.CustomAudience,
    )
    custom_conversion_goal = proto.Field(
        proto.MESSAGE,
        number=176,
        message=gagr_custom_conversion_goal.CustomConversionGoal,
    )
    custom_interest = proto.Field(
        proto.MESSAGE, number=104, message=gagr_custom_interest.CustomInterest,
    )
    customer = proto.Field(
        proto.MESSAGE, number=1, message=gagr_customer.Customer,
    )
    customer_asset = proto.Field(
        proto.MESSAGE, number=155, message=gagr_customer_asset.CustomerAsset,
    )
    customer_asset_set = proto.Field(
        proto.MESSAGE,
        number=195,
        message=gagr_customer_asset_set.CustomerAssetSet,
    )
    accessible_bidding_strategy = proto.Field(
        proto.MESSAGE,
        number=169,
        message=gagr_accessible_bidding_strategy.AccessibleBiddingStrategy,
    )
    customer_customizer = proto.Field(
        proto.MESSAGE,
        number=184,
        message=gagr_customer_customizer.CustomerCustomizer,
    )
    customer_manager_link = proto.Field(
        proto.MESSAGE,
        number=61,
        message=gagr_customer_manager_link.CustomerManagerLink,
    )
    customer_client_link = proto.Field(
        proto.MESSAGE,
        number=62,
        message=gagr_customer_client_link.CustomerClientLink,
    )
    customer_client = proto.Field(
        proto.MESSAGE, number=70, message=gagr_customer_client.CustomerClient,
    )
    customer_conversion_goal = proto.Field(
        proto.MESSAGE,
        number=174,
        message=gagr_customer_conversion_goal.CustomerConversionGoal,
    )
    customer_extension_setting = proto.Field(
        proto.MESSAGE,
        number=114,
        message=gagr_customer_extension_setting.CustomerExtensionSetting,
    )
    customer_feed = proto.Field(
        proto.MESSAGE, number=64, message=gagr_customer_feed.CustomerFeed,
    )
    customer_label = proto.Field(
        proto.MESSAGE, number=124, message=gagr_customer_label.CustomerLabel,
    )
    customer_negative_criterion = proto.Field(
        proto.MESSAGE,
        number=88,
        message=gagr_customer_negative_criterion.CustomerNegativeCriterion,
    )
    customer_user_access = proto.Field(
        proto.MESSAGE,
        number=146,
        message=gagr_customer_user_access.CustomerUserAccess,
    )
    customer_user_access_invitation = proto.Field(
        proto.MESSAGE,
        number=150,
        message=gagr_customer_user_access_invitation.CustomerUserAccessInvitation,
    )
    customizer_attribute = proto.Field(
        proto.MESSAGE,
        number=178,
        message=gagr_customizer_attribute.CustomizerAttribute,
    )
    detail_placement_view = proto.Field(
        proto.MESSAGE,
        number=118,
        message=gagr_detail_placement_view.DetailPlacementView,
    )
    detailed_demographic = proto.Field(
        proto.MESSAGE,
        number=166,
        message=gagr_detailed_demographic.DetailedDemographic,
    )
    display_keyword_view = proto.Field(
        proto.MESSAGE,
        number=47,
        message=gagr_display_keyword_view.DisplayKeywordView,
    )
    distance_view = proto.Field(
        proto.MESSAGE, number=132, message=gagr_distance_view.DistanceView,
    )
    dynamic_search_ads_search_term_view = proto.Field(
        proto.MESSAGE,
        number=106,
        message=gagr_dynamic_search_ads_search_term_view.DynamicSearchAdsSearchTermView,
    )
    expanded_landing_page_view = proto.Field(
        proto.MESSAGE,
        number=128,
        message=gagr_expanded_landing_page_view.ExpandedLandingPageView,
    )
    extension_feed_item = proto.Field(
        proto.MESSAGE,
        number=85,
        message=gagr_extension_feed_item.ExtensionFeedItem,
    )
    feed = proto.Field(proto.MESSAGE, number=46, message=gagr_feed.Feed,)
    feed_item = proto.Field(
        proto.MESSAGE, number=50, message=gagr_feed_item.FeedItem,
    )
    feed_item_set = proto.Field(
        proto.MESSAGE, number=149, message=gagr_feed_item_set.FeedItemSet,
    )
    feed_item_set_link = proto.Field(
        proto.MESSAGE,
        number=151,
        message=gagr_feed_item_set_link.FeedItemSetLink,
    )
    feed_item_target = proto.Field(
        proto.MESSAGE, number=116, message=gagr_feed_item_target.FeedItemTarget,
    )
    feed_mapping = proto.Field(
        proto.MESSAGE, number=58, message=gagr_feed_mapping.FeedMapping,
    )
    feed_placeholder_view = proto.Field(
        proto.MESSAGE,
        number=97,
        message=gagr_feed_placeholder_view.FeedPlaceholderView,
    )
    gender_view = proto.Field(
        proto.MESSAGE, number=40, message=gagr_gender_view.GenderView,
    )
    geo_target_constant = proto.Field(
        proto.MESSAGE,
        number=23,
        message=gagr_geo_target_constant.GeoTargetConstant,
    )
    geographic_view = proto.Field(
        proto.MESSAGE, number=125, message=gagr_geographic_view.GeographicView,
    )
    group_placement_view = proto.Field(
        proto.MESSAGE,
        number=119,
        message=gagr_group_placement_view.GroupPlacementView,
    )
    hotel_group_view = proto.Field(
        proto.MESSAGE, number=51, message=gagr_hotel_group_view.HotelGroupView,
    )
    hotel_performance_view = proto.Field(
        proto.MESSAGE,
        number=71,
        message=gagr_hotel_performance_view.HotelPerformanceView,
    )
    hotel_reconciliation = proto.Field(
        proto.MESSAGE,
        number=188,
        message=gagr_hotel_reconciliation.HotelReconciliation,
    )
    income_range_view = proto.Field(
        proto.MESSAGE,
        number=138,
        message=gagr_income_range_view.IncomeRangeView,
    )
    keyword_view = proto.Field(
        proto.MESSAGE, number=21, message=gagr_keyword_view.KeywordView,
    )
    keyword_plan = proto.Field(
        proto.MESSAGE, number=32, message=gagr_keyword_plan.KeywordPlan,
    )
    keyword_plan_campaign = proto.Field(
        proto.MESSAGE,
        number=33,
        message=gagr_keyword_plan_campaign.KeywordPlanCampaign,
    )
    keyword_plan_campaign_keyword = proto.Field(
        proto.MESSAGE,
        number=140,
        message=gagr_keyword_plan_campaign_keyword.KeywordPlanCampaignKeyword,
    )
    keyword_plan_ad_group = proto.Field(
        proto.MESSAGE,
        number=35,
        message=gagr_keyword_plan_ad_group.KeywordPlanAdGroup,
    )
    keyword_plan_ad_group_keyword = proto.Field(
        proto.MESSAGE,
        number=141,
        message=gagr_keyword_plan_ad_group_keyword.KeywordPlanAdGroupKeyword,
    )
    keyword_theme_constant = proto.Field(
        proto.MESSAGE,
        number=163,
        message=gagr_keyword_theme_constant.KeywordThemeConstant,
    )
    label = proto.Field(proto.MESSAGE, number=52, message=gagr_label.Label,)
    landing_page_view = proto.Field(
        proto.MESSAGE,
        number=126,
        message=gagr_landing_page_view.LandingPageView,
    )
    language_constant = proto.Field(
        proto.MESSAGE,
        number=55,
        message=gagr_language_constant.LanguageConstant,
    )
    location_view = proto.Field(
        proto.MESSAGE, number=123, message=gagr_location_view.LocationView,
    )
    managed_placement_view = proto.Field(
        proto.MESSAGE,
        number=53,
        message=gagr_managed_placement_view.ManagedPlacementView,
    )
    media_file = proto.Field(
        proto.MESSAGE, number=90, message=gagr_media_file.MediaFile,
    )
    mobile_app_category_constant = proto.Field(
        proto.MESSAGE,
        number=87,
        message=gagr_mobile_app_category_constant.MobileAppCategoryConstant,
    )
    mobile_device_constant = proto.Field(
        proto.MESSAGE,
        number=98,
        message=gagr_mobile_device_constant.MobileDeviceConstant,
    )
    offline_user_data_job = proto.Field(
        proto.MESSAGE,
        number=137,
        message=gagr_offline_user_data_job.OfflineUserDataJob,
    )
    operating_system_version_constant = proto.Field(
        proto.MESSAGE,
        number=86,
        message=gagr_operating_system_version_constant.OperatingSystemVersionConstant,
    )
    paid_organic_search_term_view = proto.Field(
        proto.MESSAGE,
        number=129,
        message=gagr_paid_organic_search_term_view.PaidOrganicSearchTermView,
    )
    parental_status_view = proto.Field(
        proto.MESSAGE,
        number=45,
        message=gagr_parental_status_view.ParentalStatusView,
    )
    per_store_view = proto.Field(
        proto.MESSAGE, number=198, message=gagr_per_store_view.PerStoreView,
    )
    product_bidding_category_constant = proto.Field(
        proto.MESSAGE,
        number=109,
        message=gagr_product_bidding_category_constant.ProductBiddingCategoryConstant,
    )
    product_group_view = proto.Field(
        proto.MESSAGE,
        number=54,
        message=gagr_product_group_view.ProductGroupView,
    )
    recommendation = proto.Field(
        proto.MESSAGE, number=22, message=gagr_recommendation.Recommendation,
    )
    search_term_view = proto.Field(
        proto.MESSAGE, number=68, message=gagr_search_term_view.SearchTermView,
    )
    shared_criterion = proto.Field(
        proto.MESSAGE, number=29, message=gagr_shared_criterion.SharedCriterion,
    )
    shared_set = proto.Field(
        proto.MESSAGE, number=27, message=gagr_shared_set.SharedSet,
    )
    smart_campaign_setting = proto.Field(
        proto.MESSAGE,
        number=167,
        message=gagr_smart_campaign_setting.SmartCampaignSetting,
    )
    shopping_performance_view = proto.Field(
        proto.MESSAGE,
        number=117,
        message=gagr_shopping_performance_view.ShoppingPerformanceView,
    )
    smart_campaign_search_term_view = proto.Field(
        proto.MESSAGE,
        number=170,
        message=gagr_smart_campaign_search_term_view.SmartCampaignSearchTermView,
    )
    third_party_app_analytics_link = proto.Field(
        proto.MESSAGE,
        number=144,
        message=gagr_third_party_app_analytics_link.ThirdPartyAppAnalyticsLink,
    )
    topic_view = proto.Field(
        proto.MESSAGE, number=44, message=gagr_topic_view.TopicView,
    )
    experiment = proto.Field(
        proto.MESSAGE, number=133, message=gagr_experiment.Experiment,
    )
    experiment_arm = proto.Field(
        proto.MESSAGE, number=183, message=gagr_experiment_arm.ExperimentArm,
    )
    user_interest = proto.Field(
        proto.MESSAGE, number=59, message=gagr_user_interest.UserInterest,
    )
    life_event = proto.Field(
        proto.MESSAGE, number=161, message=gagr_life_event.LifeEvent,
    )
    user_list = proto.Field(
        proto.MESSAGE, number=38, message=gagr_user_list.UserList,
    )
    user_location_view = proto.Field(
        proto.MESSAGE,
        number=135,
        message=gagr_user_location_view.UserLocationView,
    )
    remarketing_action = proto.Field(
        proto.MESSAGE,
        number=60,
        message=gagr_remarketing_action.RemarketingAction,
    )
    topic_constant = proto.Field(
        proto.MESSAGE, number=31, message=gagr_topic_constant.TopicConstant,
    )
    video = proto.Field(proto.MESSAGE, number=39, message=gagr_video.Video,)
    webpage_view = proto.Field(
        proto.MESSAGE, number=162, message=gagr_webpage_view.WebpageView,
    )
    lead_form_submission_data = proto.Field(
        proto.MESSAGE,
        number=192,
        message=gagr_lead_form_submission_data.LeadFormSubmissionData,
    )
    metrics = proto.Field(
        proto.MESSAGE, number=4, message=gagc_metrics.Metrics,
    )
    segments = proto.Field(
        proto.MESSAGE, number=102, message=gagc_segments.Segments,
    )


class MutateGoogleAdsRequest(proto.Message):
    r"""Request message for
    [GoogleAdsService.Mutate][google.ads.googleads.v12.services.GoogleAdsService.Mutate].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            resources are being modified.
        mutate_operations (Sequence[google.ads.googleads.v12.services.types.MutateOperation]):
            Required. The list of operations to perform
            on individual resources.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
        response_content_type (google.ads.googleads.v12.enums.types.ResponseContentTypeEnum.ResponseContentType):
            The response content type setting. Determines
            whether the mutable resource or just the
            resource name should be returned post mutation.
            The mutable resource will only be returned if
            the resource has the appropriate response field.
            For example, MutateCampaignResult.campaign.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    mutate_operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="MutateOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)
    response_content_type = proto.Field(
        proto.ENUM,
        number=5,
        enum=gage_response_content_type.ResponseContentTypeEnum.ResponseContentType,
    )


class MutateGoogleAdsResponse(proto.Message):
    r"""Response message for
    [GoogleAdsService.Mutate][google.ads.googleads.v12.services.GoogleAdsService.Mutate].

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        mutate_operation_responses (Sequence[google.ads.googleads.v12.services.types.MutateOperationResponse]):
            All responses for the mutate.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=3, message=status_pb2.Status,
    )
    mutate_operation_responses = proto.RepeatedField(
        proto.MESSAGE, number=1, message="MutateOperationResponse",
    )


class MutateOperation(proto.Message):
    r"""A single operation (create, update, remove) on a resource.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        ad_group_ad_label_operation (google.ads.googleads.v12.services.types.AdGroupAdLabelOperation):
            An ad group ad label mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_ad_operation (google.ads.googleads.v12.services.types.AdGroupAdOperation):
            An ad group ad mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_asset_operation (google.ads.googleads.v12.services.types.AdGroupAssetOperation):
            An ad group asset mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_bid_modifier_operation (google.ads.googleads.v12.services.types.AdGroupBidModifierOperation):
            An ad group bid modifier mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_criterion_customizer_operation (google.ads.googleads.v12.services.types.AdGroupCriterionCustomizerOperation):
            An ad group criterion customizer mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_criterion_label_operation (google.ads.googleads.v12.services.types.AdGroupCriterionLabelOperation):
            An ad group criterion label mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_criterion_operation (google.ads.googleads.v12.services.types.AdGroupCriterionOperation):
            An ad group criterion mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_customizer_operation (google.ads.googleads.v12.services.types.AdGroupCustomizerOperation):
            An ad group customizer mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_extension_setting_operation (google.ads.googleads.v12.services.types.AdGroupExtensionSettingOperation):
            An ad group extension setting mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_feed_operation (google.ads.googleads.v12.services.types.AdGroupFeedOperation):
            An ad group feed mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_label_operation (google.ads.googleads.v12.services.types.AdGroupLabelOperation):
            An ad group label mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_group_operation (google.ads.googleads.v12.services.types.AdGroupOperation):
            An ad group mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_operation (google.ads.googleads.v12.services.types.AdOperation):
            An ad mutate operation.

            This field is a member of `oneof`_ ``operation``.
        ad_parameter_operation (google.ads.googleads.v12.services.types.AdParameterOperation):
            An ad parameter mutate operation.

            This field is a member of `oneof`_ ``operation``.
        asset_operation (google.ads.googleads.v12.services.types.AssetOperation):
            An asset mutate operation.

            This field is a member of `oneof`_ ``operation``.
        asset_group_asset_operation (google.ads.googleads.v12.services.types.AssetGroupAssetOperation):
            An asset group asset mutate operation.

            This field is a member of `oneof`_ ``operation``.
        asset_group_listing_group_filter_operation (google.ads.googleads.v12.services.types.AssetGroupListingGroupFilterOperation):
            An asset group listing group filter mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        asset_group_signal_operation (google.ads.googleads.v12.services.types.AssetGroupSignalOperation):
            An asset group signal mutate operation.

            This field is a member of `oneof`_ ``operation``.
        asset_group_operation (google.ads.googleads.v12.services.types.AssetGroupOperation):
            An asset group mutate operation.

            This field is a member of `oneof`_ ``operation``.
        asset_set_asset_operation (google.ads.googleads.v12.services.types.AssetSetAssetOperation):
            An asset set asset mutate operation.

            This field is a member of `oneof`_ ``operation``.
        asset_set_operation (google.ads.googleads.v12.services.types.AssetSetOperation):
            An asset set mutate operation.

            This field is a member of `oneof`_ ``operation``.
        audience_operation (google.ads.googleads.v12.services.types.AudienceOperation):
            An audience mutate operation.

            This field is a member of `oneof`_ ``operation``.
        bidding_data_exclusion_operation (google.ads.googleads.v12.services.types.BiddingDataExclusionOperation):
            A bidding data exclusion mutate operation.

            This field is a member of `oneof`_ ``operation``.
        bidding_seasonality_adjustment_operation (google.ads.googleads.v12.services.types.BiddingSeasonalityAdjustmentOperation):
            A bidding seasonality adjustment mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        bidding_strategy_operation (google.ads.googleads.v12.services.types.BiddingStrategyOperation):
            A bidding strategy mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_asset_operation (google.ads.googleads.v12.services.types.CampaignAssetOperation):
            A campaign asset mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_asset_set_operation (google.ads.googleads.v12.services.types.CampaignAssetSetOperation):
            A campaign asset mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_bid_modifier_operation (google.ads.googleads.v12.services.types.CampaignBidModifierOperation):
            A campaign bid modifier mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_budget_operation (google.ads.googleads.v12.services.types.CampaignBudgetOperation):
            A campaign budget mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_conversion_goal_operation (google.ads.googleads.v12.services.types.CampaignConversionGoalOperation):
            A campaign conversion goal mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_criterion_operation (google.ads.googleads.v12.services.types.CampaignCriterionOperation):
            A campaign criterion mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_customizer_operation (google.ads.googleads.v12.services.types.CampaignCustomizerOperation):
            A campaign customizer mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_draft_operation (google.ads.googleads.v12.services.types.CampaignDraftOperation):
            A campaign draft mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_extension_setting_operation (google.ads.googleads.v12.services.types.CampaignExtensionSettingOperation):
            A campaign extension setting mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_feed_operation (google.ads.googleads.v12.services.types.CampaignFeedOperation):
            A campaign feed mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_group_operation (google.ads.googleads.v12.services.types.CampaignGroupOperation):
            A campaign group mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_label_operation (google.ads.googleads.v12.services.types.CampaignLabelOperation):
            A campaign label mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_operation (google.ads.googleads.v12.services.types.CampaignOperation):
            A campaign mutate operation.

            This field is a member of `oneof`_ ``operation``.
        campaign_shared_set_operation (google.ads.googleads.v12.services.types.CampaignSharedSetOperation):
            A campaign shared set mutate operation.

            This field is a member of `oneof`_ ``operation``.
        conversion_action_operation (google.ads.googleads.v12.services.types.ConversionActionOperation):
            A conversion action mutate operation.

            This field is a member of `oneof`_ ``operation``.
        conversion_custom_variable_operation (google.ads.googleads.v12.services.types.ConversionCustomVariableOperation):
            A conversion custom variable mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        conversion_goal_campaign_config_operation (google.ads.googleads.v12.services.types.ConversionGoalCampaignConfigOperation):
            A conversion goal campaign config mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        conversion_value_rule_operation (google.ads.googleads.v12.services.types.ConversionValueRuleOperation):
            A conversion value rule mutate operation.

            This field is a member of `oneof`_ ``operation``.
        conversion_value_rule_set_operation (google.ads.googleads.v12.services.types.ConversionValueRuleSetOperation):
            A conversion value rule set mutate operation.

            This field is a member of `oneof`_ ``operation``.
        custom_conversion_goal_operation (google.ads.googleads.v12.services.types.CustomConversionGoalOperation):
            A custom conversion goal mutate operation.

            This field is a member of `oneof`_ ``operation``.
        customer_asset_operation (google.ads.googleads.v12.services.types.CustomerAssetOperation):
            A customer asset mutate operation.

            This field is a member of `oneof`_ ``operation``.
        customer_conversion_goal_operation (google.ads.googleads.v12.services.types.CustomerConversionGoalOperation):
            A customer conversion goal mutate operation.

            This field is a member of `oneof`_ ``operation``.
        customer_customizer_operation (google.ads.googleads.v12.services.types.CustomerCustomizerOperation):
            A customer customizer mutate operation.

            This field is a member of `oneof`_ ``operation``.
        customer_extension_setting_operation (google.ads.googleads.v12.services.types.CustomerExtensionSettingOperation):
            A customer extension setting mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        customer_feed_operation (google.ads.googleads.v12.services.types.CustomerFeedOperation):
            A customer feed mutate operation.

            This field is a member of `oneof`_ ``operation``.
        customer_label_operation (google.ads.googleads.v12.services.types.CustomerLabelOperation):
            A customer label mutate operation.

            This field is a member of `oneof`_ ``operation``.
        customer_negative_criterion_operation (google.ads.googleads.v12.services.types.CustomerNegativeCriterionOperation):
            A customer negative criterion mutate
            operation.

            This field is a member of `oneof`_ ``operation``.
        customer_operation (google.ads.googleads.v12.services.types.CustomerOperation):
            A customer mutate operation.

            This field is a member of `oneof`_ ``operation``.
        customizer_attribute_operation (google.ads.googleads.v12.services.types.CustomizerAttributeOperation):
            A customizer attribute mutate operation.

            This field is a member of `oneof`_ ``operation``.
        experiment_operation (google.ads.googleads.v12.services.types.ExperimentOperation):
            An experiment mutate operation.

            This field is a member of `oneof`_ ``operation``.
        experiment_arm_operation (google.ads.googleads.v12.services.types.ExperimentArmOperation):
            An experiment arm mutate operation.

            This field is a member of `oneof`_ ``operation``.
        extension_feed_item_operation (google.ads.googleads.v12.services.types.ExtensionFeedItemOperation):
            An extension feed item mutate operation.

            This field is a member of `oneof`_ ``operation``.
        feed_item_operation (google.ads.googleads.v12.services.types.FeedItemOperation):
            A feed item mutate operation.

            This field is a member of `oneof`_ ``operation``.
        feed_item_set_operation (google.ads.googleads.v12.services.types.FeedItemSetOperation):
            A feed item set mutate operation.

            This field is a member of `oneof`_ ``operation``.
        feed_item_set_link_operation (google.ads.googleads.v12.services.types.FeedItemSetLinkOperation):
            A feed item set link mutate operation.

            This field is a member of `oneof`_ ``operation``.
        feed_item_target_operation (google.ads.googleads.v12.services.types.FeedItemTargetOperation):
            A feed item target mutate operation.

            This field is a member of `oneof`_ ``operation``.
        feed_mapping_operation (google.ads.googleads.v12.services.types.FeedMappingOperation):
            A feed mapping mutate operation.

            This field is a member of `oneof`_ ``operation``.
        feed_operation (google.ads.googleads.v12.services.types.FeedOperation):
            A feed mutate operation.

            This field is a member of `oneof`_ ``operation``.
        keyword_plan_ad_group_operation (google.ads.googleads.v12.services.types.KeywordPlanAdGroupOperation):
            A keyword plan ad group operation.

            This field is a member of `oneof`_ ``operation``.
        keyword_plan_ad_group_keyword_operation (google.ads.googleads.v12.services.types.KeywordPlanAdGroupKeywordOperation):
            A keyword plan ad group keyword operation.

            This field is a member of `oneof`_ ``operation``.
        keyword_plan_campaign_keyword_operation (google.ads.googleads.v12.services.types.KeywordPlanCampaignKeywordOperation):
            A keyword plan campaign keyword operation.

            This field is a member of `oneof`_ ``operation``.
        keyword_plan_campaign_operation (google.ads.googleads.v12.services.types.KeywordPlanCampaignOperation):
            A keyword plan campaign operation.

            This field is a member of `oneof`_ ``operation``.
        keyword_plan_operation (google.ads.googleads.v12.services.types.KeywordPlanOperation):
            A keyword plan operation.

            This field is a member of `oneof`_ ``operation``.
        label_operation (google.ads.googleads.v12.services.types.LabelOperation):
            A label mutate operation.

            This field is a member of `oneof`_ ``operation``.
        media_file_operation (google.ads.googleads.v12.services.types.MediaFileOperation):
            A media file mutate operation.

            This field is a member of `oneof`_ ``operation``.
        remarketing_action_operation (google.ads.googleads.v12.services.types.RemarketingActionOperation):
            A remarketing action mutate operation.

            This field is a member of `oneof`_ ``operation``.
        shared_criterion_operation (google.ads.googleads.v12.services.types.SharedCriterionOperation):
            A shared criterion mutate operation.

            This field is a member of `oneof`_ ``operation``.
        shared_set_operation (google.ads.googleads.v12.services.types.SharedSetOperation):
            A shared set mutate operation.

            This field is a member of `oneof`_ ``operation``.
        smart_campaign_setting_operation (google.ads.googleads.v12.services.types.SmartCampaignSettingOperation):
            A Smart campaign setting mutate operation.

            This field is a member of `oneof`_ ``operation``.
        user_list_operation (google.ads.googleads.v12.services.types.UserListOperation):
            A user list mutate operation.

            This field is a member of `oneof`_ ``operation``.
    """

    ad_group_ad_label_operation = proto.Field(
        proto.MESSAGE,
        number=17,
        oneof="operation",
        message=ad_group_ad_label_service.AdGroupAdLabelOperation,
    )
    ad_group_ad_operation = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=ad_group_ad_service.AdGroupAdOperation,
    )
    ad_group_asset_operation = proto.Field(
        proto.MESSAGE,
        number=56,
        oneof="operation",
        message=ad_group_asset_service.AdGroupAssetOperation,
    )
    ad_group_bid_modifier_operation = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=ad_group_bid_modifier_service.AdGroupBidModifierOperation,
    )
    ad_group_criterion_customizer_operation = proto.Field(
        proto.MESSAGE,
        number=77,
        oneof="operation",
        message=ad_group_criterion_customizer_service.AdGroupCriterionCustomizerOperation,
    )
    ad_group_criterion_label_operation = proto.Field(
        proto.MESSAGE,
        number=18,
        oneof="operation",
        message=ad_group_criterion_label_service.AdGroupCriterionLabelOperation,
    )
    ad_group_criterion_operation = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="operation",
        message=ad_group_criterion_service.AdGroupCriterionOperation,
    )
    ad_group_customizer_operation = proto.Field(
        proto.MESSAGE,
        number=75,
        oneof="operation",
        message=ad_group_customizer_service.AdGroupCustomizerOperation,
    )
    ad_group_extension_setting_operation = proto.Field(
        proto.MESSAGE,
        number=19,
        oneof="operation",
        message=ad_group_extension_setting_service.AdGroupExtensionSettingOperation,
    )
    ad_group_feed_operation = proto.Field(
        proto.MESSAGE,
        number=20,
        oneof="operation",
        message=ad_group_feed_service.AdGroupFeedOperation,
    )
    ad_group_label_operation = proto.Field(
        proto.MESSAGE,
        number=21,
        oneof="operation",
        message=ad_group_label_service.AdGroupLabelOperation,
    )
    ad_group_operation = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="operation",
        message=ad_group_service.AdGroupOperation,
    )
    ad_operation = proto.Field(
        proto.MESSAGE,
        number=49,
        oneof="operation",
        message=ad_service.AdOperation,
    )
    ad_parameter_operation = proto.Field(
        proto.MESSAGE,
        number=22,
        oneof="operation",
        message=ad_parameter_service.AdParameterOperation,
    )
    asset_operation = proto.Field(
        proto.MESSAGE,
        number=23,
        oneof="operation",
        message=asset_service.AssetOperation,
    )
    asset_group_asset_operation = proto.Field(
        proto.MESSAGE,
        number=65,
        oneof="operation",
        message=asset_group_asset_service.AssetGroupAssetOperation,
    )
    asset_group_listing_group_filter_operation = proto.Field(
        proto.MESSAGE,
        number=78,
        oneof="operation",
        message=asset_group_listing_group_filter_service.AssetGroupListingGroupFilterOperation,
    )
    asset_group_signal_operation = proto.Field(
        proto.MESSAGE,
        number=80,
        oneof="operation",
        message=asset_group_signal_service.AssetGroupSignalOperation,
    )
    asset_group_operation = proto.Field(
        proto.MESSAGE,
        number=62,
        oneof="operation",
        message=asset_group_service.AssetGroupOperation,
    )
    asset_set_asset_operation = proto.Field(
        proto.MESSAGE,
        number=71,
        oneof="operation",
        message=asset_set_asset_service.AssetSetAssetOperation,
    )
    asset_set_operation = proto.Field(
        proto.MESSAGE,
        number=72,
        oneof="operation",
        message=asset_set_service.AssetSetOperation,
    )
    audience_operation = proto.Field(
        proto.MESSAGE,
        number=81,
        oneof="operation",
        message=audience_service.AudienceOperation,
    )
    bidding_data_exclusion_operation = proto.Field(
        proto.MESSAGE,
        number=58,
        oneof="operation",
        message=bidding_data_exclusion_service.BiddingDataExclusionOperation,
    )
    bidding_seasonality_adjustment_operation = proto.Field(
        proto.MESSAGE,
        number=59,
        oneof="operation",
        message=bidding_seasonality_adjustment_service.BiddingSeasonalityAdjustmentOperation,
    )
    bidding_strategy_operation = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="operation",
        message=bidding_strategy_service.BiddingStrategyOperation,
    )
    campaign_asset_operation = proto.Field(
        proto.MESSAGE,
        number=52,
        oneof="operation",
        message=campaign_asset_service.CampaignAssetOperation,
    )
    campaign_asset_set_operation = proto.Field(
        proto.MESSAGE,
        number=73,
        oneof="operation",
        message=campaign_asset_set_service.CampaignAssetSetOperation,
    )
    campaign_bid_modifier_operation = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="operation",
        message=campaign_bid_modifier_service.CampaignBidModifierOperation,
    )
    campaign_budget_operation = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="operation",
        message=campaign_budget_service.CampaignBudgetOperation,
    )
    campaign_conversion_goal_operation = proto.Field(
        proto.MESSAGE,
        number=67,
        oneof="operation",
        message=campaign_conversion_goal_service.CampaignConversionGoalOperation,
    )
    campaign_criterion_operation = proto.Field(
        proto.MESSAGE,
        number=13,
        oneof="operation",
        message=campaign_criterion_service.CampaignCriterionOperation,
    )
    campaign_customizer_operation = proto.Field(
        proto.MESSAGE,
        number=76,
        oneof="operation",
        message=campaign_customizer_service.CampaignCustomizerOperation,
    )
    campaign_draft_operation = proto.Field(
        proto.MESSAGE,
        number=24,
        oneof="operation",
        message=campaign_draft_service.CampaignDraftOperation,
    )
    campaign_extension_setting_operation = proto.Field(
        proto.MESSAGE,
        number=26,
        oneof="operation",
        message=campaign_extension_setting_service.CampaignExtensionSettingOperation,
    )
    campaign_feed_operation = proto.Field(
        proto.MESSAGE,
        number=27,
        oneof="operation",
        message=campaign_feed_service.CampaignFeedOperation,
    )
    campaign_group_operation = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="operation",
        message=campaign_group_service.CampaignGroupOperation,
    )
    campaign_label_operation = proto.Field(
        proto.MESSAGE,
        number=28,
        oneof="operation",
        message=campaign_label_service.CampaignLabelOperation,
    )
    campaign_operation = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="operation",
        message=campaign_service.CampaignOperation,
    )
    campaign_shared_set_operation = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="operation",
        message=campaign_shared_set_service.CampaignSharedSetOperation,
    )
    conversion_action_operation = proto.Field(
        proto.MESSAGE,
        number=12,
        oneof="operation",
        message=conversion_action_service.ConversionActionOperation,
    )
    conversion_custom_variable_operation = proto.Field(
        proto.MESSAGE,
        number=55,
        oneof="operation",
        message=conversion_custom_variable_service.ConversionCustomVariableOperation,
    )
    conversion_goal_campaign_config_operation = proto.Field(
        proto.MESSAGE,
        number=69,
        oneof="operation",
        message=conversion_goal_campaign_config_service.ConversionGoalCampaignConfigOperation,
    )
    conversion_value_rule_operation = proto.Field(
        proto.MESSAGE,
        number=63,
        oneof="operation",
        message=conversion_value_rule_service.ConversionValueRuleOperation,
    )
    conversion_value_rule_set_operation = proto.Field(
        proto.MESSAGE,
        number=64,
        oneof="operation",
        message=conversion_value_rule_set_service.ConversionValueRuleSetOperation,
    )
    custom_conversion_goal_operation = proto.Field(
        proto.MESSAGE,
        number=68,
        oneof="operation",
        message=custom_conversion_goal_service.CustomConversionGoalOperation,
    )
    customer_asset_operation = proto.Field(
        proto.MESSAGE,
        number=57,
        oneof="operation",
        message=customer_asset_service.CustomerAssetOperation,
    )
    customer_conversion_goal_operation = proto.Field(
        proto.MESSAGE,
        number=66,
        oneof="operation",
        message=customer_conversion_goal_service.CustomerConversionGoalOperation,
    )
    customer_customizer_operation = proto.Field(
        proto.MESSAGE,
        number=79,
        oneof="operation",
        message=customer_customizer_service.CustomerCustomizerOperation,
    )
    customer_extension_setting_operation = proto.Field(
        proto.MESSAGE,
        number=30,
        oneof="operation",
        message=customer_extension_setting_service.CustomerExtensionSettingOperation,
    )
    customer_feed_operation = proto.Field(
        proto.MESSAGE,
        number=31,
        oneof="operation",
        message=customer_feed_service.CustomerFeedOperation,
    )
    customer_label_operation = proto.Field(
        proto.MESSAGE,
        number=32,
        oneof="operation",
        message=customer_label_service.CustomerLabelOperation,
    )
    customer_negative_criterion_operation = proto.Field(
        proto.MESSAGE,
        number=34,
        oneof="operation",
        message=customer_negative_criterion_service.CustomerNegativeCriterionOperation,
    )
    customer_operation = proto.Field(
        proto.MESSAGE,
        number=35,
        oneof="operation",
        message=customer_service.CustomerOperation,
    )
    customizer_attribute_operation = proto.Field(
        proto.MESSAGE,
        number=70,
        oneof="operation",
        message=customizer_attribute_service.CustomizerAttributeOperation,
    )
    experiment_operation = proto.Field(
        proto.MESSAGE,
        number=82,
        oneof="operation",
        message=experiment_service.ExperimentOperation,
    )
    experiment_arm_operation = proto.Field(
        proto.MESSAGE,
        number=83,
        oneof="operation",
        message=experiment_arm_service.ExperimentArmOperation,
    )
    extension_feed_item_operation = proto.Field(
        proto.MESSAGE,
        number=36,
        oneof="operation",
        message=extension_feed_item_service.ExtensionFeedItemOperation,
    )
    feed_item_operation = proto.Field(
        proto.MESSAGE,
        number=37,
        oneof="operation",
        message=feed_item_service.FeedItemOperation,
    )
    feed_item_set_operation = proto.Field(
        proto.MESSAGE,
        number=53,
        oneof="operation",
        message=feed_item_set_service.FeedItemSetOperation,
    )
    feed_item_set_link_operation = proto.Field(
        proto.MESSAGE,
        number=54,
        oneof="operation",
        message=feed_item_set_link_service.FeedItemSetLinkOperation,
    )
    feed_item_target_operation = proto.Field(
        proto.MESSAGE,
        number=38,
        oneof="operation",
        message=feed_item_target_service.FeedItemTargetOperation,
    )
    feed_mapping_operation = proto.Field(
        proto.MESSAGE,
        number=39,
        oneof="operation",
        message=feed_mapping_service.FeedMappingOperation,
    )
    feed_operation = proto.Field(
        proto.MESSAGE,
        number=40,
        oneof="operation",
        message=feed_service.FeedOperation,
    )
    keyword_plan_ad_group_operation = proto.Field(
        proto.MESSAGE,
        number=44,
        oneof="operation",
        message=keyword_plan_ad_group_service.KeywordPlanAdGroupOperation,
    )
    keyword_plan_ad_group_keyword_operation = proto.Field(
        proto.MESSAGE,
        number=50,
        oneof="operation",
        message=keyword_plan_ad_group_keyword_service.KeywordPlanAdGroupKeywordOperation,
    )
    keyword_plan_campaign_keyword_operation = proto.Field(
        proto.MESSAGE,
        number=51,
        oneof="operation",
        message=keyword_plan_campaign_keyword_service.KeywordPlanCampaignKeywordOperation,
    )
    keyword_plan_campaign_operation = proto.Field(
        proto.MESSAGE,
        number=45,
        oneof="operation",
        message=keyword_plan_campaign_service.KeywordPlanCampaignOperation,
    )
    keyword_plan_operation = proto.Field(
        proto.MESSAGE,
        number=48,
        oneof="operation",
        message=keyword_plan_service.KeywordPlanOperation,
    )
    label_operation = proto.Field(
        proto.MESSAGE,
        number=41,
        oneof="operation",
        message=label_service.LabelOperation,
    )
    media_file_operation = proto.Field(
        proto.MESSAGE,
        number=42,
        oneof="operation",
        message=media_file_service.MediaFileOperation,
    )
    remarketing_action_operation = proto.Field(
        proto.MESSAGE,
        number=43,
        oneof="operation",
        message=remarketing_action_service.RemarketingActionOperation,
    )
    shared_criterion_operation = proto.Field(
        proto.MESSAGE,
        number=14,
        oneof="operation",
        message=shared_criterion_service.SharedCriterionOperation,
    )
    shared_set_operation = proto.Field(
        proto.MESSAGE,
        number=15,
        oneof="operation",
        message=shared_set_service.SharedSetOperation,
    )
    smart_campaign_setting_operation = proto.Field(
        proto.MESSAGE,
        number=61,
        oneof="operation",
        message=smart_campaign_setting_service.SmartCampaignSettingOperation,
    )
    user_list_operation = proto.Field(
        proto.MESSAGE,
        number=16,
        oneof="operation",
        message=user_list_service.UserListOperation,
    )


class MutateOperationResponse(proto.Message):
    r"""Response message for the resource mutate.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        ad_group_ad_label_result (google.ads.googleads.v12.services.types.MutateAdGroupAdLabelResult):
            The result for the ad group ad label mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_ad_result (google.ads.googleads.v12.services.types.MutateAdGroupAdResult):
            The result for the ad group ad mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_asset_result (google.ads.googleads.v12.services.types.MutateAdGroupAssetResult):
            The result for the ad group asset mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_bid_modifier_result (google.ads.googleads.v12.services.types.MutateAdGroupBidModifierResult):
            The result for the ad group bid modifier
            mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_criterion_customizer_result (google.ads.googleads.v12.services.types.MutateAdGroupCriterionCustomizerResult):
            The result for the ad group criterion
            customizer mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_criterion_label_result (google.ads.googleads.v12.services.types.MutateAdGroupCriterionLabelResult):
            The result for the ad group criterion label
            mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_criterion_result (google.ads.googleads.v12.services.types.MutateAdGroupCriterionResult):
            The result for the ad group criterion mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_customizer_result (google.ads.googleads.v12.services.types.MutateAdGroupCustomizerResult):
            The result for the ad group customizer
            mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_extension_setting_result (google.ads.googleads.v12.services.types.MutateAdGroupExtensionSettingResult):
            The result for the ad group extension setting
            mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_feed_result (google.ads.googleads.v12.services.types.MutateAdGroupFeedResult):
            The result for the ad group feed mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_label_result (google.ads.googleads.v12.services.types.MutateAdGroupLabelResult):
            The result for the ad group label mutate.

            This field is a member of `oneof`_ ``response``.
        ad_group_result (google.ads.googleads.v12.services.types.MutateAdGroupResult):
            The result for the ad group mutate.

            This field is a member of `oneof`_ ``response``.
        ad_parameter_result (google.ads.googleads.v12.services.types.MutateAdParameterResult):
            The result for the ad parameter mutate.

            This field is a member of `oneof`_ ``response``.
        ad_result (google.ads.googleads.v12.services.types.MutateAdResult):
            The result for the ad mutate.

            This field is a member of `oneof`_ ``response``.
        asset_result (google.ads.googleads.v12.services.types.MutateAssetResult):
            The result for the asset mutate.

            This field is a member of `oneof`_ ``response``.
        asset_group_asset_result (google.ads.googleads.v12.services.types.MutateAssetGroupAssetResult):
            The result for the asset group asset mutate.

            This field is a member of `oneof`_ ``response``.
        asset_group_listing_group_filter_result (google.ads.googleads.v12.services.types.MutateAssetGroupListingGroupFilterResult):
            The result for the asset group listing group
            filter mutate.

            This field is a member of `oneof`_ ``response``.
        asset_group_signal_result (google.ads.googleads.v12.services.types.MutateAssetGroupSignalResult):
            The result for the asset group signal mutate.

            This field is a member of `oneof`_ ``response``.
        asset_group_result (google.ads.googleads.v12.services.types.MutateAssetGroupResult):
            The result for the asset group mutate.

            This field is a member of `oneof`_ ``response``.
        asset_set_asset_result (google.ads.googleads.v12.services.types.MutateAssetSetAssetResult):
            The result for the asset set asset mutate.

            This field is a member of `oneof`_ ``response``.
        asset_set_result (google.ads.googleads.v12.services.types.MutateAssetSetResult):
            The result for the asset set mutate.

            This field is a member of `oneof`_ ``response``.
        audience_result (google.ads.googleads.v12.services.types.MutateAudienceResult):
            The result for the audience mutate.

            This field is a member of `oneof`_ ``response``.
        bidding_data_exclusion_result (google.ads.googleads.v12.services.types.MutateBiddingDataExclusionsResult):
            The result for the bidding data exclusion
            mutate.

            This field is a member of `oneof`_ ``response``.
        bidding_seasonality_adjustment_result (google.ads.googleads.v12.services.types.MutateBiddingSeasonalityAdjustmentsResult):
            The result for the bidding seasonality
            adjustment mutate.

            This field is a member of `oneof`_ ``response``.
        bidding_strategy_result (google.ads.googleads.v12.services.types.MutateBiddingStrategyResult):
            The result for the bidding strategy mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_asset_result (google.ads.googleads.v12.services.types.MutateCampaignAssetResult):
            The result for the campaign asset mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_asset_set_result (google.ads.googleads.v12.services.types.MutateCampaignAssetSetResult):
            The result for the campaign asset set mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_bid_modifier_result (google.ads.googleads.v12.services.types.MutateCampaignBidModifierResult):
            The result for the campaign bid modifier
            mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_budget_result (google.ads.googleads.v12.services.types.MutateCampaignBudgetResult):
            The result for the campaign budget mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_conversion_goal_result (google.ads.googleads.v12.services.types.MutateCampaignConversionGoalResult):
            The result for the campaign conversion goal
            mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_criterion_result (google.ads.googleads.v12.services.types.MutateCampaignCriterionResult):
            The result for the campaign criterion mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_customizer_result (google.ads.googleads.v12.services.types.MutateCampaignCustomizerResult):
            The result for the campaign customizer
            mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_draft_result (google.ads.googleads.v12.services.types.MutateCampaignDraftResult):
            The result for the campaign draft mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_extension_setting_result (google.ads.googleads.v12.services.types.MutateCampaignExtensionSettingResult):
            The result for the campaign extension setting
            mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_feed_result (google.ads.googleads.v12.services.types.MutateCampaignFeedResult):
            The result for the campaign feed mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_group_result (google.ads.googleads.v12.services.types.MutateCampaignGroupResult):
            The result for the campaign group mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_label_result (google.ads.googleads.v12.services.types.MutateCampaignLabelResult):
            The result for the campaign label mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_result (google.ads.googleads.v12.services.types.MutateCampaignResult):
            The result for the campaign mutate.

            This field is a member of `oneof`_ ``response``.
        campaign_shared_set_result (google.ads.googleads.v12.services.types.MutateCampaignSharedSetResult):
            The result for the campaign shared set
            mutate.

            This field is a member of `oneof`_ ``response``.
        conversion_action_result (google.ads.googleads.v12.services.types.MutateConversionActionResult):
            The result for the conversion action mutate.

            This field is a member of `oneof`_ ``response``.
        conversion_custom_variable_result (google.ads.googleads.v12.services.types.MutateConversionCustomVariableResult):
            The result for the conversion custom variable
            mutate.

            This field is a member of `oneof`_ ``response``.
        conversion_goal_campaign_config_result (google.ads.googleads.v12.services.types.MutateConversionGoalCampaignConfigResult):
            The result for the conversion goal campaign
            config mutate.

            This field is a member of `oneof`_ ``response``.
        conversion_value_rule_result (google.ads.googleads.v12.services.types.MutateConversionValueRuleResult):
            The result for the conversion value rule
            mutate.

            This field is a member of `oneof`_ ``response``.
        conversion_value_rule_set_result (google.ads.googleads.v12.services.types.MutateConversionValueRuleSetResult):
            The result for the conversion value rule set
            mutate.

            This field is a member of `oneof`_ ``response``.
        custom_conversion_goal_result (google.ads.googleads.v12.services.types.MutateCustomConversionGoalResult):
            The result for the custom conversion goal
            mutate.

            This field is a member of `oneof`_ ``response``.
        customer_asset_result (google.ads.googleads.v12.services.types.MutateCustomerAssetResult):
            The result for the customer asset mutate.

            This field is a member of `oneof`_ ``response``.
        customer_conversion_goal_result (google.ads.googleads.v12.services.types.MutateCustomerConversionGoalResult):
            The result for the customer conversion goal
            mutate.

            This field is a member of `oneof`_ ``response``.
        customer_customizer_result (google.ads.googleads.v12.services.types.MutateCustomerCustomizerResult):
            The result for the customer customizer
            mutate.

            This field is a member of `oneof`_ ``response``.
        customer_extension_setting_result (google.ads.googleads.v12.services.types.MutateCustomerExtensionSettingResult):
            The result for the customer extension setting
            mutate.

            This field is a member of `oneof`_ ``response``.
        customer_feed_result (google.ads.googleads.v12.services.types.MutateCustomerFeedResult):
            The result for the customer feed mutate.

            This field is a member of `oneof`_ ``response``.
        customer_label_result (google.ads.googleads.v12.services.types.MutateCustomerLabelResult):
            The result for the customer label mutate.

            This field is a member of `oneof`_ ``response``.
        customer_negative_criterion_result (google.ads.googleads.v12.services.types.MutateCustomerNegativeCriteriaResult):
            The result for the customer negative
            criterion mutate.

            This field is a member of `oneof`_ ``response``.
        customer_result (google.ads.googleads.v12.services.types.MutateCustomerResult):
            The result for the customer mutate.

            This field is a member of `oneof`_ ``response``.
        customizer_attribute_result (google.ads.googleads.v12.services.types.MutateCustomizerAttributeResult):
            The result for the customizer attribute
            mutate.

            This field is a member of `oneof`_ ``response``.
        experiment_result (google.ads.googleads.v12.services.types.MutateExperimentResult):
            The result for the experiment mutate.

            This field is a member of `oneof`_ ``response``.
        experiment_arm_result (google.ads.googleads.v12.services.types.MutateExperimentArmResult):
            The result for the experiment arm mutate.

            This field is a member of `oneof`_ ``response``.
        extension_feed_item_result (google.ads.googleads.v12.services.types.MutateExtensionFeedItemResult):
            The result for the extension feed item
            mutate.

            This field is a member of `oneof`_ ``response``.
        feed_item_result (google.ads.googleads.v12.services.types.MutateFeedItemResult):
            The result for the feed item mutate.

            This field is a member of `oneof`_ ``response``.
        feed_item_set_result (google.ads.googleads.v12.services.types.MutateFeedItemSetResult):
            The result for the feed item set mutate.

            This field is a member of `oneof`_ ``response``.
        feed_item_set_link_result (google.ads.googleads.v12.services.types.MutateFeedItemSetLinkResult):
            The result for the feed item set link mutate.

            This field is a member of `oneof`_ ``response``.
        feed_item_target_result (google.ads.googleads.v12.services.types.MutateFeedItemTargetResult):
            The result for the feed item target mutate.

            This field is a member of `oneof`_ ``response``.
        feed_mapping_result (google.ads.googleads.v12.services.types.MutateFeedMappingResult):
            The result for the feed mapping mutate.

            This field is a member of `oneof`_ ``response``.
        feed_result (google.ads.googleads.v12.services.types.MutateFeedResult):
            The result for the feed mutate.

            This field is a member of `oneof`_ ``response``.
        keyword_plan_ad_group_result (google.ads.googleads.v12.services.types.MutateKeywordPlanAdGroupResult):
            The result for the keyword plan ad group
            mutate.

            This field is a member of `oneof`_ ``response``.
        keyword_plan_campaign_result (google.ads.googleads.v12.services.types.MutateKeywordPlanCampaignResult):
            The result for the keyword plan campaign
            mutate.

            This field is a member of `oneof`_ ``response``.
        keyword_plan_ad_group_keyword_result (google.ads.googleads.v12.services.types.MutateKeywordPlanAdGroupKeywordResult):
            The result for the keyword plan ad group
            keyword mutate.

            This field is a member of `oneof`_ ``response``.
        keyword_plan_campaign_keyword_result (google.ads.googleads.v12.services.types.MutateKeywordPlanCampaignKeywordResult):
            The result for the keyword plan campaign
            keyword mutate.

            This field is a member of `oneof`_ ``response``.
        keyword_plan_result (google.ads.googleads.v12.services.types.MutateKeywordPlansResult):
            The result for the keyword plan mutate.

            This field is a member of `oneof`_ ``response``.
        label_result (google.ads.googleads.v12.services.types.MutateLabelResult):
            The result for the label mutate.

            This field is a member of `oneof`_ ``response``.
        media_file_result (google.ads.googleads.v12.services.types.MutateMediaFileResult):
            The result for the media file mutate.

            This field is a member of `oneof`_ ``response``.
        remarketing_action_result (google.ads.googleads.v12.services.types.MutateRemarketingActionResult):
            The result for the remarketing action mutate.

            This field is a member of `oneof`_ ``response``.
        shared_criterion_result (google.ads.googleads.v12.services.types.MutateSharedCriterionResult):
            The result for the shared criterion mutate.

            This field is a member of `oneof`_ ``response``.
        shared_set_result (google.ads.googleads.v12.services.types.MutateSharedSetResult):
            The result for the shared set mutate.

            This field is a member of `oneof`_ ``response``.
        smart_campaign_setting_result (google.ads.googleads.v12.services.types.MutateSmartCampaignSettingResult):
            The result for the Smart campaign setting
            mutate.

            This field is a member of `oneof`_ ``response``.
        user_list_result (google.ads.googleads.v12.services.types.MutateUserListResult):
            The result for the user list mutate.

            This field is a member of `oneof`_ ``response``.
    """

    ad_group_ad_label_result = proto.Field(
        proto.MESSAGE,
        number=17,
        oneof="response",
        message=ad_group_ad_label_service.MutateAdGroupAdLabelResult,
    )
    ad_group_ad_result = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="response",
        message=ad_group_ad_service.MutateAdGroupAdResult,
    )
    ad_group_asset_result = proto.Field(
        proto.MESSAGE,
        number=56,
        oneof="response",
        message=ad_group_asset_service.MutateAdGroupAssetResult,
    )
    ad_group_bid_modifier_result = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="response",
        message=ad_group_bid_modifier_service.MutateAdGroupBidModifierResult,
    )
    ad_group_criterion_customizer_result = proto.Field(
        proto.MESSAGE,
        number=77,
        oneof="response",
        message=ad_group_criterion_customizer_service.MutateAdGroupCriterionCustomizerResult,
    )
    ad_group_criterion_label_result = proto.Field(
        proto.MESSAGE,
        number=18,
        oneof="response",
        message=ad_group_criterion_label_service.MutateAdGroupCriterionLabelResult,
    )
    ad_group_criterion_result = proto.Field(
        proto.MESSAGE,
        number=3,
        oneof="response",
        message=ad_group_criterion_service.MutateAdGroupCriterionResult,
    )
    ad_group_customizer_result = proto.Field(
        proto.MESSAGE,
        number=75,
        oneof="response",
        message=ad_group_customizer_service.MutateAdGroupCustomizerResult,
    )
    ad_group_extension_setting_result = proto.Field(
        proto.MESSAGE,
        number=19,
        oneof="response",
        message=ad_group_extension_setting_service.MutateAdGroupExtensionSettingResult,
    )
    ad_group_feed_result = proto.Field(
        proto.MESSAGE,
        number=20,
        oneof="response",
        message=ad_group_feed_service.MutateAdGroupFeedResult,
    )
    ad_group_label_result = proto.Field(
        proto.MESSAGE,
        number=21,
        oneof="response",
        message=ad_group_label_service.MutateAdGroupLabelResult,
    )
    ad_group_result = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="response",
        message=ad_group_service.MutateAdGroupResult,
    )
    ad_parameter_result = proto.Field(
        proto.MESSAGE,
        number=22,
        oneof="response",
        message=ad_parameter_service.MutateAdParameterResult,
    )
    ad_result = proto.Field(
        proto.MESSAGE,
        number=49,
        oneof="response",
        message=ad_service.MutateAdResult,
    )
    asset_result = proto.Field(
        proto.MESSAGE,
        number=23,
        oneof="response",
        message=asset_service.MutateAssetResult,
    )
    asset_group_asset_result = proto.Field(
        proto.MESSAGE,
        number=65,
        oneof="response",
        message=asset_group_asset_service.MutateAssetGroupAssetResult,
    )
    asset_group_listing_group_filter_result = proto.Field(
        proto.MESSAGE,
        number=78,
        oneof="response",
        message=asset_group_listing_group_filter_service.MutateAssetGroupListingGroupFilterResult,
    )
    asset_group_signal_result = proto.Field(
        proto.MESSAGE,
        number=79,
        oneof="response",
        message=asset_group_signal_service.MutateAssetGroupSignalResult,
    )
    asset_group_result = proto.Field(
        proto.MESSAGE,
        number=62,
        oneof="response",
        message=asset_group_service.MutateAssetGroupResult,
    )
    asset_set_asset_result = proto.Field(
        proto.MESSAGE,
        number=71,
        oneof="response",
        message=asset_set_asset_service.MutateAssetSetAssetResult,
    )
    asset_set_result = proto.Field(
        proto.MESSAGE,
        number=72,
        oneof="response",
        message=asset_set_service.MutateAssetSetResult,
    )
    audience_result = proto.Field(
        proto.MESSAGE,
        number=80,
        oneof="response",
        message=audience_service.MutateAudienceResult,
    )
    bidding_data_exclusion_result = proto.Field(
        proto.MESSAGE,
        number=58,
        oneof="response",
        message=bidding_data_exclusion_service.MutateBiddingDataExclusionsResult,
    )
    bidding_seasonality_adjustment_result = proto.Field(
        proto.MESSAGE,
        number=59,
        oneof="response",
        message=bidding_seasonality_adjustment_service.MutateBiddingSeasonalityAdjustmentsResult,
    )
    bidding_strategy_result = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="response",
        message=bidding_strategy_service.MutateBiddingStrategyResult,
    )
    campaign_asset_result = proto.Field(
        proto.MESSAGE,
        number=52,
        oneof="response",
        message=campaign_asset_service.MutateCampaignAssetResult,
    )
    campaign_asset_set_result = proto.Field(
        proto.MESSAGE,
        number=73,
        oneof="response",
        message=campaign_asset_set_service.MutateCampaignAssetSetResult,
    )
    campaign_bid_modifier_result = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="response",
        message=campaign_bid_modifier_service.MutateCampaignBidModifierResult,
    )
    campaign_budget_result = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="response",
        message=campaign_budget_service.MutateCampaignBudgetResult,
    )
    campaign_conversion_goal_result = proto.Field(
        proto.MESSAGE,
        number=67,
        oneof="response",
        message=campaign_conversion_goal_service.MutateCampaignConversionGoalResult,
    )
    campaign_criterion_result = proto.Field(
        proto.MESSAGE,
        number=13,
        oneof="response",
        message=campaign_criterion_service.MutateCampaignCriterionResult,
    )
    campaign_customizer_result = proto.Field(
        proto.MESSAGE,
        number=76,
        oneof="response",
        message=campaign_customizer_service.MutateCampaignCustomizerResult,
    )
    campaign_draft_result = proto.Field(
        proto.MESSAGE,
        number=24,
        oneof="response",
        message=campaign_draft_service.MutateCampaignDraftResult,
    )
    campaign_extension_setting_result = proto.Field(
        proto.MESSAGE,
        number=26,
        oneof="response",
        message=campaign_extension_setting_service.MutateCampaignExtensionSettingResult,
    )
    campaign_feed_result = proto.Field(
        proto.MESSAGE,
        number=27,
        oneof="response",
        message=campaign_feed_service.MutateCampaignFeedResult,
    )
    campaign_group_result = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="response",
        message=campaign_group_service.MutateCampaignGroupResult,
    )
    campaign_label_result = proto.Field(
        proto.MESSAGE,
        number=28,
        oneof="response",
        message=campaign_label_service.MutateCampaignLabelResult,
    )
    campaign_result = proto.Field(
        proto.MESSAGE,
        number=10,
        oneof="response",
        message=campaign_service.MutateCampaignResult,
    )
    campaign_shared_set_result = proto.Field(
        proto.MESSAGE,
        number=11,
        oneof="response",
        message=campaign_shared_set_service.MutateCampaignSharedSetResult,
    )
    conversion_action_result = proto.Field(
        proto.MESSAGE,
        number=12,
        oneof="response",
        message=conversion_action_service.MutateConversionActionResult,
    )
    conversion_custom_variable_result = proto.Field(
        proto.MESSAGE,
        number=55,
        oneof="response",
        message=conversion_custom_variable_service.MutateConversionCustomVariableResult,
    )
    conversion_goal_campaign_config_result = proto.Field(
        proto.MESSAGE,
        number=69,
        oneof="response",
        message=conversion_goal_campaign_config_service.MutateConversionGoalCampaignConfigResult,
    )
    conversion_value_rule_result = proto.Field(
        proto.MESSAGE,
        number=63,
        oneof="response",
        message=conversion_value_rule_service.MutateConversionValueRuleResult,
    )
    conversion_value_rule_set_result = proto.Field(
        proto.MESSAGE,
        number=64,
        oneof="response",
        message=conversion_value_rule_set_service.MutateConversionValueRuleSetResult,
    )
    custom_conversion_goal_result = proto.Field(
        proto.MESSAGE,
        number=68,
        oneof="response",
        message=custom_conversion_goal_service.MutateCustomConversionGoalResult,
    )
    customer_asset_result = proto.Field(
        proto.MESSAGE,
        number=57,
        oneof="response",
        message=customer_asset_service.MutateCustomerAssetResult,
    )
    customer_conversion_goal_result = proto.Field(
        proto.MESSAGE,
        number=66,
        oneof="response",
        message=customer_conversion_goal_service.MutateCustomerConversionGoalResult,
    )
    customer_customizer_result = proto.Field(
        proto.MESSAGE,
        number=74,
        oneof="response",
        message=customer_customizer_service.MutateCustomerCustomizerResult,
    )
    customer_extension_setting_result = proto.Field(
        proto.MESSAGE,
        number=30,
        oneof="response",
        message=customer_extension_setting_service.MutateCustomerExtensionSettingResult,
    )
    customer_feed_result = proto.Field(
        proto.MESSAGE,
        number=31,
        oneof="response",
        message=customer_feed_service.MutateCustomerFeedResult,
    )
    customer_label_result = proto.Field(
        proto.MESSAGE,
        number=32,
        oneof="response",
        message=customer_label_service.MutateCustomerLabelResult,
    )
    customer_negative_criterion_result = proto.Field(
        proto.MESSAGE,
        number=34,
        oneof="response",
        message=customer_negative_criterion_service.MutateCustomerNegativeCriteriaResult,
    )
    customer_result = proto.Field(
        proto.MESSAGE,
        number=35,
        oneof="response",
        message=customer_service.MutateCustomerResult,
    )
    customizer_attribute_result = proto.Field(
        proto.MESSAGE,
        number=70,
        oneof="response",
        message=customizer_attribute_service.MutateCustomizerAttributeResult,
    )
    experiment_result = proto.Field(
        proto.MESSAGE,
        number=81,
        oneof="response",
        message=experiment_service.MutateExperimentResult,
    )
    experiment_arm_result = proto.Field(
        proto.MESSAGE,
        number=82,
        oneof="response",
        message=experiment_arm_service.MutateExperimentArmResult,
    )
    extension_feed_item_result = proto.Field(
        proto.MESSAGE,
        number=36,
        oneof="response",
        message=extension_feed_item_service.MutateExtensionFeedItemResult,
    )
    feed_item_result = proto.Field(
        proto.MESSAGE,
        number=37,
        oneof="response",
        message=feed_item_service.MutateFeedItemResult,
    )
    feed_item_set_result = proto.Field(
        proto.MESSAGE,
        number=53,
        oneof="response",
        message=feed_item_set_service.MutateFeedItemSetResult,
    )
    feed_item_set_link_result = proto.Field(
        proto.MESSAGE,
        number=54,
        oneof="response",
        message=feed_item_set_link_service.MutateFeedItemSetLinkResult,
    )
    feed_item_target_result = proto.Field(
        proto.MESSAGE,
        number=38,
        oneof="response",
        message=feed_item_target_service.MutateFeedItemTargetResult,
    )
    feed_mapping_result = proto.Field(
        proto.MESSAGE,
        number=39,
        oneof="response",
        message=feed_mapping_service.MutateFeedMappingResult,
    )
    feed_result = proto.Field(
        proto.MESSAGE,
        number=40,
        oneof="response",
        message=feed_service.MutateFeedResult,
    )
    keyword_plan_ad_group_result = proto.Field(
        proto.MESSAGE,
        number=44,
        oneof="response",
        message=keyword_plan_ad_group_service.MutateKeywordPlanAdGroupResult,
    )
    keyword_plan_campaign_result = proto.Field(
        proto.MESSAGE,
        number=45,
        oneof="response",
        message=keyword_plan_campaign_service.MutateKeywordPlanCampaignResult,
    )
    keyword_plan_ad_group_keyword_result = proto.Field(
        proto.MESSAGE,
        number=50,
        oneof="response",
        message=keyword_plan_ad_group_keyword_service.MutateKeywordPlanAdGroupKeywordResult,
    )
    keyword_plan_campaign_keyword_result = proto.Field(
        proto.MESSAGE,
        number=51,
        oneof="response",
        message=keyword_plan_campaign_keyword_service.MutateKeywordPlanCampaignKeywordResult,
    )
    keyword_plan_result = proto.Field(
        proto.MESSAGE,
        number=48,
        oneof="response",
        message=keyword_plan_service.MutateKeywordPlansResult,
    )
    label_result = proto.Field(
        proto.MESSAGE,
        number=41,
        oneof="response",
        message=label_service.MutateLabelResult,
    )
    media_file_result = proto.Field(
        proto.MESSAGE,
        number=42,
        oneof="response",
        message=media_file_service.MutateMediaFileResult,
    )
    remarketing_action_result = proto.Field(
        proto.MESSAGE,
        number=43,
        oneof="response",
        message=remarketing_action_service.MutateRemarketingActionResult,
    )
    shared_criterion_result = proto.Field(
        proto.MESSAGE,
        number=14,
        oneof="response",
        message=shared_criterion_service.MutateSharedCriterionResult,
    )
    shared_set_result = proto.Field(
        proto.MESSAGE,
        number=15,
        oneof="response",
        message=shared_set_service.MutateSharedSetResult,
    )
    smart_campaign_setting_result = proto.Field(
        proto.MESSAGE,
        number=61,
        oneof="response",
        message=smart_campaign_setting_service.MutateSmartCampaignSettingResult,
    )
    user_list_result = proto.Field(
        proto.MESSAGE,
        number=16,
        oneof="response",
        message=user_list_service.MutateUserListResult,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
