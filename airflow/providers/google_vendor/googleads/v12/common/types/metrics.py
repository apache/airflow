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

from airflow.providers.google_vendor.googleads.v12.enums.types import interaction_event_type
from airflow.providers.google_vendor.googleads.v12.enums.types import quality_score_bucket


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"Metrics",},
)


class Metrics(proto.Message):
    r"""Metrics data.

    Attributes:
        absolute_top_impression_percentage (float):
            The percent of your ad impressions that are
            shown as the very first ad above the organic
            search results.

            This field is a member of `oneof`_ ``_absolute_top_impression_percentage``.
        active_view_cpm (float):
            Average cost of viewable impressions
            (``active_view_impressions``).

            This field is a member of `oneof`_ ``_active_view_cpm``.
        active_view_ctr (float):
            Active view measurable clicks divided by
            active view viewable impressions. This metric is
            reported only for display network.

            This field is a member of `oneof`_ ``_active_view_ctr``.
        active_view_impressions (int):
            A measurement of how often your ad has become
            viewable on a Display Network site.

            This field is a member of `oneof`_ ``_active_view_impressions``.
        active_view_measurability (float):
            The ratio of impressions that could be
            measured by Active View over the number of
            served impressions.

            This field is a member of `oneof`_ ``_active_view_measurability``.
        active_view_measurable_cost_micros (int):
            The cost of the impressions you received that
            were measurable by Active View.

            This field is a member of `oneof`_ ``_active_view_measurable_cost_micros``.
        active_view_measurable_impressions (int):
            The number of times your ads are appearing on
            placements in positions where they can be seen.

            This field is a member of `oneof`_ ``_active_view_measurable_impressions``.
        active_view_viewability (float):
            The percentage of time when your ad appeared
            on an Active View enabled site (measurable
            impressions) and was viewable (viewable
            impressions).

            This field is a member of `oneof`_ ``_active_view_viewability``.
        all_conversions_from_interactions_rate (float):
            All conversions from interactions (as oppose
            to view through conversions) divided by the
            number of ad interactions.

            This field is a member of `oneof`_ ``_all_conversions_from_interactions_rate``.
        all_conversions_value (float):
            The value of all conversions.

            This field is a member of `oneof`_ ``_all_conversions_value``.
        all_conversions_value_by_conversion_date (float):
            The value of all conversions. When this column is selected
            with date, the values in date column means the conversion
            date. Details for the by_conversion_date columns are
            available at
            https://support.google.com/google-ads/answer/9549009.
        all_conversions (float):
            The total number of conversions. This includes all
            conversions regardless of the value of
            include_in_conversions_metric.

            This field is a member of `oneof`_ ``_all_conversions``.
        all_conversions_by_conversion_date (float):
            The total number of conversions. This includes all
            conversions regardless of the value of
            include_in_conversions_metric. When this column is selected
            with date, the values in date column means the conversion
            date. Details for the by_conversion_date columns are
            available at
            https://support.google.com/google-ads/answer/9549009.
        all_conversions_value_per_cost (float):
            The value of all conversions divided by the
            total cost of ad interactions (such as clicks
            for text ads or views for video ads).

            This field is a member of `oneof`_ ``_all_conversions_value_per_cost``.
        all_conversions_from_click_to_call (float):
            The number of times people clicked the "Call"
            button to call a store during or after clicking
            an ad. This number doesn't include whether or
            not calls were connected, or the duration of any
            calls. This metric applies to feed items only.

            This field is a member of `oneof`_ ``_all_conversions_from_click_to_call``.
        all_conversions_from_directions (float):
            The number of times people clicked a "Get
            directions" button to navigate to a store after
            clicking an ad. This metric applies to feed
            items only.

            This field is a member of `oneof`_ ``_all_conversions_from_directions``.
        all_conversions_from_interactions_value_per_interaction (float):
            The value of all conversions from
            interactions divided by the total number of
            interactions.

            This field is a member of `oneof`_ ``_all_conversions_from_interactions_value_per_interaction``.
        all_conversions_from_menu (float):
            The number of times people clicked a link to
            view a store's menu after clicking an ad.
            This metric applies to feed items only.

            This field is a member of `oneof`_ ``_all_conversions_from_menu``.
        all_conversions_from_order (float):
            The number of times people placed an order at
            a store after clicking an ad. This metric
            applies to feed items only.

            This field is a member of `oneof`_ ``_all_conversions_from_order``.
        all_conversions_from_other_engagement (float):
            The number of other conversions (for example,
            posting a review or saving a location for a
            store) that occurred after people clicked an ad.
            This metric applies to feed items only.

            This field is a member of `oneof`_ ``_all_conversions_from_other_engagement``.
        all_conversions_from_store_visit (float):
            Estimated number of times people visited a
            store after clicking an ad. This metric applies
            to feed items only.

            This field is a member of `oneof`_ ``_all_conversions_from_store_visit``.
        all_conversions_from_store_website (float):
            The number of times that people were taken to
            a store's URL after clicking an ad.
            This metric applies to feed items only.

            This field is a member of `oneof`_ ``_all_conversions_from_store_website``.
        auction_insight_search_absolute_top_impression_percentage (float):
            This metric is part of the Auction Insights
            report, and tells how often the ads of another
            participant showed as the very first ad above
            the organic search results.
            This percentage is computed only over the
            auctions that you appeared in the page.
            This metric is not publicly available.

            This field is a member of `oneof`_ ``_auction_insight_search_absolute_top_impression_percentage``.
        auction_insight_search_impression_share (float):
            This metric is part of the Auction Insights
            report, and tells the percentage of impressions
            that another participant obtained, over the
            total number of impressions that your ads were
            eligible for. Any value below 0.1 is reported as
            0.0999.
            This metric is not publicly available.

            This field is a member of `oneof`_ ``_auction_insight_search_impression_share``.
        auction_insight_search_outranking_share (float):
            This metric is part of the Auction Insights
            report, and tells the percentage of impressions
            that your ads outranked (showed above) another
            participant in the auction, compared to the
            total number of impressions that your ads were
            eligible for.
            Any value below 0.1 is reported as 0.0999.
            This metric is not publicly available.

            This field is a member of `oneof`_ ``_auction_insight_search_outranking_share``.
        auction_insight_search_overlap_rate (float):
            This metric is part of the Auction Insights
            report, and tells how often another
            participant's ad received an impression when
            your ad also received an impression.
            This metric is not publicly available.

            This field is a member of `oneof`_ ``_auction_insight_search_overlap_rate``.
        auction_insight_search_position_above_rate (float):
            This metric is part of the Auction Insights
            report, and tells how often another
            participant's ad was shown in a higher position
            than yours, when both of your ads were shown at
            the same page. This metric is not publicly
            available.

            This field is a member of `oneof`_ ``_auction_insight_search_position_above_rate``.
        auction_insight_search_top_impression_percentage (float):
            This metric is part of the Auction Insights
            report, and tells how often the ads of another
            participant showed above the organic search
            results. This percentage is computed only over
            the auctions that you appeared in the page.
            This metric is not publicly available.

            This field is a member of `oneof`_ ``_auction_insight_search_top_impression_percentage``.
        average_cost (float):
            The average amount you pay per interaction.
            This amount is the total cost of your ads
            divided by the total number of interactions.

            This field is a member of `oneof`_ ``_average_cost``.
        average_cpc (float):
            The total cost of all clicks divided by the
            total number of clicks received.

            This field is a member of `oneof`_ ``_average_cpc``.
        average_cpe (float):
            The average amount that you've been charged
            for an ad engagement. This amount is the total
            cost of all ad engagements divided by the total
            number of ad engagements.

            This field is a member of `oneof`_ ``_average_cpe``.
        average_cpm (float):
            Average cost-per-thousand impressions (CPM).

            This field is a member of `oneof`_ ``_average_cpm``.
        average_cpv (float):
            The average amount you pay each time someone
            views your ad. The average CPV is defined by the
            total cost of all ad views divided by the number
            of views.

            This field is a member of `oneof`_ ``_average_cpv``.
        average_page_views (float):
            Average number of pages viewed per session.

            This field is a member of `oneof`_ ``_average_page_views``.
        average_time_on_site (float):
            Total duration of all sessions (in seconds) /
            number of sessions. Imported from Google
            Analytics.

            This field is a member of `oneof`_ ``_average_time_on_site``.
        benchmark_average_max_cpc (float):
            An indication of how other advertisers are
            bidding on similar products.

            This field is a member of `oneof`_ ``_benchmark_average_max_cpc``.
        biddable_app_install_conversions (float):
            Number of app installs.

            This field is a member of `oneof`_ ``_biddable_app_install_conversions``.
        biddable_app_post_install_conversions (float):
            Number of in-app actions.

            This field is a member of `oneof`_ ``_biddable_app_post_install_conversions``.
        benchmark_ctr (float):
            An indication on how other advertisers'
            Shopping ads for similar products are performing
            based on how often people who see their ad click
            on it.

            This field is a member of `oneof`_ ``_benchmark_ctr``.
        bounce_rate (float):
            Percentage of clicks where the user only
            visited a single page on your site. Imported
            from Google Analytics.

            This field is a member of `oneof`_ ``_bounce_rate``.
        clicks (int):
            The number of clicks.

            This field is a member of `oneof`_ ``_clicks``.
        combined_clicks (int):
            The number of times your ad or your site's
            listing in the unpaid results was clicked. See
            the help page at
            https://support.google.com/google-ads/answer/3097241
            for details.

            This field is a member of `oneof`_ ``_combined_clicks``.
        combined_clicks_per_query (float):
            The number of times your ad or your site's listing in the
            unpaid results was clicked (combined_clicks) divided by
            combined_queries. See the help page at
            https://support.google.com/google-ads/answer/3097241 for
            details.

            This field is a member of `oneof`_ ``_combined_clicks_per_query``.
        combined_queries (int):
            The number of searches that returned pages
            from your site in the unpaid results or showed
            one of your text ads. See the help page at
            https://support.google.com/google-ads/answer/3097241
            for details.

            This field is a member of `oneof`_ ``_combined_queries``.
        content_budget_lost_impression_share (float):
            The estimated percent of times that your ad
            was eligible to show on the Display Network but
            didn't because your budget was too low. Note:
            Content budget lost impression share is reported
            in the range of 0 to 0.9. Any value above 0.9 is
            reported as 0.9001.

            This field is a member of `oneof`_ ``_content_budget_lost_impression_share``.
        content_impression_share (float):
            The impressions you've received on the
            Display Network divided by the estimated number
            of impressions you were eligible to receive.
            Note: Content impression share is reported in
            the range of 0.1 to 1. Any value below 0.1 is
            reported as 0.0999.

            This field is a member of `oneof`_ ``_content_impression_share``.
        conversion_last_received_request_date_time (str):
            The last date/time a conversion tag for this
            conversion action successfully fired and was
            seen by Google Ads. This firing event may not
            have been the result of an attributable
            conversion (for example, because the tag was
            fired from a browser that did not previously
            click an ad from an appropriate advertiser). The
            date/time is in the customer's time zone.

            This field is a member of `oneof`_ ``_conversion_last_received_request_date_time``.
        conversion_last_conversion_date (str):
            The date of the most recent conversion for
            this conversion action. The date is in the
            customer's time zone.

            This field is a member of `oneof`_ ``_conversion_last_conversion_date``.
        content_rank_lost_impression_share (float):
            The estimated percentage of impressions on
            the Display Network that your ads didn't receive
            due to poor Ad Rank. Note: Content rank lost
            impression share is reported in the range of 0
            to 0.9. Any value above 0.9 is reported as
            0.9001.

            This field is a member of `oneof`_ ``_content_rank_lost_impression_share``.
        conversions_from_interactions_rate (float):
            Conversions from interactions divided by the number of ad
            interactions (such as clicks for text ads or views for video
            ads). This only includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions.

            This field is a member of `oneof`_ ``_conversions_from_interactions_rate``.
        conversions_value (float):
            The value of conversions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions.

            This field is a member of `oneof`_ ``_conversions_value``.
        conversions_value_by_conversion_date (float):
            The value of conversions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions. When this
            column is selected with date, the values in date column
            means the conversion date. Details for the
            by_conversion_date columns are available at
            https://support.google.com/google-ads/answer/9549009.
        conversions_value_per_cost (float):
            The value of conversions divided by the cost of ad
            interactions. This only includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions.

            This field is a member of `oneof`_ ``_conversions_value_per_cost``.
        conversions_from_interactions_value_per_interaction (float):
            The value of conversions from interactions divided by the
            number of ad interactions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions.

            This field is a member of `oneof`_ ``_conversions_from_interactions_value_per_interaction``.
        conversions (float):
            The number of conversions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions.

            This field is a member of `oneof`_ ``_conversions``.
        conversions_by_conversion_date (float):
            The number of conversions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions. When this
            column is selected with date, the values in date column
            means the conversion date. Details for the
            by_conversion_date columns are available at
            https://support.google.com/google-ads/answer/9549009.
        cost_micros (int):
            The sum of your cost-per-click (CPC) and
            cost-per-thousand impressions (CPM) costs during
            this period.

            This field is a member of `oneof`_ ``_cost_micros``.
        cost_per_all_conversions (float):
            The cost of ad interactions divided by all
            conversions.

            This field is a member of `oneof`_ ``_cost_per_all_conversions``.
        cost_per_conversion (float):
            The cost of ad interactions divided by conversions. This
            only includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions.

            This field is a member of `oneof`_ ``_cost_per_conversion``.
        cost_per_current_model_attributed_conversion (float):
            The cost of ad interactions divided by current model
            attributed conversions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions.

            This field is a member of `oneof`_ ``_cost_per_current_model_attributed_conversion``.
        cross_device_conversions (float):
            Conversions from when a customer clicks on a Google Ads ad
            on one device, then converts on a different device or
            browser. Cross-device conversions are already included in
            all_conversions.

            This field is a member of `oneof`_ ``_cross_device_conversions``.
        ctr (float):
            The number of clicks your ad receives
            (Clicks) divided by the number of times your ad
            is shown (Impressions).

            This field is a member of `oneof`_ ``_ctr``.
        current_model_attributed_conversions (float):
            Shows how your historic conversions data would look under
            the attribution model you've currently selected. This only
            includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions.

            This field is a member of `oneof`_ ``_current_model_attributed_conversions``.
        current_model_attributed_conversions_from_interactions_rate (float):
            Current model attributed conversions from interactions
            divided by the number of ad interactions (such as clicks for
            text ads or views for video ads). This only includes
            conversion actions which include_in_conversions_metric
            attribute is set to true. If you use conversion-based
            bidding, your bid strategies will optimize for these
            conversions.

            This field is a member of `oneof`_ ``_current_model_attributed_conversions_from_interactions_rate``.
        current_model_attributed_conversions_from_interactions_value_per_interaction (float):
            The value of current model attributed conversions from
            interactions divided by the number of ad interactions. This
            only includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions.

            This field is a member of `oneof`_ ``_current_model_attributed_conversions_from_interactions_value_per_interaction``.
        current_model_attributed_conversions_value (float):
            The value of current model attributed conversions. This only
            includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions.

            This field is a member of `oneof`_ ``_current_model_attributed_conversions_value``.
        current_model_attributed_conversions_value_per_cost (float):
            The value of current model attributed conversions divided by
            the cost of ad interactions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions.

            This field is a member of `oneof`_ ``_current_model_attributed_conversions_value_per_cost``.
        engagement_rate (float):
            How often people engage with your ad after
            it's shown to them. This is the number of ad
            expansions divided by the number of times your
            ad is shown.

            This field is a member of `oneof`_ ``_engagement_rate``.
        engagements (int):
            The number of engagements.
            An engagement occurs when a viewer expands your
            Lightbox ad. Also, in the future, other ad types
            may support engagement metrics.

            This field is a member of `oneof`_ ``_engagements``.
        hotel_average_lead_value_micros (float):
            Average lead value based on clicks.

            This field is a member of `oneof`_ ``_hotel_average_lead_value_micros``.
        hotel_commission_rate_micros (int):
            Commission bid rate in micros. A 20%
            commission is represented as 200,000.

            This field is a member of `oneof`_ ``_hotel_commission_rate_micros``.
        hotel_expected_commission_cost (float):
            Expected commission cost. The result of multiplying the
            commission value times the hotel_commission_rate in
            advertiser currency.

            This field is a member of `oneof`_ ``_hotel_expected_commission_cost``.
        hotel_price_difference_percentage (float):
            The average price difference between the
            price offered by reporting hotel advertiser and
            the cheapest price offered by the competing
            advertiser.

            This field is a member of `oneof`_ ``_hotel_price_difference_percentage``.
        hotel_eligible_impressions (int):
            The number of impressions that hotel partners
            could have had given their feed performance.

            This field is a member of `oneof`_ ``_hotel_eligible_impressions``.
        historical_creative_quality_score (google.ads.googleads.v12.enums.types.QualityScoreBucketEnum.QualityScoreBucket):
            The creative historical quality score.
        historical_landing_page_quality_score (google.ads.googleads.v12.enums.types.QualityScoreBucketEnum.QualityScoreBucket):
            The quality of historical landing page
            experience.
        historical_quality_score (int):
            The historical quality score.

            This field is a member of `oneof`_ ``_historical_quality_score``.
        historical_search_predicted_ctr (google.ads.googleads.v12.enums.types.QualityScoreBucketEnum.QualityScoreBucket):
            The historical search predicted click through
            rate (CTR).
        gmail_forwards (int):
            The number of times the ad was forwarded to
            someone else as a message.

            This field is a member of `oneof`_ ``_gmail_forwards``.
        gmail_saves (int):
            The number of times someone has saved your
            Gmail ad to their inbox as a message.

            This field is a member of `oneof`_ ``_gmail_saves``.
        gmail_secondary_clicks (int):
            The number of clicks to the landing page on
            the expanded state of Gmail ads.

            This field is a member of `oneof`_ ``_gmail_secondary_clicks``.
        impressions_from_store_reach (int):
            The number of times a store's location-based
            ad was shown. This metric applies to feed items
            only.

            This field is a member of `oneof`_ ``_impressions_from_store_reach``.
        impressions (int):
            Count of how often your ad has appeared on a
            search results page or website on the Google
            Network.

            This field is a member of `oneof`_ ``_impressions``.
        interaction_rate (float):
            How often people interact with your ad after
            it is shown to them. This is the number of
            interactions divided by the number of times your
            ad is shown.

            This field is a member of `oneof`_ ``_interaction_rate``.
        interactions (int):
            The number of interactions.
            An interaction is the main user action
            associated with an ad format-clicks for text and
            shopping ads, views for video ads, and so on.

            This field is a member of `oneof`_ ``_interactions``.
        interaction_event_types (Sequence[google.ads.googleads.v12.enums.types.InteractionEventTypeEnum.InteractionEventType]):
            The types of payable and free interactions.
        invalid_click_rate (float):
            The percentage of clicks filtered out of your
            total number of clicks (filtered + non-filtered
            clicks) during the reporting period.

            This field is a member of `oneof`_ ``_invalid_click_rate``.
        invalid_clicks (int):
            Number of clicks Google considers
            illegitimate and doesn't charge you for.

            This field is a member of `oneof`_ ``_invalid_clicks``.
        message_chats (int):
            Number of message chats initiated for Click
            To Message impressions that were message
            tracking eligible.

            This field is a member of `oneof`_ ``_message_chats``.
        message_impressions (int):
            Number of Click To Message impressions that
            were message tracking eligible.

            This field is a member of `oneof`_ ``_message_impressions``.
        message_chat_rate (float):
            Number of message chats initiated (message_chats) divided by
            the number of message impressions (message_impressions).
            Rate at which a user initiates a message chat from an ad
            impression with a messaging option and message tracking
            enabled. Note that this rate can be more than 1.0 for a
            given message impression.

            This field is a member of `oneof`_ ``_message_chat_rate``.
        mobile_friendly_clicks_percentage (float):
            The percentage of mobile clicks that go to a
            mobile-friendly page.

            This field is a member of `oneof`_ ``_mobile_friendly_clicks_percentage``.
        optimization_score_uplift (float):
            Total optimization score uplift of all
            recommendations.

            This field is a member of `oneof`_ ``_optimization_score_uplift``.
        optimization_score_url (str):
            URL for the optimization score page in the Google Ads web
            interface. This metric can be selected from ``customer`` or
            ``campaign``, and can be segmented by
            ``segments.recommendation_type``. For example,
            ``SELECT metrics.optimization_score_url, segments.recommendation_type FROM customer``
            will return a URL for each unique (customer,
            recommendation_type) combination.

            This field is a member of `oneof`_ ``_optimization_score_url``.
        organic_clicks (int):
            The number of times someone clicked your
            site's listing in the unpaid results for a
            particular query. See the help page at
            https://support.google.com/google-ads/answer/3097241
            for details.

            This field is a member of `oneof`_ ``_organic_clicks``.
        organic_clicks_per_query (float):
            The number of times someone clicked your site's listing in
            the unpaid results (organic_clicks) divided by the total
            number of searches that returned pages from your site
            (organic_queries). See the help page at
            https://support.google.com/google-ads/answer/3097241 for
            details.

            This field is a member of `oneof`_ ``_organic_clicks_per_query``.
        organic_impressions (int):
            The number of listings for your site in the
            unpaid search results. See the help page at
            https://support.google.com/google-ads/answer/3097241
            for details.

            This field is a member of `oneof`_ ``_organic_impressions``.
        organic_impressions_per_query (float):
            The number of times a page from your site was listed in the
            unpaid search results (organic_impressions) divided by the
            number of searches returning your site's listing in the
            unpaid results (organic_queries). See the help page at
            https://support.google.com/google-ads/answer/3097241 for
            details.

            This field is a member of `oneof`_ ``_organic_impressions_per_query``.
        organic_queries (int):
            The total number of searches that returned
            your site's listing in the unpaid results. See
            the help page at
            https://support.google.com/google-ads/answer/3097241
            for details.

            This field is a member of `oneof`_ ``_organic_queries``.
        percent_new_visitors (float):
            Percentage of first-time sessions (from
            people who had never visited your site before).
            Imported from Google Analytics.

            This field is a member of `oneof`_ ``_percent_new_visitors``.
        phone_calls (int):
            Number of offline phone calls.

            This field is a member of `oneof`_ ``_phone_calls``.
        phone_impressions (int):
            Number of offline phone impressions.

            This field is a member of `oneof`_ ``_phone_impressions``.
        phone_through_rate (float):
            Number of phone calls received (phone_calls) divided by the
            number of times your phone number is shown
            (phone_impressions).

            This field is a member of `oneof`_ ``_phone_through_rate``.
        relative_ctr (float):
            Your clickthrough rate (Ctr) divided by the
            average clickthrough rate of all advertisers on
            the websites that show your ads. Measures how
            your ads perform on Display Network sites
            compared to other ads on the same sites.

            This field is a member of `oneof`_ ``_relative_ctr``.
        search_absolute_top_impression_share (float):
            The percentage of the customer's Shopping or
            Search ad impressions that are shown in the most
            prominent Shopping position. See
            https://support.google.com/google-ads/answer/7501826
            for details. Any value below 0.1 is reported as
            0.0999.

            This field is a member of `oneof`_ ``_search_absolute_top_impression_share``.
        search_budget_lost_absolute_top_impression_share (float):
            The number estimating how often your ad
            wasn't the very first ad above the organic
            search results due to a low budget. Note: Search
            budget lost absolute top impression share is
            reported in the range of 0 to 0.9. Any value
            above 0.9 is reported as 0.9001.

            This field is a member of `oneof`_ ``_search_budget_lost_absolute_top_impression_share``.
        search_budget_lost_impression_share (float):
            The estimated percent of times that your ad
            was eligible to show on the Search Network but
            didn't because your budget was too low. Note:
            Search budget lost impression share is reported
            in the range of 0 to 0.9. Any value above 0.9 is
            reported as 0.9001.

            This field is a member of `oneof`_ ``_search_budget_lost_impression_share``.
        search_budget_lost_top_impression_share (float):
            The number estimating how often your ad
            didn't show anywhere above the organic search
            results due to a low budget. Note: Search budget
            lost top impression share is reported in the
            range of 0 to 0.9. Any value above 0.9 is
            reported as 0.9001.

            This field is a member of `oneof`_ ``_search_budget_lost_top_impression_share``.
        search_click_share (float):
            The number of clicks you've received on the
            Search Network divided by the estimated number
            of clicks you were eligible to receive. Note:
            Search click share is reported in the range of
            0.1 to 1. Any value below 0.1 is reported as
            0.0999.

            This field is a member of `oneof`_ ``_search_click_share``.
        search_exact_match_impression_share (float):
            The impressions you've received divided by
            the estimated number of impressions you were
            eligible to receive on the Search Network for
            search terms that matched your keywords exactly
            (or were close variants of your keyword),
            regardless of your keyword match types. Note:
            Search exact match impression share is reported
            in the range of 0.1 to 1. Any value below 0.1 is
            reported as 0.0999.

            This field is a member of `oneof`_ ``_search_exact_match_impression_share``.
        search_impression_share (float):
            The impressions you've received on the Search
            Network divided by the estimated number of
            impressions you were eligible to receive. Note:
            Search impression share is reported in the range
            of 0.1 to 1. Any value below 0.1 is reported as
            0.0999.

            This field is a member of `oneof`_ ``_search_impression_share``.
        search_rank_lost_absolute_top_impression_share (float):
            The number estimating how often your ad
            wasn't the very first ad above the organic
            search results due to poor Ad Rank. Note: Search
            rank lost absolute top impression share is
            reported in the range of 0 to 0.9. Any value
            above 0.9 is reported as 0.9001.

            This field is a member of `oneof`_ ``_search_rank_lost_absolute_top_impression_share``.
        search_rank_lost_impression_share (float):
            The estimated percentage of impressions on
            the Search Network that your ads didn't receive
            due to poor Ad Rank. Note: Search rank lost
            impression share is reported in the range of 0
            to 0.9. Any value above 0.9 is reported as
            0.9001.

            This field is a member of `oneof`_ ``_search_rank_lost_impression_share``.
        search_rank_lost_top_impression_share (float):
            The number estimating how often your ad
            didn't show anywhere above the organic search
            results due to poor Ad Rank. Note: Search rank
            lost top impression share is reported in the
            range of 0 to 0.9. Any value above 0.9 is
            reported as 0.9001.

            This field is a member of `oneof`_ ``_search_rank_lost_top_impression_share``.
        search_top_impression_share (float):
            The impressions you've received in the top
            location (anywhere above the organic search
            results) compared to the estimated number of
            impressions you were eligible to receive in the
            top location. Note: Search top impression share
            is reported in the range of 0.1 to 1. Any value
            below 0.1 is reported as 0.0999.

            This field is a member of `oneof`_ ``_search_top_impression_share``.
        speed_score (int):
            A measure of how quickly your page loads
            after clicks on your mobile ads. The score is a
            range from 1 to 10, 10 being the fastest.

            This field is a member of `oneof`_ ``_speed_score``.
        top_impression_percentage (float):
            The percent of your ad impressions that are
            shown anywhere above the organic search results.

            This field is a member of `oneof`_ ``_top_impression_percentage``.
        valid_accelerated_mobile_pages_clicks_percentage (float):
            The percentage of ad clicks to Accelerated
            Mobile Pages (AMP) landing pages that reach a
            valid AMP page.

            This field is a member of `oneof`_ ``_valid_accelerated_mobile_pages_clicks_percentage``.
        value_per_all_conversions (float):
            The value of all conversions divided by the
            number of all conversions.

            This field is a member of `oneof`_ ``_value_per_all_conversions``.
        value_per_all_conversions_by_conversion_date (float):
            The value of all conversions divided by the number of all
            conversions. When this column is selected with date, the
            values in date column means the conversion date. Details for
            the by_conversion_date columns are available at
            https://support.google.com/google-ads/answer/9549009.

            This field is a member of `oneof`_ ``_value_per_all_conversions_by_conversion_date``.
        value_per_conversion (float):
            The value of conversions divided by the number of
            conversions. This only includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions.

            This field is a member of `oneof`_ ``_value_per_conversion``.
        value_per_conversions_by_conversion_date (float):
            The value of conversions divided by the number of
            conversions. This only includes conversion actions which
            include_in_conversions_metric attribute is set to true. If
            you use conversion-based bidding, your bid strategies will
            optimize for these conversions. When this column is selected
            with date, the values in date column means the conversion
            date. Details for the by_conversion_date columns are
            available at
            https://support.google.com/google-ads/answer/9549009.

            This field is a member of `oneof`_ ``_value_per_conversions_by_conversion_date``.
        value_per_current_model_attributed_conversion (float):
            The value of current model attributed conversions divided by
            the number of the conversions. This only includes conversion
            actions which include_in_conversions_metric attribute is set
            to true. If you use conversion-based bidding, your bid
            strategies will optimize for these conversions.

            This field is a member of `oneof`_ ``_value_per_current_model_attributed_conversion``.
        video_quartile_p100_rate (float):
            Percentage of impressions where the viewer
            watched all of your video.

            This field is a member of `oneof`_ ``_video_quartile_p100_rate``.
        video_quartile_p25_rate (float):
            Percentage of impressions where the viewer
            watched 25% of your video.

            This field is a member of `oneof`_ ``_video_quartile_p25_rate``.
        video_quartile_p50_rate (float):
            Percentage of impressions where the viewer
            watched 50% of your video.

            This field is a member of `oneof`_ ``_video_quartile_p50_rate``.
        video_quartile_p75_rate (float):
            Percentage of impressions where the viewer
            watched 75% of your video.

            This field is a member of `oneof`_ ``_video_quartile_p75_rate``.
        video_view_rate (float):
            The number of views your TrueView video ad
            receives divided by its number of impressions,
            including thumbnail impressions for TrueView
            in-display ads.

            This field is a member of `oneof`_ ``_video_view_rate``.
        video_views (int):
            The number of times your video ads were
            viewed.

            This field is a member of `oneof`_ ``_video_views``.
        view_through_conversions (int):
            The total number of view-through conversions.
            These happen when a customer sees an image or
            rich media ad, then later completes a conversion
            on your site without interacting with (for
            example, clicking on) another ad.

            This field is a member of `oneof`_ ``_view_through_conversions``.
        sk_ad_network_conversions (int):
            The number of iOS Store Kit Ad Network
            conversions.
        publisher_purchased_clicks (int):
            Clicks from properties not owned by the
            publisher for which the traffic the publisher
            has paid for or acquired through incentivized
            activity
        publisher_organic_clicks (int):
            Clicks from properties for which the traffic
            the publisher has not paid for or acquired
            through incentivized activity
        publisher_unknown_clicks (int):
            Clicks from traffic which is not identified
            as "Publisher Purchased" or "Publisher Organic".
        all_conversions_from_location_asset_click_to_call (float):
            Number of call button clicks on any location
            surface after a chargeable ad event (click or
            impression). This measure is coming from Asset
            based location.

            This field is a member of `oneof`_ ``_all_conversions_from_location_asset_click_to_call``.
        all_conversions_from_location_asset_directions (float):
            Number of driving directions clicks on any
            location surface after a chargeable ad event
            (click or impression). This measure is coming
            from Asset based location.

            This field is a member of `oneof`_ ``_all_conversions_from_location_asset_directions``.
        all_conversions_from_location_asset_menu (float):
            Number of menu link clicks on any location
            surface after a chargeable ad event (click or
            impression). This measure is coming from Asset
            based location.

            This field is a member of `oneof`_ ``_all_conversions_from_location_asset_menu``.
        all_conversions_from_location_asset_order (float):
            Number of order clicks on any location
            surface after a chargeable ad event (click or
            impression). This measure is coming from Asset
            based location.

            This field is a member of `oneof`_ ``_all_conversions_from_location_asset_order``.
        all_conversions_from_location_asset_other_engagement (float):
            Number of other types of local action clicks
            on any location surface after a chargeable ad
            event (click or impression). This measure is
            coming from Asset based location.

            This field is a member of `oneof`_ ``_all_conversions_from_location_asset_other_engagement``.
        all_conversions_from_location_asset_store_visits (float):
            Estimated number of visits to the store after
            a chargeable ad event (click or impression).
            This measure is coming from Asset based
            location.

            This field is a member of `oneof`_ ``_all_conversions_from_location_asset_store_visits``.
        all_conversions_from_location_asset_website (float):
            Number of website URL clicks on any location
            surface after a chargeable ad event (click or
            impression). This measure is coming from Asset
            based location.

            This field is a member of `oneof`_ ``_all_conversions_from_location_asset_website``.
        eligible_impressions_from_location_asset_store_reach (int):
            Number of impressions in which the store
            location was shown or the location was used for
            targeting. This measure is coming from Asset
            based location.

            This field is a member of `oneof`_ ``_eligible_impressions_from_location_asset_store_reach``.
        view_through_conversions_from_location_asset_click_to_call (float):
            Number of call button clicks on any location
            surface after an impression. This measure is
            coming from Asset based location.

            This field is a member of `oneof`_ ``_view_through_conversions_from_location_asset_click_to_call``.
        view_through_conversions_from_location_asset_directions (float):
            Number of driving directions clicks on any
            location surface after an impression. This
            measure is coming from Asset based location.

            This field is a member of `oneof`_ ``_view_through_conversions_from_location_asset_directions``.
        view_through_conversions_from_location_asset_menu (float):
            Number of menu link clicks on any location
            surface after an impression. This measure is
            coming from Asset based location.

            This field is a member of `oneof`_ ``_view_through_conversions_from_location_asset_menu``.
        view_through_conversions_from_location_asset_order (float):
            Number of order clicks on any location
            surface after an impression. This measure is
            coming from Asset based location.

            This field is a member of `oneof`_ ``_view_through_conversions_from_location_asset_order``.
        view_through_conversions_from_location_asset_other_engagement (float):
            Number of other types of local action clicks
            on any location surface after an impression.
            This measure is coming from Asset based
            location.

            This field is a member of `oneof`_ ``_view_through_conversions_from_location_asset_other_engagement``.
        view_through_conversions_from_location_asset_store_visits (float):
            Estimated number of visits to the store after
            an impression. This measure is coming from Asset
            based location.

            This field is a member of `oneof`_ ``_view_through_conversions_from_location_asset_store_visits``.
        view_through_conversions_from_location_asset_website (float):
            Number of website URL clicks on any location
            surface after an impression. This measure is
            coming from Asset based location.

            This field is a member of `oneof`_ ``_view_through_conversions_from_location_asset_website``.
    """

    absolute_top_impression_percentage = proto.Field(
        proto.DOUBLE, number=183, optional=True,
    )
    active_view_cpm = proto.Field(proto.DOUBLE, number=184, optional=True,)
    active_view_ctr = proto.Field(proto.DOUBLE, number=185, optional=True,)
    active_view_impressions = proto.Field(
        proto.INT64, number=186, optional=True,
    )
    active_view_measurability = proto.Field(
        proto.DOUBLE, number=187, optional=True,
    )
    active_view_measurable_cost_micros = proto.Field(
        proto.INT64, number=188, optional=True,
    )
    active_view_measurable_impressions = proto.Field(
        proto.INT64, number=189, optional=True,
    )
    active_view_viewability = proto.Field(
        proto.DOUBLE, number=190, optional=True,
    )
    all_conversions_from_interactions_rate = proto.Field(
        proto.DOUBLE, number=191, optional=True,
    )
    all_conversions_value = proto.Field(
        proto.DOUBLE, number=192, optional=True,
    )
    all_conversions_value_by_conversion_date = proto.Field(
        proto.DOUBLE, number=240,
    )
    all_conversions = proto.Field(proto.DOUBLE, number=193, optional=True,)
    all_conversions_by_conversion_date = proto.Field(proto.DOUBLE, number=241,)
    all_conversions_value_per_cost = proto.Field(
        proto.DOUBLE, number=194, optional=True,
    )
    all_conversions_from_click_to_call = proto.Field(
        proto.DOUBLE, number=195, optional=True,
    )
    all_conversions_from_directions = proto.Field(
        proto.DOUBLE, number=196, optional=True,
    )
    all_conversions_from_interactions_value_per_interaction = proto.Field(
        proto.DOUBLE, number=197, optional=True,
    )
    all_conversions_from_menu = proto.Field(
        proto.DOUBLE, number=198, optional=True,
    )
    all_conversions_from_order = proto.Field(
        proto.DOUBLE, number=199, optional=True,
    )
    all_conversions_from_other_engagement = proto.Field(
        proto.DOUBLE, number=200, optional=True,
    )
    all_conversions_from_store_visit = proto.Field(
        proto.DOUBLE, number=201, optional=True,
    )
    all_conversions_from_store_website = proto.Field(
        proto.DOUBLE, number=202, optional=True,
    )
    auction_insight_search_absolute_top_impression_percentage = proto.Field(
        proto.DOUBLE, number=258, optional=True,
    )
    auction_insight_search_impression_share = proto.Field(
        proto.DOUBLE, number=259, optional=True,
    )
    auction_insight_search_outranking_share = proto.Field(
        proto.DOUBLE, number=260, optional=True,
    )
    auction_insight_search_overlap_rate = proto.Field(
        proto.DOUBLE, number=261, optional=True,
    )
    auction_insight_search_position_above_rate = proto.Field(
        proto.DOUBLE, number=262, optional=True,
    )
    auction_insight_search_top_impression_percentage = proto.Field(
        proto.DOUBLE, number=263, optional=True,
    )
    average_cost = proto.Field(proto.DOUBLE, number=203, optional=True,)
    average_cpc = proto.Field(proto.DOUBLE, number=204, optional=True,)
    average_cpe = proto.Field(proto.DOUBLE, number=205, optional=True,)
    average_cpm = proto.Field(proto.DOUBLE, number=206, optional=True,)
    average_cpv = proto.Field(proto.DOUBLE, number=207, optional=True,)
    average_page_views = proto.Field(proto.DOUBLE, number=208, optional=True,)
    average_time_on_site = proto.Field(proto.DOUBLE, number=209, optional=True,)
    benchmark_average_max_cpc = proto.Field(
        proto.DOUBLE, number=210, optional=True,
    )
    biddable_app_install_conversions = proto.Field(
        proto.DOUBLE, number=254, optional=True,
    )
    biddable_app_post_install_conversions = proto.Field(
        proto.DOUBLE, number=255, optional=True,
    )
    benchmark_ctr = proto.Field(proto.DOUBLE, number=211, optional=True,)
    bounce_rate = proto.Field(proto.DOUBLE, number=212, optional=True,)
    clicks = proto.Field(proto.INT64, number=131, optional=True,)
    combined_clicks = proto.Field(proto.INT64, number=156, optional=True,)
    combined_clicks_per_query = proto.Field(
        proto.DOUBLE, number=157, optional=True,
    )
    combined_queries = proto.Field(proto.INT64, number=158, optional=True,)
    content_budget_lost_impression_share = proto.Field(
        proto.DOUBLE, number=159, optional=True,
    )
    content_impression_share = proto.Field(
        proto.DOUBLE, number=160, optional=True,
    )
    conversion_last_received_request_date_time = proto.Field(
        proto.STRING, number=161, optional=True,
    )
    conversion_last_conversion_date = proto.Field(
        proto.STRING, number=162, optional=True,
    )
    content_rank_lost_impression_share = proto.Field(
        proto.DOUBLE, number=163, optional=True,
    )
    conversions_from_interactions_rate = proto.Field(
        proto.DOUBLE, number=164, optional=True,
    )
    conversions_value = proto.Field(proto.DOUBLE, number=165, optional=True,)
    conversions_value_by_conversion_date = proto.Field(
        proto.DOUBLE, number=242,
    )
    conversions_value_per_cost = proto.Field(
        proto.DOUBLE, number=166, optional=True,
    )
    conversions_from_interactions_value_per_interaction = proto.Field(
        proto.DOUBLE, number=167, optional=True,
    )
    conversions = proto.Field(proto.DOUBLE, number=168, optional=True,)
    conversions_by_conversion_date = proto.Field(proto.DOUBLE, number=243,)
    cost_micros = proto.Field(proto.INT64, number=169, optional=True,)
    cost_per_all_conversions = proto.Field(
        proto.DOUBLE, number=170, optional=True,
    )
    cost_per_conversion = proto.Field(proto.DOUBLE, number=171, optional=True,)
    cost_per_current_model_attributed_conversion = proto.Field(
        proto.DOUBLE, number=172, optional=True,
    )
    cross_device_conversions = proto.Field(
        proto.DOUBLE, number=173, optional=True,
    )
    ctr = proto.Field(proto.DOUBLE, number=174, optional=True,)
    current_model_attributed_conversions = proto.Field(
        proto.DOUBLE, number=175, optional=True,
    )
    current_model_attributed_conversions_from_interactions_rate = proto.Field(
        proto.DOUBLE, number=176, optional=True,
    )
    current_model_attributed_conversions_from_interactions_value_per_interaction = proto.Field(
        proto.DOUBLE, number=177, optional=True,
    )
    current_model_attributed_conversions_value = proto.Field(
        proto.DOUBLE, number=178, optional=True,
    )
    current_model_attributed_conversions_value_per_cost = proto.Field(
        proto.DOUBLE, number=179, optional=True,
    )
    engagement_rate = proto.Field(proto.DOUBLE, number=180, optional=True,)
    engagements = proto.Field(proto.INT64, number=181, optional=True,)
    hotel_average_lead_value_micros = proto.Field(
        proto.DOUBLE, number=213, optional=True,
    )
    hotel_commission_rate_micros = proto.Field(
        proto.INT64, number=256, optional=True,
    )
    hotel_expected_commission_cost = proto.Field(
        proto.DOUBLE, number=257, optional=True,
    )
    hotel_price_difference_percentage = proto.Field(
        proto.DOUBLE, number=214, optional=True,
    )
    hotel_eligible_impressions = proto.Field(
        proto.INT64, number=215, optional=True,
    )
    historical_creative_quality_score = proto.Field(
        proto.ENUM,
        number=80,
        enum=quality_score_bucket.QualityScoreBucketEnum.QualityScoreBucket,
    )
    historical_landing_page_quality_score = proto.Field(
        proto.ENUM,
        number=81,
        enum=quality_score_bucket.QualityScoreBucketEnum.QualityScoreBucket,
    )
    historical_quality_score = proto.Field(
        proto.INT64, number=216, optional=True,
    )
    historical_search_predicted_ctr = proto.Field(
        proto.ENUM,
        number=83,
        enum=quality_score_bucket.QualityScoreBucketEnum.QualityScoreBucket,
    )
    gmail_forwards = proto.Field(proto.INT64, number=217, optional=True,)
    gmail_saves = proto.Field(proto.INT64, number=218, optional=True,)
    gmail_secondary_clicks = proto.Field(
        proto.INT64, number=219, optional=True,
    )
    impressions_from_store_reach = proto.Field(
        proto.INT64, number=220, optional=True,
    )
    impressions = proto.Field(proto.INT64, number=221, optional=True,)
    interaction_rate = proto.Field(proto.DOUBLE, number=222, optional=True,)
    interactions = proto.Field(proto.INT64, number=223, optional=True,)
    interaction_event_types = proto.RepeatedField(
        proto.ENUM,
        number=100,
        enum=interaction_event_type.InteractionEventTypeEnum.InteractionEventType,
    )
    invalid_click_rate = proto.Field(proto.DOUBLE, number=224, optional=True,)
    invalid_clicks = proto.Field(proto.INT64, number=225, optional=True,)
    message_chats = proto.Field(proto.INT64, number=226, optional=True,)
    message_impressions = proto.Field(proto.INT64, number=227, optional=True,)
    message_chat_rate = proto.Field(proto.DOUBLE, number=228, optional=True,)
    mobile_friendly_clicks_percentage = proto.Field(
        proto.DOUBLE, number=229, optional=True,
    )
    optimization_score_uplift = proto.Field(
        proto.DOUBLE, number=247, optional=True,
    )
    optimization_score_url = proto.Field(
        proto.STRING, number=248, optional=True,
    )
    organic_clicks = proto.Field(proto.INT64, number=230, optional=True,)
    organic_clicks_per_query = proto.Field(
        proto.DOUBLE, number=231, optional=True,
    )
    organic_impressions = proto.Field(proto.INT64, number=232, optional=True,)
    organic_impressions_per_query = proto.Field(
        proto.DOUBLE, number=233, optional=True,
    )
    organic_queries = proto.Field(proto.INT64, number=234, optional=True,)
    percent_new_visitors = proto.Field(proto.DOUBLE, number=235, optional=True,)
    phone_calls = proto.Field(proto.INT64, number=236, optional=True,)
    phone_impressions = proto.Field(proto.INT64, number=237, optional=True,)
    phone_through_rate = proto.Field(proto.DOUBLE, number=238, optional=True,)
    relative_ctr = proto.Field(proto.DOUBLE, number=239, optional=True,)
    search_absolute_top_impression_share = proto.Field(
        proto.DOUBLE, number=136, optional=True,
    )
    search_budget_lost_absolute_top_impression_share = proto.Field(
        proto.DOUBLE, number=137, optional=True,
    )
    search_budget_lost_impression_share = proto.Field(
        proto.DOUBLE, number=138, optional=True,
    )
    search_budget_lost_top_impression_share = proto.Field(
        proto.DOUBLE, number=139, optional=True,
    )
    search_click_share = proto.Field(proto.DOUBLE, number=140, optional=True,)
    search_exact_match_impression_share = proto.Field(
        proto.DOUBLE, number=141, optional=True,
    )
    search_impression_share = proto.Field(
        proto.DOUBLE, number=142, optional=True,
    )
    search_rank_lost_absolute_top_impression_share = proto.Field(
        proto.DOUBLE, number=143, optional=True,
    )
    search_rank_lost_impression_share = proto.Field(
        proto.DOUBLE, number=144, optional=True,
    )
    search_rank_lost_top_impression_share = proto.Field(
        proto.DOUBLE, number=145, optional=True,
    )
    search_top_impression_share = proto.Field(
        proto.DOUBLE, number=146, optional=True,
    )
    speed_score = proto.Field(proto.INT64, number=147, optional=True,)
    top_impression_percentage = proto.Field(
        proto.DOUBLE, number=148, optional=True,
    )
    valid_accelerated_mobile_pages_clicks_percentage = proto.Field(
        proto.DOUBLE, number=149, optional=True,
    )
    value_per_all_conversions = proto.Field(
        proto.DOUBLE, number=150, optional=True,
    )
    value_per_all_conversions_by_conversion_date = proto.Field(
        proto.DOUBLE, number=244, optional=True,
    )
    value_per_conversion = proto.Field(proto.DOUBLE, number=151, optional=True,)
    value_per_conversions_by_conversion_date = proto.Field(
        proto.DOUBLE, number=245, optional=True,
    )
    value_per_current_model_attributed_conversion = proto.Field(
        proto.DOUBLE, number=152, optional=True,
    )
    video_quartile_p100_rate = proto.Field(
        proto.DOUBLE, number=132, optional=True,
    )
    video_quartile_p25_rate = proto.Field(
        proto.DOUBLE, number=133, optional=True,
    )
    video_quartile_p50_rate = proto.Field(
        proto.DOUBLE, number=134, optional=True,
    )
    video_quartile_p75_rate = proto.Field(
        proto.DOUBLE, number=135, optional=True,
    )
    video_view_rate = proto.Field(proto.DOUBLE, number=153, optional=True,)
    video_views = proto.Field(proto.INT64, number=154, optional=True,)
    view_through_conversions = proto.Field(
        proto.INT64, number=155, optional=True,
    )
    sk_ad_network_conversions = proto.Field(proto.INT64, number=246,)
    publisher_purchased_clicks = proto.Field(proto.INT64, number=264,)
    publisher_organic_clicks = proto.Field(proto.INT64, number=265,)
    publisher_unknown_clicks = proto.Field(proto.INT64, number=266,)
    all_conversions_from_location_asset_click_to_call = proto.Field(
        proto.DOUBLE, number=267, optional=True,
    )
    all_conversions_from_location_asset_directions = proto.Field(
        proto.DOUBLE, number=268, optional=True,
    )
    all_conversions_from_location_asset_menu = proto.Field(
        proto.DOUBLE, number=269, optional=True,
    )
    all_conversions_from_location_asset_order = proto.Field(
        proto.DOUBLE, number=270, optional=True,
    )
    all_conversions_from_location_asset_other_engagement = proto.Field(
        proto.DOUBLE, number=271, optional=True,
    )
    all_conversions_from_location_asset_store_visits = proto.Field(
        proto.DOUBLE, number=272, optional=True,
    )
    all_conversions_from_location_asset_website = proto.Field(
        proto.DOUBLE, number=273, optional=True,
    )
    eligible_impressions_from_location_asset_store_reach = proto.Field(
        proto.INT64, number=274, optional=True,
    )
    view_through_conversions_from_location_asset_click_to_call = proto.Field(
        proto.DOUBLE, number=275, optional=True,
    )
    view_through_conversions_from_location_asset_directions = proto.Field(
        proto.DOUBLE, number=276, optional=True,
    )
    view_through_conversions_from_location_asset_menu = proto.Field(
        proto.DOUBLE, number=277, optional=True,
    )
    view_through_conversions_from_location_asset_order = proto.Field(
        proto.DOUBLE, number=278, optional=True,
    )
    view_through_conversions_from_location_asset_other_engagement = proto.Field(
        proto.DOUBLE, number=279, optional=True,
    )
    view_through_conversions_from_location_asset_store_visits = proto.Field(
        proto.DOUBLE, number=280, optional=True,
    )
    view_through_conversions_from_location_asset_website = proto.Field(
        proto.DOUBLE, number=281, optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
