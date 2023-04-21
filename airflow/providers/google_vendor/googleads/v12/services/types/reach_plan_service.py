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
from airflow.providers.google_vendor.googleads.v12.common.types import dates
from airflow.providers.google_vendor.googleads.v12.enums.types import frequency_cap_time_unit
from airflow.providers.google_vendor.googleads.v12.enums.types import reach_plan_age_range
from airflow.providers.google_vendor.googleads.v12.enums.types import reach_plan_network


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "ListPlannableLocationsRequest",
        "ListPlannableLocationsResponse",
        "PlannableLocation",
        "ListPlannableProductsRequest",
        "ListPlannableProductsResponse",
        "ProductMetadata",
        "PlannableTargeting",
        "GenerateReachForecastRequest",
        "EffectiveFrequencyLimit",
        "FrequencyCap",
        "Targeting",
        "CampaignDuration",
        "PlannedProduct",
        "GenerateReachForecastResponse",
        "ReachCurve",
        "ReachForecast",
        "Forecast",
        "PlannedProductReachForecast",
        "PlannedProductForecast",
        "OnTargetAudienceMetrics",
        "EffectiveFrequencyBreakdown",
        "ForecastMetricOptions",
        "AudienceTargeting",
        "AdvancedProductTargeting",
        "YouTubeSelectSettings",
        "YouTubeSelectLineUp",
    },
)


class ListPlannableLocationsRequest(proto.Message):
    r"""Request message for
    [ReachPlanService.ListPlannableLocations][google.ads.googleads.v12.services.ReachPlanService.ListPlannableLocations].

    """


class ListPlannableLocationsResponse(proto.Message):
    r"""The list of plannable locations.

    Attributes:
        plannable_locations (Sequence[google.ads.googleads.v12.services.types.PlannableLocation]):
            The list of locations available for planning.
            See
            https://developers.google.com/google-ads/api/reference/data/geotargets
            for sample locations.
    """

    plannable_locations = proto.RepeatedField(
        proto.MESSAGE, number=1, message="PlannableLocation",
    )


class PlannableLocation(proto.Message):
    r"""A plannable location: country, metro region, province, etc.

    Attributes:
        id (str):
            The location identifier.

            This field is a member of `oneof`_ ``_id``.
        name (str):
            The unique location name in English.

            This field is a member of `oneof`_ ``_name``.
        parent_country_id (int):
            The parent country (not present if location is a country).
            If present, will always be a GeoTargetConstant ID.
            Additional information such as country name is provided by
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v12.services.ReachPlanService.ListPlannableLocations]
            or [GoogleAdsService.Search/SearchStream][].

            This field is a member of `oneof`_ ``_parent_country_id``.
        country_code (str):
            The ISO-3166-1 alpha-2 country code that is
            associated with the location.

            This field is a member of `oneof`_ ``_country_code``.
        location_type (str):
            The location's type. Location types correspond to
            target_type returned by searching location type in
            [GoogleAdsService.Search/SearchStream][].

            This field is a member of `oneof`_ ``_location_type``.
    """

    id = proto.Field(proto.STRING, number=4, optional=True,)
    name = proto.Field(proto.STRING, number=5, optional=True,)
    parent_country_id = proto.Field(proto.INT64, number=6, optional=True,)
    country_code = proto.Field(proto.STRING, number=7, optional=True,)
    location_type = proto.Field(proto.STRING, number=8, optional=True,)


class ListPlannableProductsRequest(proto.Message):
    r"""Request to list available products in a given location.

    Attributes:
        plannable_location_id (str):
            Required. The ID of the selected location for planning. To
            list the available plannable location IDs use
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v12.services.ReachPlanService.ListPlannableLocations].
    """

    plannable_location_id = proto.Field(proto.STRING, number=2,)


class ListPlannableProductsResponse(proto.Message):
    r"""A response with all available products.

    Attributes:
        product_metadata (Sequence[google.ads.googleads.v12.services.types.ProductMetadata]):
            The list of products available for planning
            and related targeting metadata.
    """

    product_metadata = proto.RepeatedField(
        proto.MESSAGE, number=1, message="ProductMetadata",
    )


class ProductMetadata(proto.Message):
    r"""The metadata associated with an available plannable product.

    Attributes:
        plannable_product_code (str):
            The code associated with the ad product (for example:
            BUMPER, TRUEVIEW_IN_STREAM). To list the available plannable
            product codes use
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v12.services.ReachPlanService.ListPlannableProducts].

            This field is a member of `oneof`_ ``_plannable_product_code``.
        plannable_product_name (str):
            The name associated with the ad product.
        plannable_targeting (google.ads.googleads.v12.services.types.PlannableTargeting):
            The allowed plannable targeting for this
            product.
    """

    plannable_product_code = proto.Field(proto.STRING, number=4, optional=True,)
    plannable_product_name = proto.Field(proto.STRING, number=3,)
    plannable_targeting = proto.Field(
        proto.MESSAGE, number=2, message="PlannableTargeting",
    )


class PlannableTargeting(proto.Message):
    r"""The targeting for which traffic metrics will be reported.

    Attributes:
        age_ranges (Sequence[google.ads.googleads.v12.enums.types.ReachPlanAgeRangeEnum.ReachPlanAgeRange]):
            Allowed plannable age ranges for the product
            for which metrics will be reported. Actual
            targeting is computed by mapping this age range
            onto standard Google common.AgeRangeInfo values.
        genders (Sequence[google.ads.googleads.v12.common.types.GenderInfo]):
            Targetable genders for the ad product.
        devices (Sequence[google.ads.googleads.v12.common.types.DeviceInfo]):
            Targetable devices for the ad product. TABLET device
            targeting is automatically applied to reported metrics when
            MOBILE targeting is selected for CPM_MASTHEAD,
            GOOGLE_PREFERRED_BUMPER, and GOOGLE_PREFERRED_SHORT
            products.
        networks (Sequence[google.ads.googleads.v12.enums.types.ReachPlanNetworkEnum.ReachPlanNetwork]):
            Targetable networks for the ad product.
        youtube_select_lineups (Sequence[google.ads.googleads.v12.services.types.YouTubeSelectLineUp]):
            Targetable YouTube Select Lineups for the ad
            product.
    """

    age_ranges = proto.RepeatedField(
        proto.ENUM,
        number=1,
        enum=reach_plan_age_range.ReachPlanAgeRangeEnum.ReachPlanAgeRange,
    )
    genders = proto.RepeatedField(
        proto.MESSAGE, number=2, message=criteria.GenderInfo,
    )
    devices = proto.RepeatedField(
        proto.MESSAGE, number=3, message=criteria.DeviceInfo,
    )
    networks = proto.RepeatedField(
        proto.ENUM,
        number=4,
        enum=reach_plan_network.ReachPlanNetworkEnum.ReachPlanNetwork,
    )
    youtube_select_lineups = proto.RepeatedField(
        proto.MESSAGE, number=5, message="YouTubeSelectLineUp",
    )


class GenerateReachForecastRequest(proto.Message):
    r"""Request message for
    [ReachPlanService.GenerateReachForecast][google.ads.googleads.v12.services.ReachPlanService.GenerateReachForecast].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        currency_code (str):
            The currency code.
            Three-character ISO 4217 currency code.

            This field is a member of `oneof`_ ``_currency_code``.
        campaign_duration (google.ads.googleads.v12.services.types.CampaignDuration):
            Required. Campaign duration.
        cookie_frequency_cap (int):
            Chosen cookie frequency cap to be applied to each planned
            product. This is equivalent to the frequency cap exposed in
            Google Ads when creating a campaign, it represents the
            maximum number of times an ad can be shown to the same user.
            If not specified, no cap is applied.

            This field is deprecated in v4 and will eventually be
            removed. Use cookie_frequency_cap_setting instead.

            This field is a member of `oneof`_ ``_cookie_frequency_cap``.
        cookie_frequency_cap_setting (google.ads.googleads.v12.services.types.FrequencyCap):
            Chosen cookie frequency cap to be applied to each planned
            product. This is equivalent to the frequency cap exposed in
            Google Ads when creating a campaign, it represents the
            maximum number of times an ad can be shown to the same user
            during a specified time interval. If not specified, a
            default of 0 (no cap) is applied.

            This field replaces the deprecated cookie_frequency_cap
            field.
        min_effective_frequency (int):
            Chosen minimum effective frequency (the number of times a
            person was exposed to the ad) for the reported reach metrics
            [1-10]. This won't affect the targeting, but just the
            reporting. If not specified, a default of 1 is applied.

            This field cannot be combined with the
            effective_frequency_limit field.

            This field is a member of `oneof`_ ``_min_effective_frequency``.
        effective_frequency_limit (google.ads.googleads.v12.services.types.EffectiveFrequencyLimit):
            The highest minimum effective frequency (the number of times
            a person was exposed to the ad) value [1-10] to include in
            Forecast.effective_frequency_breakdowns. If not specified,
            Forecast.effective_frequency_breakdowns will not be
            provided.

            The effective frequency value provided here will also be
            used as the minimum effective frequency for the reported
            reach metrics.

            This field cannot be combined with the
            min_effective_frequency field.

            This field is a member of `oneof`_ ``_effective_frequency_limit``.
        targeting (google.ads.googleads.v12.services.types.Targeting):
            The targeting to be applied to all products
            selected in the product mix.
            This is planned targeting: execution details
            might vary based on the advertising product,
            consult an implementation specialist.
            See specific metrics for details on how
            targeting affects them.
        planned_products (Sequence[google.ads.googleads.v12.services.types.PlannedProduct]):
            Required. The products to be forecast.
            The max number of allowed planned products is
            15.
        forecast_metric_options (google.ads.googleads.v12.services.types.ForecastMetricOptions):
            Controls the forecast metrics returned in the
            response.
        customer_reach_group (str):
            The name of the customer being planned for. This is a
            user-defined value. Required if targeting.audience_targeting
            is set.

            This field is a member of `oneof`_ ``_customer_reach_group``.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    currency_code = proto.Field(proto.STRING, number=9, optional=True,)
    campaign_duration = proto.Field(
        proto.MESSAGE, number=3, message="CampaignDuration",
    )
    cookie_frequency_cap = proto.Field(proto.INT32, number=10, optional=True,)
    cookie_frequency_cap_setting = proto.Field(
        proto.MESSAGE, number=8, message="FrequencyCap",
    )
    min_effective_frequency = proto.Field(
        proto.INT32, number=11, optional=True,
    )
    effective_frequency_limit = proto.Field(
        proto.MESSAGE,
        number=12,
        optional=True,
        message="EffectiveFrequencyLimit",
    )
    targeting = proto.Field(proto.MESSAGE, number=6, message="Targeting",)
    planned_products = proto.RepeatedField(
        proto.MESSAGE, number=7, message="PlannedProduct",
    )
    forecast_metric_options = proto.Field(
        proto.MESSAGE, number=13, message="ForecastMetricOptions",
    )
    customer_reach_group = proto.Field(proto.STRING, number=14, optional=True,)


class EffectiveFrequencyLimit(proto.Message):
    r"""Effective frequency limit.

    Attributes:
        effective_frequency_breakdown_limit (int):
            The highest effective frequency value to include in
            Forecast.effective_frequency_breakdowns. This field supports
            frequencies 1-10, inclusive.
    """

    effective_frequency_breakdown_limit = proto.Field(proto.INT32, number=1,)


class FrequencyCap(proto.Message):
    r"""A rule specifying the maximum number of times an ad can be
    shown to a user over a particular time period.

    Attributes:
        impressions (int):
            Required. The number of impressions,
            inclusive.
        time_unit (google.ads.googleads.v12.enums.types.FrequencyCapTimeUnitEnum.FrequencyCapTimeUnit):
            Required. The type of time unit.
    """

    impressions = proto.Field(proto.INT32, number=3,)
    time_unit = proto.Field(
        proto.ENUM,
        number=2,
        enum=frequency_cap_time_unit.FrequencyCapTimeUnitEnum.FrequencyCapTimeUnit,
    )


class Targeting(proto.Message):
    r"""The targeting for which traffic metrics will be reported.

    Attributes:
        plannable_location_id (str):
            The ID of the selected location. Plannable location IDs can
            be obtained from
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v12.services.ReachPlanService.ListPlannableLocations].

            Requests must set either this field or
            ``plannable_location_ids``.

            This field is deprecated as of V12 and will be removed in a
            future release. Use ``plannable_location_ids`` instead.

            This field is a member of `oneof`_ ``_plannable_location_id``.
        plannable_location_ids (Sequence[str]):
            The list of plannable location IDs to target with this
            forecast.

            If more than one ID is provided, all IDs must have the same
            ``parent_country_id``. Planning for more than
            ``parent_county`` is not supported. Plannable location IDs
            and their ``parent_country_id`` can be obtained from
            [ReachPlanService.ListPlannableLocations][google.ads.googleads.v12.services.ReachPlanService.ListPlannableLocations].

            Requests must set either this field or
            ``plannable_location_id``.
        age_range (google.ads.googleads.v12.enums.types.ReachPlanAgeRangeEnum.ReachPlanAgeRange):
            Targeted age range.
            An unset value is equivalent to targeting all
            ages.
        genders (Sequence[google.ads.googleads.v12.common.types.GenderInfo]):
            Targeted genders.
            An unset value is equivalent to targeting MALE
            and FEMALE.
        devices (Sequence[google.ads.googleads.v12.common.types.DeviceInfo]):
            Targeted devices. If not specified, targets all applicable
            devices. Applicable devices vary by product and region and
            can be obtained from
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v12.services.ReachPlanService.ListPlannableProducts].
        network (google.ads.googleads.v12.enums.types.ReachPlanNetworkEnum.ReachPlanNetwork):
            Targetable network for the ad product. If not specified,
            targets all applicable networks. Applicable networks vary by
            product and region and can be obtained from
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v12.services.ReachPlanService.ListPlannableProducts].
        audience_targeting (google.ads.googleads.v12.services.types.AudienceTargeting):
            Targeted audiences.
            If not specified, does not target any specific
            audience.
    """

    plannable_location_id = proto.Field(proto.STRING, number=6, optional=True,)
    plannable_location_ids = proto.RepeatedField(proto.STRING, number=8,)
    age_range = proto.Field(
        proto.ENUM,
        number=2,
        enum=reach_plan_age_range.ReachPlanAgeRangeEnum.ReachPlanAgeRange,
    )
    genders = proto.RepeatedField(
        proto.MESSAGE, number=3, message=criteria.GenderInfo,
    )
    devices = proto.RepeatedField(
        proto.MESSAGE, number=4, message=criteria.DeviceInfo,
    )
    network = proto.Field(
        proto.ENUM,
        number=5,
        enum=reach_plan_network.ReachPlanNetworkEnum.ReachPlanNetwork,
    )
    audience_targeting = proto.Field(
        proto.MESSAGE, number=7, message="AudienceTargeting",
    )


class CampaignDuration(proto.Message):
    r"""The duration of a planned campaign.

    Attributes:
        duration_in_days (int):
            The duration value in days.

            This field cannot be combined with the date_range field.

            This field is a member of `oneof`_ ``_duration_in_days``.
        date_range (google.ads.googleads.v12.common.types.DateRange):
            Date range of the campaign. Dates are in the yyyy-mm-dd
            format and inclusive. The end date must be < 1 year in the
            future and the date range must be <= 92 days long.

            This field cannot be combined with the duration_in_days
            field.
    """

    duration_in_days = proto.Field(proto.INT32, number=2, optional=True,)
    date_range = proto.Field(proto.MESSAGE, number=3, message=dates.DateRange,)


class PlannedProduct(proto.Message):
    r"""A product being planned for reach.

    Attributes:
        plannable_product_code (str):
            Required. Selected product for planning. The code associated
            with the ad product (for example: Trueview, Bumper). To list
            the available plannable product codes use
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v12.services.ReachPlanService.ListPlannableProducts].

            This field is a member of `oneof`_ ``_plannable_product_code``.
        budget_micros (int):
            Required. Maximum budget allocation in micros for the
            selected product. The value is specified in the selected
            planning currency_code. For example: 1 000 000$ = 1 000 000
            000 000 micros.

            This field is a member of `oneof`_ ``_budget_micros``.
        advanced_product_targeting (google.ads.googleads.v12.services.types.AdvancedProductTargeting):
            Targeting settings for the selected product. To list the
            available targeting for each product use
            [ReachPlanService.ListPlannableProducts][google.ads.googleads.v12.services.ReachPlanService.ListPlannableProducts].
    """

    plannable_product_code = proto.Field(proto.STRING, number=3, optional=True,)
    budget_micros = proto.Field(proto.INT64, number=4, optional=True,)
    advanced_product_targeting = proto.Field(
        proto.MESSAGE, number=5, message="AdvancedProductTargeting",
    )


class GenerateReachForecastResponse(proto.Message):
    r"""Response message containing the generated reach curve.

    Attributes:
        on_target_audience_metrics (google.ads.googleads.v12.services.types.OnTargetAudienceMetrics):
            Reference on target audiences for this curve.
        reach_curve (google.ads.googleads.v12.services.types.ReachCurve):
            The generated reach curve for the planned
            product mix.
    """

    on_target_audience_metrics = proto.Field(
        proto.MESSAGE, number=1, message="OnTargetAudienceMetrics",
    )
    reach_curve = proto.Field(proto.MESSAGE, number=2, message="ReachCurve",)


class ReachCurve(proto.Message):
    r"""The reach curve for the planned products.

    Attributes:
        reach_forecasts (Sequence[google.ads.googleads.v12.services.types.ReachForecast]):
            All points on the reach curve.
    """

    reach_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=1, message="ReachForecast",
    )


class ReachForecast(proto.Message):
    r"""A point on reach curve.

    Attributes:
        cost_micros (int):
            The cost in micros.
        forecast (google.ads.googleads.v12.services.types.Forecast):
            Forecasted traffic metrics for this point.
        planned_product_reach_forecasts (Sequence[google.ads.googleads.v12.services.types.PlannedProductReachForecast]):
            The forecasted allocation and traffic metrics
            for each planned product at this point on the
            reach curve.
    """

    cost_micros = proto.Field(proto.INT64, number=5,)
    forecast = proto.Field(proto.MESSAGE, number=2, message="Forecast",)
    planned_product_reach_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=4, message="PlannedProductReachForecast",
    )


class Forecast(proto.Message):
    r"""Forecasted traffic metrics for the planned products and
    targeting.

    Attributes:
        on_target_reach (int):
            Number of unique people reached at least
            GenerateReachForecastRequest.min_effective_frequency or
            GenerateReachForecastRequest.effective_frequency_limit times
            that exactly matches the Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the on_target_reach value will be rounded to 0.

            This field is a member of `oneof`_ ``_on_target_reach``.
        total_reach (int):
            Total number of unique people reached at least
            GenerateReachForecastRequest.min_effective_frequency or
            GenerateReachForecastRequest.effective_frequency_limit
            times. This includes people that may fall outside the
            specified Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the total_reach value will be rounded to 0.

            This field is a member of `oneof`_ ``_total_reach``.
        on_target_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting.

            This field is a member of `oneof`_ ``_on_target_impressions``.
        total_impressions (int):
            Total number of ad impressions. This includes
            impressions that may fall outside the specified
            Targeting, due to insufficient information on
            signed-in users.

            This field is a member of `oneof`_ ``_total_impressions``.
        viewable_impressions (int):
            Number of times the ad's impressions were
            considered viewable. See
            https://support.google.com/google-ads/answer/7029393
            for more information about what makes an ad
            viewable and how viewability is measured.

            This field is a member of `oneof`_ ``_viewable_impressions``.
        effective_frequency_breakdowns (Sequence[google.ads.googleads.v12.services.types.EffectiveFrequencyBreakdown]):
            A list of effective frequency forecasts. The list is ordered
            starting with 1+ and ending with the value set in
            GenerateReachForecastRequest.effective_frequency_limit. If
            no effective_frequency_limit was set, this list will be
            empty.
        on_target_coview_reach (int):
            Number of unique people reached that exactly
            matches the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_reach``.
        total_coview_reach (int):
            Number of unique people reached including
            co-viewers. This includes people that may fall
            outside the specified Targeting.

            This field is a member of `oneof`_ ``_total_coview_reach``.
        on_target_coview_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_impressions``.
        total_coview_impressions (int):
            Total number of ad impressions including
            co-viewers. This includes impressions that may
            fall outside the specified Targeting, due to
            insufficient information on signed-in users.

            This field is a member of `oneof`_ ``_total_coview_impressions``.
    """

    on_target_reach = proto.Field(proto.INT64, number=5, optional=True,)
    total_reach = proto.Field(proto.INT64, number=6, optional=True,)
    on_target_impressions = proto.Field(proto.INT64, number=7, optional=True,)
    total_impressions = proto.Field(proto.INT64, number=8, optional=True,)
    viewable_impressions = proto.Field(proto.INT64, number=9, optional=True,)
    effective_frequency_breakdowns = proto.RepeatedField(
        proto.MESSAGE, number=10, message="EffectiveFrequencyBreakdown",
    )
    on_target_coview_reach = proto.Field(proto.INT64, number=11, optional=True,)
    total_coview_reach = proto.Field(proto.INT64, number=12, optional=True,)
    on_target_coview_impressions = proto.Field(
        proto.INT64, number=13, optional=True,
    )
    total_coview_impressions = proto.Field(
        proto.INT64, number=14, optional=True,
    )


class PlannedProductReachForecast(proto.Message):
    r"""The forecasted allocation and traffic metrics for a specific
    product at a point on the reach curve.

    Attributes:
        plannable_product_code (str):
            Selected product for planning. The product
            codes returned are within the set of the ones
            returned by ListPlannableProducts when using the
            same location ID.
        cost_micros (int):
            The cost in micros. This may differ from the
            product's input allocation if one or more
            planned products cannot fulfill the budget
            because of limited inventory.
        planned_product_forecast (google.ads.googleads.v12.services.types.PlannedProductForecast):
            Forecasted traffic metrics for this product.
    """

    plannable_product_code = proto.Field(proto.STRING, number=1,)
    cost_micros = proto.Field(proto.INT64, number=2,)
    planned_product_forecast = proto.Field(
        proto.MESSAGE, number=3, message="PlannedProductForecast",
    )


class PlannedProductForecast(proto.Message):
    r"""Forecasted traffic metrics for a planned product.

    Attributes:
        on_target_reach (int):
            Number of unique people reached that exactly matches the
            Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the on_target_reach value will be rounded to 0.
        total_reach (int):
            Number of unique people reached. This includes people that
            may fall outside the specified Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the total_reach value will be rounded to 0.
        on_target_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting.
        total_impressions (int):
            Total number of ad impressions. This includes
            impressions that may fall outside the specified
            Targeting, due to insufficient information on
            signed-in users.
        viewable_impressions (int):
            Number of times the ad's impressions were
            considered viewable. See
            https://support.google.com/google-ads/answer/7029393
            for more information about what makes an ad
            viewable and how viewability is measured.

            This field is a member of `oneof`_ ``_viewable_impressions``.
        on_target_coview_reach (int):
            Number of unique people reached that exactly
            matches the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_reach``.
        total_coview_reach (int):
            Number of unique people reached including
            co-viewers. This includes people that may fall
            outside the specified Targeting.

            This field is a member of `oneof`_ ``_total_coview_reach``.
        on_target_coview_impressions (int):
            Number of ad impressions that exactly matches
            the Targeting including co-viewers.

            This field is a member of `oneof`_ ``_on_target_coview_impressions``.
        total_coview_impressions (int):
            Total number of ad impressions including
            co-viewers. This includes impressions that may
            fall outside the specified Targeting, due to
            insufficient information on signed-in users.

            This field is a member of `oneof`_ ``_total_coview_impressions``.
    """

    on_target_reach = proto.Field(proto.INT64, number=1,)
    total_reach = proto.Field(proto.INT64, number=2,)
    on_target_impressions = proto.Field(proto.INT64, number=3,)
    total_impressions = proto.Field(proto.INT64, number=4,)
    viewable_impressions = proto.Field(proto.INT64, number=5, optional=True,)
    on_target_coview_reach = proto.Field(proto.INT64, number=6, optional=True,)
    total_coview_reach = proto.Field(proto.INT64, number=7, optional=True,)
    on_target_coview_impressions = proto.Field(
        proto.INT64, number=8, optional=True,
    )
    total_coview_impressions = proto.Field(
        proto.INT64, number=9, optional=True,
    )


class OnTargetAudienceMetrics(proto.Message):
    r"""Audience metrics for the planned products.
    These metrics consider the following targeting dimensions:
    - Location
    - PlannableAgeRange
    - Gender

    Attributes:
        youtube_audience_size (int):
            Reference audience size matching the
            considered targeting for YouTube.

            This field is a member of `oneof`_ ``_youtube_audience_size``.
        census_audience_size (int):
            Reference audience size matching the
            considered targeting for Census.

            This field is a member of `oneof`_ ``_census_audience_size``.
    """

    youtube_audience_size = proto.Field(proto.INT64, number=3, optional=True,)
    census_audience_size = proto.Field(proto.INT64, number=4, optional=True,)


class EffectiveFrequencyBreakdown(proto.Message):
    r"""A breakdown of the number of unique people reached at a given
    effective frequency.

    Attributes:
        effective_frequency (int):
            The effective frequency [1-10].
        on_target_reach (int):
            The number of unique people reached at least
            effective_frequency times that exactly matches the
            Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the on_target_reach value will be rounded to 0.
        total_reach (int):
            Total number of unique people reached at least
            effective_frequency times. This includes people that may
            fall outside the specified Targeting.

            Note that a minimum number of unique people must be reached
            in order for data to be reported. If the minimum number is
            not met, the total_reach value will be rounded to 0.
        effective_coview_reach (int):
            The number of users (including co-viewing users) reached for
            the associated effective_frequency value.

            This field is a member of `oneof`_ ``_effective_coview_reach``.
        on_target_effective_coview_reach (int):
            The number of users (including co-viewing users) reached for
            the associated effective_frequency value within the
            specified plan demographic.

            This field is a member of `oneof`_ ``_on_target_effective_coview_reach``.
    """

    effective_frequency = proto.Field(proto.INT32, number=1,)
    on_target_reach = proto.Field(proto.INT64, number=2,)
    total_reach = proto.Field(proto.INT64, number=3,)
    effective_coview_reach = proto.Field(proto.INT64, number=4, optional=True,)
    on_target_effective_coview_reach = proto.Field(
        proto.INT64, number=5, optional=True,
    )


class ForecastMetricOptions(proto.Message):
    r"""Controls forecast metrics to return.

    Attributes:
        include_coview (bool):
            Indicates whether to include co-view metrics
            in the response forecast.
    """

    include_coview = proto.Field(proto.BOOL, number=1,)


class AudienceTargeting(proto.Message):
    r"""Audience targeting for reach forecast.

    Attributes:
        user_interest (Sequence[google.ads.googleads.v12.common.types.UserInterestInfo]):
            List of audiences based on user interests to
            be targeted.
    """

    user_interest = proto.RepeatedField(
        proto.MESSAGE, number=1, message=criteria.UserInterestInfo,
    )


class AdvancedProductTargeting(proto.Message):
    r"""Advanced targeting settings for products.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        youtube_select_settings (google.ads.googleads.v12.services.types.YouTubeSelectSettings):
            Settings for YouTube Select targeting.

            This field is a member of `oneof`_ ``advanced_targeting``.
    """

    youtube_select_settings = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="advanced_targeting",
        message="YouTubeSelectSettings",
    )


class YouTubeSelectSettings(proto.Message):
    r"""Request settings for YouTube Select Lineups

    Attributes:
        lineup_id (int):
            Lineup for YouTube Select Targeting.
    """

    lineup_id = proto.Field(proto.INT64, number=1,)


class YouTubeSelectLineUp(proto.Message):
    r"""A Plannable YouTube Select Lineup for product targeting.

    Attributes:
        lineup_id (int):
            The ID of the YouTube Select Lineup.
        lineup_name (str):
            The unique name of the YouTube Select Lineup.
    """

    lineup_id = proto.Field(proto.INT64, number=1,)
    lineup_name = proto.Field(proto.STRING, number=2,)


__all__ = tuple(sorted(__protobuf__.manifest))
