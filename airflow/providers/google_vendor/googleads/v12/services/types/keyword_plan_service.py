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

from airflow.providers.google_vendor.googleads.v12.common.types import keyword_plan_common
from airflow.providers.google_vendor.googleads.v12.resources.types import (
    keyword_plan as gagr_keyword_plan,
)
from google.protobuf import field_mask_pb2  # type: ignore
from google.rpc import status_pb2  # type: ignore


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "MutateKeywordPlansRequest",
        "KeywordPlanOperation",
        "MutateKeywordPlansResponse",
        "MutateKeywordPlansResult",
        "GenerateForecastCurveRequest",
        "GenerateForecastCurveResponse",
        "GenerateForecastTimeSeriesRequest",
        "GenerateForecastTimeSeriesResponse",
        "GenerateForecastMetricsRequest",
        "GenerateForecastMetricsResponse",
        "KeywordPlanCampaignForecast",
        "KeywordPlanAdGroupForecast",
        "KeywordPlanKeywordForecast",
        "KeywordPlanCampaignForecastCurve",
        "KeywordPlanMaxCpcBidForecastCurve",
        "KeywordPlanMaxCpcBidForecast",
        "KeywordPlanWeeklyTimeSeriesForecast",
        "KeywordPlanWeeklyForecast",
        "ForecastMetrics",
        "GenerateHistoricalMetricsRequest",
        "GenerateHistoricalMetricsResponse",
        "KeywordPlanKeywordHistoricalMetrics",
    },
)


class MutateKeywordPlansRequest(proto.Message):
    r"""Request message for
    [KeywordPlanService.MutateKeywordPlans][google.ads.googleads.v12.services.KeywordPlanService.MutateKeywordPlans].

    Attributes:
        customer_id (str):
            Required. The ID of the customer whose
            keyword plans are being modified.
        operations (Sequence[google.ads.googleads.v12.services.types.KeywordPlanOperation]):
            Required. The list of operations to perform
            on individual keyword plans.
        partial_failure (bool):
            If true, successful operations will be
            carried out and invalid operations will return
            errors. If false, all operations will be carried
            out in one transaction if and only if they are
            all valid. Default is false.
        validate_only (bool):
            If true, the request is validated but not
            executed. Only errors are returned, not results.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=2, message="KeywordPlanOperation",
    )
    partial_failure = proto.Field(proto.BOOL, number=3,)
    validate_only = proto.Field(proto.BOOL, number=4,)


class KeywordPlanOperation(proto.Message):
    r"""A single operation (create, update, remove) on a keyword
    plan.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        update_mask (google.protobuf.field_mask_pb2.FieldMask):
            The FieldMask that determines which resource
            fields are modified in an update.
        create (google.ads.googleads.v12.resources.types.KeywordPlan):
            Create operation: No resource name is
            expected for the new keyword plan.

            This field is a member of `oneof`_ ``operation``.
        update (google.ads.googleads.v12.resources.types.KeywordPlan):
            Update operation: The keyword plan is
            expected to have a valid resource name.

            This field is a member of `oneof`_ ``operation``.
        remove (str):
            Remove operation: A resource name for the removed keyword
            plan is expected in this format:

            ``customers/{customer_id}/keywordPlans/{keyword_plan_id}``

            This field is a member of `oneof`_ ``operation``.
    """

    update_mask = proto.Field(
        proto.MESSAGE, number=4, message=field_mask_pb2.FieldMask,
    )
    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=gagr_keyword_plan.KeywordPlan,
    )
    update = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=gagr_keyword_plan.KeywordPlan,
    )
    remove = proto.Field(proto.STRING, number=3, oneof="operation",)


class MutateKeywordPlansResponse(proto.Message):
    r"""Response message for a keyword plan mutate.

    Attributes:
        partial_failure_error (google.rpc.status_pb2.Status):
            Errors that pertain to operation failures in the partial
            failure mode. Returned only when partial_failure = true and
            all errors occur inside the operations. If any errors occur
            outside the operations (for example, auth errors), we return
            an RPC level error.
        results (Sequence[google.ads.googleads.v12.services.types.MutateKeywordPlansResult]):
            All results for the mutate.
    """

    partial_failure_error = proto.Field(
        proto.MESSAGE, number=3, message=status_pb2.Status,
    )
    results = proto.RepeatedField(
        proto.MESSAGE, number=2, message="MutateKeywordPlansResult",
    )


class MutateKeywordPlansResult(proto.Message):
    r"""The result for the keyword plan mutate.

    Attributes:
        resource_name (str):
            Returned for successful operations.
    """

    resource_name = proto.Field(proto.STRING, number=1,)


class GenerateForecastCurveRequest(proto.Message):
    r"""Request message for
    [KeywordPlanService.GenerateForecastCurve][google.ads.googleads.v12.services.KeywordPlanService.GenerateForecastCurve].

    Attributes:
        keyword_plan (str):
            Required. The resource name of the keyword
            plan to be forecasted.
    """

    keyword_plan = proto.Field(proto.STRING, number=1,)


class GenerateForecastCurveResponse(proto.Message):
    r"""Response message for
    [KeywordPlanService.GenerateForecastCurve][google.ads.googleads.v12.services.KeywordPlanService.GenerateForecastCurve].

    Attributes:
        campaign_forecast_curves (Sequence[google.ads.googleads.v12.services.types.KeywordPlanCampaignForecastCurve]):
            List of forecast curves for the keyword plan
            campaign. One maximum.
    """

    campaign_forecast_curves = proto.RepeatedField(
        proto.MESSAGE, number=1, message="KeywordPlanCampaignForecastCurve",
    )


class GenerateForecastTimeSeriesRequest(proto.Message):
    r"""Request message for
    [KeywordPlanService.GenerateForecastTimeSeries][google.ads.googleads.v12.services.KeywordPlanService.GenerateForecastTimeSeries].

    Attributes:
        keyword_plan (str):
            Required. The resource name of the keyword
            plan to be forecasted.
    """

    keyword_plan = proto.Field(proto.STRING, number=1,)


class GenerateForecastTimeSeriesResponse(proto.Message):
    r"""Response message for
    [KeywordPlanService.GenerateForecastTimeSeries][google.ads.googleads.v12.services.KeywordPlanService.GenerateForecastTimeSeries].

    Attributes:
        weekly_time_series_forecasts (Sequence[google.ads.googleads.v12.services.types.KeywordPlanWeeklyTimeSeriesForecast]):
            List of weekly time series forecasts for the
            keyword plan campaign. One maximum.
    """

    weekly_time_series_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=1, message="KeywordPlanWeeklyTimeSeriesForecast",
    )


class GenerateForecastMetricsRequest(proto.Message):
    r"""Request message for
    [KeywordPlanService.GenerateForecastMetrics][google.ads.googleads.v12.services.KeywordPlanService.GenerateForecastMetrics].

    Attributes:
        keyword_plan (str):
            Required. The resource name of the keyword
            plan to be forecasted.
    """

    keyword_plan = proto.Field(proto.STRING, number=1,)


class GenerateForecastMetricsResponse(proto.Message):
    r"""Response message for
    [KeywordPlanService.GenerateForecastMetrics][google.ads.googleads.v12.services.KeywordPlanService.GenerateForecastMetrics].

    Attributes:
        campaign_forecasts (Sequence[google.ads.googleads.v12.services.types.KeywordPlanCampaignForecast]):
            List of campaign forecasts.
            One maximum.
        ad_group_forecasts (Sequence[google.ads.googleads.v12.services.types.KeywordPlanAdGroupForecast]):
            List of ad group forecasts.
        keyword_forecasts (Sequence[google.ads.googleads.v12.services.types.KeywordPlanKeywordForecast]):
            List of keyword forecasts.
    """

    campaign_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=1, message="KeywordPlanCampaignForecast",
    )
    ad_group_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=2, message="KeywordPlanAdGroupForecast",
    )
    keyword_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=3, message="KeywordPlanKeywordForecast",
    )


class KeywordPlanCampaignForecast(proto.Message):
    r"""A campaign forecast.

    Attributes:
        keyword_plan_campaign (str):
            The resource name of the Keyword Plan campaign related to
            the forecast.

            ``customers/{customer_id}/keywordPlanCampaigns/{keyword_plan_campaign_id}``

            This field is a member of `oneof`_ ``_keyword_plan_campaign``.
        campaign_forecast (google.ads.googleads.v12.services.types.ForecastMetrics):
            The forecast for the Keyword Plan campaign.
    """

    keyword_plan_campaign = proto.Field(proto.STRING, number=3, optional=True,)
    campaign_forecast = proto.Field(
        proto.MESSAGE, number=2, message="ForecastMetrics",
    )


class KeywordPlanAdGroupForecast(proto.Message):
    r"""An ad group forecast.

    Attributes:
        keyword_plan_ad_group (str):
            The resource name of the Keyword Plan ad group related to
            the forecast.

            ``customers/{customer_id}/keywordPlanAdGroups/{keyword_plan_ad_group_id}``

            This field is a member of `oneof`_ ``_keyword_plan_ad_group``.
        ad_group_forecast (google.ads.googleads.v12.services.types.ForecastMetrics):
            The forecast for the Keyword Plan ad group.
    """

    keyword_plan_ad_group = proto.Field(proto.STRING, number=3, optional=True,)
    ad_group_forecast = proto.Field(
        proto.MESSAGE, number=2, message="ForecastMetrics",
    )


class KeywordPlanKeywordForecast(proto.Message):
    r"""A keyword forecast.

    Attributes:
        keyword_plan_ad_group_keyword (str):
            The resource name of the Keyword Plan keyword related to the
            forecast.

            ``customers/{customer_id}/keywordPlanAdGroupKeywords/{keyword_plan_ad_group_keyword_id}``

            This field is a member of `oneof`_ ``_keyword_plan_ad_group_keyword``.
        keyword_forecast (google.ads.googleads.v12.services.types.ForecastMetrics):
            The forecast for the Keyword Plan keyword.
    """

    keyword_plan_ad_group_keyword = proto.Field(
        proto.STRING, number=3, optional=True,
    )
    keyword_forecast = proto.Field(
        proto.MESSAGE, number=2, message="ForecastMetrics",
    )


class KeywordPlanCampaignForecastCurve(proto.Message):
    r"""The forecast curve for the campaign.

    Attributes:
        keyword_plan_campaign (str):
            The resource name of the Keyword Plan campaign related to
            the forecast.

            ``customers/{customer_id}/keywordPlanCampaigns/{keyword_plan_campaign_id}``

            This field is a member of `oneof`_ ``_keyword_plan_campaign``.
        max_cpc_bid_forecast_curve (google.ads.googleads.v12.services.types.KeywordPlanMaxCpcBidForecastCurve):
            The max cpc bid forecast curve for the
            campaign.
    """

    keyword_plan_campaign = proto.Field(proto.STRING, number=3, optional=True,)
    max_cpc_bid_forecast_curve = proto.Field(
        proto.MESSAGE, number=2, message="KeywordPlanMaxCpcBidForecastCurve",
    )


class KeywordPlanMaxCpcBidForecastCurve(proto.Message):
    r"""The max cpc bid forecast curve.

    Attributes:
        max_cpc_bid_forecasts (Sequence[google.ads.googleads.v12.services.types.KeywordPlanMaxCpcBidForecast]):
            The forecasts for the Keyword Plan campaign
            at different max CPC bids.
    """

    max_cpc_bid_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=1, message="KeywordPlanMaxCpcBidForecast",
    )


class KeywordPlanMaxCpcBidForecast(proto.Message):
    r"""The forecast of the campaign at a specific bid.

    Attributes:
        max_cpc_bid_micros (int):
            The max cpc bid in micros.

            This field is a member of `oneof`_ ``_max_cpc_bid_micros``.
        max_cpc_bid_forecast (google.ads.googleads.v12.services.types.ForecastMetrics):
            The forecast for the Keyword Plan campaign at
            the specific bid.
    """

    max_cpc_bid_micros = proto.Field(proto.INT64, number=3, optional=True,)
    max_cpc_bid_forecast = proto.Field(
        proto.MESSAGE, number=2, message="ForecastMetrics",
    )


class KeywordPlanWeeklyTimeSeriesForecast(proto.Message):
    r"""The weekly time series forecast for the keyword plan
    campaign.

    Attributes:
        keyword_plan_campaign (str):
            The resource name of the Keyword Plan campaign related to
            the forecast.

            ``customers/{customer_id}/keywordPlanCampaigns/{keyword_plan_campaign_id}``

            This field is a member of `oneof`_ ``_keyword_plan_campaign``.
        weekly_forecasts (Sequence[google.ads.googleads.v12.services.types.KeywordPlanWeeklyForecast]):
            The forecasts for the Keyword Plan campaign
            at different max CPC bids.
    """

    keyword_plan_campaign = proto.Field(proto.STRING, number=1, optional=True,)
    weekly_forecasts = proto.RepeatedField(
        proto.MESSAGE, number=2, message="KeywordPlanWeeklyForecast",
    )


class KeywordPlanWeeklyForecast(proto.Message):
    r"""The forecast of the campaign for the week starting start_date.

    Attributes:
        start_date (str):
            The start date, in yyyy-mm-dd format. This
            date is inclusive.

            This field is a member of `oneof`_ ``_start_date``.
        forecast (google.ads.googleads.v12.services.types.ForecastMetrics):
            The forecast for the Keyword Plan campaign
            for the week.
    """

    start_date = proto.Field(proto.STRING, number=1, optional=True,)
    forecast = proto.Field(proto.MESSAGE, number=2, message="ForecastMetrics",)


class ForecastMetrics(proto.Message):
    r"""Forecast metrics.

    Attributes:
        impressions (float):
            Impressions

            This field is a member of `oneof`_ ``_impressions``.
        ctr (float):
            Ctr

            This field is a member of `oneof`_ ``_ctr``.
        average_cpc (int):
            AVG cpc

            This field is a member of `oneof`_ ``_average_cpc``.
        clicks (float):
            Clicks

            This field is a member of `oneof`_ ``_clicks``.
        cost_micros (int):
            Cost

            This field is a member of `oneof`_ ``_cost_micros``.
    """

    impressions = proto.Field(proto.DOUBLE, number=7, optional=True,)
    ctr = proto.Field(proto.DOUBLE, number=8, optional=True,)
    average_cpc = proto.Field(proto.INT64, number=9, optional=True,)
    clicks = proto.Field(proto.DOUBLE, number=10, optional=True,)
    cost_micros = proto.Field(proto.INT64, number=11, optional=True,)


class GenerateHistoricalMetricsRequest(proto.Message):
    r"""Request message for
    [KeywordPlanService.GenerateHistoricalMetrics][google.ads.googleads.v12.services.KeywordPlanService.GenerateHistoricalMetrics].

    Attributes:
        keyword_plan (str):
            Required. The resource name of the keyword
            plan of which historical metrics are requested.
        aggregate_metrics (google.ads.googleads.v12.common.types.KeywordPlanAggregateMetrics):
            The aggregate fields to include in response.
        historical_metrics_options (google.ads.googleads.v12.common.types.HistoricalMetricsOptions):
            The options for historical metrics data.
    """

    keyword_plan = proto.Field(proto.STRING, number=1,)
    aggregate_metrics = proto.Field(
        proto.MESSAGE,
        number=2,
        message=keyword_plan_common.KeywordPlanAggregateMetrics,
    )
    historical_metrics_options = proto.Field(
        proto.MESSAGE,
        number=3,
        message=keyword_plan_common.HistoricalMetricsOptions,
    )


class GenerateHistoricalMetricsResponse(proto.Message):
    r"""Response message for
    [KeywordPlanService.GenerateHistoricalMetrics][google.ads.googleads.v12.services.KeywordPlanService.GenerateHistoricalMetrics].

    Attributes:
        metrics (Sequence[google.ads.googleads.v12.services.types.KeywordPlanKeywordHistoricalMetrics]):
            List of keyword historical metrics.
        aggregate_metric_results (google.ads.googleads.v12.common.types.KeywordPlanAggregateMetricResults):
            The aggregate metrics for all the keywords in
            the keyword planner plan.
    """

    metrics = proto.RepeatedField(
        proto.MESSAGE, number=1, message="KeywordPlanKeywordHistoricalMetrics",
    )
    aggregate_metric_results = proto.Field(
        proto.MESSAGE,
        number=2,
        message=keyword_plan_common.KeywordPlanAggregateMetricResults,
    )


class KeywordPlanKeywordHistoricalMetrics(proto.Message):
    r"""A keyword historical metrics.

    Attributes:
        search_query (str):
            The text of the query associated with one or more
            ad_group_keywords in the plan.

            Note that we de-dupe your keywords list, eliminating close
            variants before returning the plan's keywords as text. For
            example, if your plan originally contained the keywords
            'car' and 'cars', the returned search query will only
            contain 'cars'. Starting V5, the list of de-duped queries
            will be included in close_variants field.

            This field is a member of `oneof`_ ``_search_query``.
        close_variants (Sequence[str]):
            The list of close variant queries for search_query whose
            search results are combined into the search_query.
        keyword_metrics (google.ads.googleads.v12.common.types.KeywordPlanHistoricalMetrics):
            The historical metrics for the query associated with one or
            more ad_group_keywords in the plan.
    """

    search_query = proto.Field(proto.STRING, number=4, optional=True,)
    close_variants = proto.RepeatedField(proto.STRING, number=3,)
    keyword_metrics = proto.Field(
        proto.MESSAGE,
        number=2,
        message=keyword_plan_common.KeywordPlanHistoricalMetrics,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
