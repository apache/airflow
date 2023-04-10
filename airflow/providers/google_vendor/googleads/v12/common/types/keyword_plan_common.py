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

from airflow.providers.google_vendor.googleads.v12.common.types import dates
from airflow.providers.google_vendor.googleads.v12.enums.types import device as gage_device
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    keyword_plan_aggregate_metric_type,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import keyword_plan_competition_level
from airflow.providers.google_vendor.googleads.v12.enums.types import keyword_plan_concept_group_type
from airflow.providers.google_vendor.googleads.v12.enums.types import month_of_year


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={
        "KeywordPlanHistoricalMetrics",
        "HistoricalMetricsOptions",
        "MonthlySearchVolume",
        "KeywordPlanAggregateMetrics",
        "KeywordPlanAggregateMetricResults",
        "KeywordPlanDeviceSearches",
        "KeywordAnnotations",
        "KeywordConcept",
        "ConceptGroup",
    },
)


class KeywordPlanHistoricalMetrics(proto.Message):
    r"""Historical metrics specific to the targeting options
    selected. Targeting options include geographies, network, etc.
    Refer to https://support.google.com/google-ads/answer/3022575
    for more details.

    Attributes:
        avg_monthly_searches (int):
            Approximate number of monthly searches on
            this query averaged for the past 12 months.

            This field is a member of `oneof`_ ``_avg_monthly_searches``.
        monthly_search_volumes (Sequence[google.ads.googleads.v12.common.types.MonthlySearchVolume]):
            Approximate number of searches on this query
            for the past twelve months.
        competition (google.ads.googleads.v12.enums.types.KeywordPlanCompetitionLevelEnum.KeywordPlanCompetitionLevel):
            The competition level for the query.
        competition_index (int):
            The competition index for the query in the range [0, 100].
            Shows how competitive ad placement is for a keyword. The
            level of competition from 0-100 is determined by the number
            of ad slots filled divided by the total number of ad slots
            available. If not enough data is available, null is
            returned.

            This field is a member of `oneof`_ ``_competition_index``.
        low_top_of_page_bid_micros (int):
            Top of page bid low range (20th percentile)
            in micros for the keyword.

            This field is a member of `oneof`_ ``_low_top_of_page_bid_micros``.
        high_top_of_page_bid_micros (int):
            Top of page bid high range (80th percentile)
            in micros for the keyword.

            This field is a member of `oneof`_ ``_high_top_of_page_bid_micros``.
        average_cpc_micros (int):
            Average Cost Per Click in micros for the
            keyword.

            This field is a member of `oneof`_ ``_average_cpc_micros``.
    """

    avg_monthly_searches = proto.Field(proto.INT64, number=7, optional=True,)
    monthly_search_volumes = proto.RepeatedField(
        proto.MESSAGE, number=6, message="MonthlySearchVolume",
    )
    competition = proto.Field(
        proto.ENUM,
        number=2,
        enum=keyword_plan_competition_level.KeywordPlanCompetitionLevelEnum.KeywordPlanCompetitionLevel,
    )
    competition_index = proto.Field(proto.INT64, number=8, optional=True,)
    low_top_of_page_bid_micros = proto.Field(
        proto.INT64, number=9, optional=True,
    )
    high_top_of_page_bid_micros = proto.Field(
        proto.INT64, number=10, optional=True,
    )
    average_cpc_micros = proto.Field(proto.INT64, number=11, optional=True,)


class HistoricalMetricsOptions(proto.Message):
    r"""Historical metrics options.

    Attributes:
        year_month_range (google.ads.googleads.v12.common.types.YearMonthRange):
            The year month range for historical metrics. If not
            specified the searches will be returned for past 12 months.
            Searches data is available for the past 4 years. If the
            search volume is not available for the entire
            year_month_range provided, the subset of the year month
            range for which search volume is available will be returned.

            This field is a member of `oneof`_ ``_year_month_range``.
        include_average_cpc (bool):
            Indicates whether to include average cost per
            click value. Average CPC is a legacy value that
            will be removed and replaced in the future, and
            as such we are including it as an optioanl value
            so clients only use it when strictly necessary
            and to better track clients that use this value.
    """

    year_month_range = proto.Field(
        proto.MESSAGE, number=1, optional=True, message=dates.YearMonthRange,
    )
    include_average_cpc = proto.Field(proto.BOOL, number=2,)


class MonthlySearchVolume(proto.Message):
    r"""Monthly search volume.

    Attributes:
        year (int):
            The year of the search volume (for example,
            2020).

            This field is a member of `oneof`_ ``_year``.
        month (google.ads.googleads.v12.enums.types.MonthOfYearEnum.MonthOfYear):
            The month of the search volume.
        monthly_searches (int):
            Approximate number of searches for the month.
            A null value indicates the search volume is
            unavailable for that month.

            This field is a member of `oneof`_ ``_monthly_searches``.
    """

    year = proto.Field(proto.INT64, number=4, optional=True,)
    month = proto.Field(
        proto.ENUM, number=2, enum=month_of_year.MonthOfYearEnum.MonthOfYear,
    )
    monthly_searches = proto.Field(proto.INT64, number=5, optional=True,)


class KeywordPlanAggregateMetrics(proto.Message):
    r"""The aggregate metrics specification of the request.

    Attributes:
        aggregate_metric_types (Sequence[google.ads.googleads.v12.enums.types.KeywordPlanAggregateMetricTypeEnum.KeywordPlanAggregateMetricType]):
            The list of aggregate metrics to fetch data.
    """

    aggregate_metric_types = proto.RepeatedField(
        proto.ENUM,
        number=1,
        enum=keyword_plan_aggregate_metric_type.KeywordPlanAggregateMetricTypeEnum.KeywordPlanAggregateMetricType,
    )


class KeywordPlanAggregateMetricResults(proto.Message):
    r"""The aggregated historical metrics for keyword plan keywords.

    Attributes:
        device_searches (Sequence[google.ads.googleads.v12.common.types.KeywordPlanDeviceSearches]):
            The aggregate searches for all the keywords
            segmented by device for the specified time.
            Supports the following device types: MOBILE,
            TABLET, DESKTOP.
            This is only set when
            KeywordPlanAggregateMetricTypeEnum.DEVICE is set
            in the KeywordPlanAggregateMetrics field in the
            request.
    """

    device_searches = proto.RepeatedField(
        proto.MESSAGE, number=1, message="KeywordPlanDeviceSearches",
    )


class KeywordPlanDeviceSearches(proto.Message):
    r"""The total searches for the device type during the specified
    time period.

    Attributes:
        device (google.ads.googleads.v12.enums.types.DeviceEnum.Device):
            The device type.
        search_count (int):
            The total searches for the device.

            This field is a member of `oneof`_ ``_search_count``.
    """

    device = proto.Field(
        proto.ENUM, number=1, enum=gage_device.DeviceEnum.Device,
    )
    search_count = proto.Field(proto.INT64, number=2, optional=True,)


class KeywordAnnotations(proto.Message):
    r"""The Annotations for the Keyword plan keywords.

    Attributes:
        concepts (Sequence[google.ads.googleads.v12.common.types.KeywordConcept]):
            The list of concepts for the keyword.
    """

    concepts = proto.RepeatedField(
        proto.MESSAGE, number=1, message="KeywordConcept",
    )


class KeywordConcept(proto.Message):
    r"""The concept for the keyword.

    Attributes:
        name (str):
            The concept name for the keyword in the concept_group.
        concept_group (google.ads.googleads.v12.common.types.ConceptGroup):
            The concept group of the concept details.
    """

    name = proto.Field(proto.STRING, number=1,)
    concept_group = proto.Field(
        proto.MESSAGE, number=2, message="ConceptGroup",
    )


class ConceptGroup(proto.Message):
    r"""The concept group for the keyword concept.

    Attributes:
        name (str):
            The concept group name.
        type_ (google.ads.googleads.v12.enums.types.KeywordPlanConceptGroupTypeEnum.KeywordPlanConceptGroupType):
            The concept group type.
    """

    name = proto.Field(proto.STRING, number=1,)
    type_ = proto.Field(
        proto.ENUM,
        number=2,
        enum=keyword_plan_concept_group_type.KeywordPlanConceptGroupTypeEnum.KeywordPlanConceptGroupType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
