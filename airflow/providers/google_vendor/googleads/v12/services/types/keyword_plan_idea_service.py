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
from airflow.providers.google_vendor.googleads.v12.enums.types import keyword_match_type
from airflow.providers.google_vendor.googleads.v12.enums.types import keyword_plan_keyword_annotation
from airflow.providers.google_vendor.googleads.v12.enums.types import (
    keyword_plan_network as gage_keyword_plan_network,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "GenerateKeywordIdeasRequest",
        "KeywordAndUrlSeed",
        "KeywordSeed",
        "SiteSeed",
        "UrlSeed",
        "GenerateKeywordIdeaResponse",
        "GenerateKeywordIdeaResult",
        "GenerateKeywordHistoricalMetricsRequest",
        "GenerateKeywordHistoricalMetricsResponse",
        "GenerateKeywordHistoricalMetricsResult",
        "GenerateAdGroupThemesRequest",
        "GenerateAdGroupThemesResponse",
        "AdGroupKeywordSuggestion",
        "UnusableAdGroup",
    },
)


class GenerateKeywordIdeasRequest(proto.Message):
    r"""Request message for
    [KeywordPlanIdeaService.GenerateKeywordIdeas][google.ads.googleads.v12.services.KeywordPlanIdeaService.GenerateKeywordIdeas].

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            The ID of the customer with the
            recommendation.
        language (str):
            The resource name of the language to target.
            Each keyword belongs to some set of languages; a
            keyword is included if language is one of its
            languages.
            If not set, all keywords will be included.

            This field is a member of `oneof`_ ``_language``.
        geo_target_constants (Sequence[str]):
            The resource names of the location to target.
            Maximum is 10. An empty list MAY be used to
            specify all targeting geos.
        include_adult_keywords (bool):
            If true, adult keywords will be included in
            response. The default value is false.
        page_token (str):
            Token of the page to retrieve. If not specified, the first
            page of results will be returned. To request next page of
            results use the value obtained from ``next_page_token`` in
            the previous response. The request fields must match across
            pages.
        page_size (int):
            Number of results to retrieve in a single page. A maximum of
            10,000 results may be returned, if the page_size exceeds
            this, it is ignored. If unspecified, at most 10,000 results
            will be returned. The server may decide to further limit the
            number of returned resources. If the response contains fewer
            than 10,000 results it may not be assumed as last page of
            results.
        keyword_plan_network (google.ads.googleads.v12.enums.types.KeywordPlanNetworkEnum.KeywordPlanNetwork):
            Targeting network.
            If not set, Google Search And Partners Network
            will be used.
        keyword_annotation (Sequence[google.ads.googleads.v12.enums.types.KeywordPlanKeywordAnnotationEnum.KeywordPlanKeywordAnnotation]):
            The keyword annotations to include in
            response.
        aggregate_metrics (google.ads.googleads.v12.common.types.KeywordPlanAggregateMetrics):
            The aggregate fields to include in response.
        historical_metrics_options (google.ads.googleads.v12.common.types.HistoricalMetricsOptions):
            The options for historical metrics data.
        keyword_and_url_seed (google.ads.googleads.v12.services.types.KeywordAndUrlSeed):
            A Keyword and a specific Url to generate
            ideas from for example, cars,
            www.example.com/cars.

            This field is a member of `oneof`_ ``seed``.
        keyword_seed (google.ads.googleads.v12.services.types.KeywordSeed):
            A Keyword or phrase to generate ideas from,
            for example, cars.

            This field is a member of `oneof`_ ``seed``.
        url_seed (google.ads.googleads.v12.services.types.UrlSeed):
            A specific url to generate ideas from, for
            example, www.example.com/cars.

            This field is a member of `oneof`_ ``seed``.
        site_seed (google.ads.googleads.v12.services.types.SiteSeed):
            The site to generate ideas from, for example,
            www.example.com.

            This field is a member of `oneof`_ ``seed``.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    language = proto.Field(proto.STRING, number=14, optional=True,)
    geo_target_constants = proto.RepeatedField(proto.STRING, number=15,)
    include_adult_keywords = proto.Field(proto.BOOL, number=10,)
    page_token = proto.Field(proto.STRING, number=12,)
    page_size = proto.Field(proto.INT32, number=13,)
    keyword_plan_network = proto.Field(
        proto.ENUM,
        number=9,
        enum=gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork,
    )
    keyword_annotation = proto.RepeatedField(
        proto.ENUM,
        number=17,
        enum=keyword_plan_keyword_annotation.KeywordPlanKeywordAnnotationEnum.KeywordPlanKeywordAnnotation,
    )
    aggregate_metrics = proto.Field(
        proto.MESSAGE,
        number=16,
        message=keyword_plan_common.KeywordPlanAggregateMetrics,
    )
    historical_metrics_options = proto.Field(
        proto.MESSAGE,
        number=18,
        message=keyword_plan_common.HistoricalMetricsOptions,
    )
    keyword_and_url_seed = proto.Field(
        proto.MESSAGE, number=2, oneof="seed", message="KeywordAndUrlSeed",
    )
    keyword_seed = proto.Field(
        proto.MESSAGE, number=3, oneof="seed", message="KeywordSeed",
    )
    url_seed = proto.Field(
        proto.MESSAGE, number=5, oneof="seed", message="UrlSeed",
    )
    site_seed = proto.Field(
        proto.MESSAGE, number=11, oneof="seed", message="SiteSeed",
    )


class KeywordAndUrlSeed(proto.Message):
    r"""Keyword And Url Seed

    Attributes:
        url (str):
            The URL to crawl in order to generate keyword
            ideas.

            This field is a member of `oneof`_ ``_url``.
        keywords (Sequence[str]):
            Requires at least one keyword.
    """

    url = proto.Field(proto.STRING, number=3, optional=True,)
    keywords = proto.RepeatedField(proto.STRING, number=4,)


class KeywordSeed(proto.Message):
    r"""Keyword Seed

    Attributes:
        keywords (Sequence[str]):
            Requires at least one keyword.
    """

    keywords = proto.RepeatedField(proto.STRING, number=2,)


class SiteSeed(proto.Message):
    r"""Site Seed

    Attributes:
        site (str):
            The domain name of the site. If the customer
            requesting the ideas doesn't own the site
            provided only public information is returned.

            This field is a member of `oneof`_ ``_site``.
    """

    site = proto.Field(proto.STRING, number=2, optional=True,)


class UrlSeed(proto.Message):
    r"""Url Seed

    Attributes:
        url (str):
            The URL to crawl in order to generate keyword
            ideas.

            This field is a member of `oneof`_ ``_url``.
    """

    url = proto.Field(proto.STRING, number=2, optional=True,)


class GenerateKeywordIdeaResponse(proto.Message):
    r"""Response message for
    [KeywordPlanIdeaService.GenerateKeywordIdeas][google.ads.googleads.v12.services.KeywordPlanIdeaService.GenerateKeywordIdeas].

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.GenerateKeywordIdeaResult]):
            Results of generating keyword ideas.
        aggregate_metric_results (google.ads.googleads.v12.common.types.KeywordPlanAggregateMetricResults):
            The aggregate metrics for all keyword ideas.
        next_page_token (str):
            Pagination token used to retrieve the next page of results.
            Pass the content of this string as the ``page_token``
            attribute of the next request. ``next_page_token`` is not
            returned for the last page.
        total_size (int):
            Total number of results available.
    """

    @property
    def raw_page(self):
        return self

    results = proto.RepeatedField(
        proto.MESSAGE, number=1, message="GenerateKeywordIdeaResult",
    )
    aggregate_metric_results = proto.Field(
        proto.MESSAGE,
        number=4,
        message=keyword_plan_common.KeywordPlanAggregateMetricResults,
    )
    next_page_token = proto.Field(proto.STRING, number=2,)
    total_size = proto.Field(proto.INT64, number=3,)


class GenerateKeywordIdeaResult(proto.Message):
    r"""The result of generating keyword ideas.

    Attributes:
        text (str):
            Text of the keyword idea.
            As in Keyword Plan historical metrics, this text
            may not be an actual keyword, but the canonical
            form of multiple keywords. See
            KeywordPlanKeywordHistoricalMetrics message in
            KeywordPlanService.

            This field is a member of `oneof`_ ``_text``.
        keyword_idea_metrics (google.ads.googleads.v12.common.types.KeywordPlanHistoricalMetrics):
            The historical metrics for the keyword.
        keyword_annotations (google.ads.googleads.v12.common.types.KeywordAnnotations):
            The annotations for the keyword.
            The annotation data is only provided if
            requested.
        close_variants (Sequence[str]):
            The list of close variants from the requested
            keywords that are combined into this
            GenerateKeywordIdeaResult. See
            https://support.google.com/google-ads/answer/9342105
            for the definition of "close variants".
    """

    text = proto.Field(proto.STRING, number=5, optional=True,)
    keyword_idea_metrics = proto.Field(
        proto.MESSAGE,
        number=3,
        message=keyword_plan_common.KeywordPlanHistoricalMetrics,
    )
    keyword_annotations = proto.Field(
        proto.MESSAGE, number=6, message=keyword_plan_common.KeywordAnnotations,
    )
    close_variants = proto.RepeatedField(proto.STRING, number=7,)


class GenerateKeywordHistoricalMetricsRequest(proto.Message):
    r"""Request message for
    [KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics][google.ads.googleads.v12.services.KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics].

    Attributes:
        customer_id (str):
            The ID of the customer with the
            recommendation.
        keywords (Sequence[str]):
            A list of keywords to get historical metrics.
            Not all inputs will be returned as a result of
            near-exact deduplication. For example, if stats
            for "car" and "cars" are requested, only "car"
            will be returned.
            A maximum of 10,000 keywords can be used.
        language (str):
            The resource name of the language to target.
            Each keyword belongs to some set of languages; a
            keyword is included if language is one of its
            languages.
            If not set, all keywords will be included.

            This field is a member of `oneof`_ ``_language``.
        include_adult_keywords (bool):
            If true, adult keywords will be included in
            response. The default value is false.
        geo_target_constants (Sequence[str]):
            The resource names of the location to target.
            Maximum is 10. An empty list MAY be used to
            specify all targeting geos.
        keyword_plan_network (google.ads.googleads.v12.enums.types.KeywordPlanNetworkEnum.KeywordPlanNetwork):
            Targeting network.
            If not set, Google Search And Partners Network
            will be used.
        aggregate_metrics (google.ads.googleads.v12.common.types.KeywordPlanAggregateMetrics):
            The aggregate fields to include in response.
        historical_metrics_options (google.ads.googleads.v12.common.types.HistoricalMetricsOptions):
            The options for historical metrics data.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    keywords = proto.RepeatedField(proto.STRING, number=2,)
    language = proto.Field(proto.STRING, number=4, optional=True,)
    include_adult_keywords = proto.Field(proto.BOOL, number=5,)
    geo_target_constants = proto.RepeatedField(proto.STRING, number=6,)
    keyword_plan_network = proto.Field(
        proto.ENUM,
        number=7,
        enum=gage_keyword_plan_network.KeywordPlanNetworkEnum.KeywordPlanNetwork,
    )
    aggregate_metrics = proto.Field(
        proto.MESSAGE,
        number=8,
        message=keyword_plan_common.KeywordPlanAggregateMetrics,
    )
    historical_metrics_options = proto.Field(
        proto.MESSAGE,
        number=3,
        message=keyword_plan_common.HistoricalMetricsOptions,
    )


class GenerateKeywordHistoricalMetricsResponse(proto.Message):
    r"""Response message for
    [KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics][google.ads.googleads.v12.services.KeywordPlanIdeaService.GenerateKeywordHistoricalMetrics].

    Attributes:
        results (Sequence[google.ads.googleads.v12.services.types.GenerateKeywordHistoricalMetricsResult]):
            List of keywords and their historical
            metrics.
        aggregate_metric_results (google.ads.googleads.v12.common.types.KeywordPlanAggregateMetricResults):
            The aggregate metrics for all keywords.
    """

    results = proto.RepeatedField(
        proto.MESSAGE,
        number=1,
        message="GenerateKeywordHistoricalMetricsResult",
    )
    aggregate_metric_results = proto.Field(
        proto.MESSAGE,
        number=2,
        message=keyword_plan_common.KeywordPlanAggregateMetricResults,
    )


class GenerateKeywordHistoricalMetricsResult(proto.Message):
    r"""The result of generating keyword historical metrics.

    Attributes:
        text (str):
            The text of the query associated with one or more keywords.
            Note that we de-dupe your keywords list, eliminating close
            variants before returning the keywords as text. For example,
            if your request originally contained the keywords "car" and
            "cars", the returned search query will only contain "cars".
            The list of de-duped queries will be included in
            close_variants field.

            This field is a member of `oneof`_ ``_text``.
        close_variants (Sequence[str]):
            The list of close variants from the requested
            keywords whose stats are combined into this
            GenerateKeywordHistoricalMetricsResult.
        keyword_metrics (google.ads.googleads.v12.common.types.KeywordPlanHistoricalMetrics):
            The historical metrics for text and its close
            variants
    """

    text = proto.Field(proto.STRING, number=1, optional=True,)
    close_variants = proto.RepeatedField(proto.STRING, number=3,)
    keyword_metrics = proto.Field(
        proto.MESSAGE,
        number=2,
        message=keyword_plan_common.KeywordPlanHistoricalMetrics,
    )


class GenerateAdGroupThemesRequest(proto.Message):
    r"""Request message for
    [KeywordPlanIdeaService.GenerateAdGroupThemes][google.ads.googleads.v12.services.KeywordPlanIdeaService.GenerateAdGroupThemes].

    Attributes:
        customer_id (str):
            Required. The ID of the customer.
        keywords (Sequence[str]):
            Required. A list of keywords to group into
            the provided AdGroups.
        ad_groups (Sequence[str]):
            Required. A list of resource names of AdGroups to group
            keywords into. Resource name format:
            ``customers/{customer_id}/adGroups/{ad_group_id}``
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    keywords = proto.RepeatedField(proto.STRING, number=2,)
    ad_groups = proto.RepeatedField(proto.STRING, number=3,)


class GenerateAdGroupThemesResponse(proto.Message):
    r"""Response message for
    [KeywordPlanIdeaService.GenerateAdGroupThemes][google.ads.googleads.v12.services.KeywordPlanIdeaService.GenerateAdGroupThemes].

    Attributes:
        ad_group_keyword_suggestions (Sequence[google.ads.googleads.v12.services.types.AdGroupKeywordSuggestion]):
            A list of suggested AdGroup/keyword pairings.
        unusable_ad_groups (Sequence[google.ads.googleads.v12.services.types.UnusableAdGroup]):
            A list of provided AdGroups that could not be
            used as suggestions.
    """

    ad_group_keyword_suggestions = proto.RepeatedField(
        proto.MESSAGE, number=1, message="AdGroupKeywordSuggestion",
    )
    unusable_ad_groups = proto.RepeatedField(
        proto.MESSAGE, number=2, message="UnusableAdGroup",
    )


class AdGroupKeywordSuggestion(proto.Message):
    r"""The suggested text and AdGroup/Campaign pairing for a given
    keyword.

    Attributes:
        keyword_text (str):
            The original keyword text.
        suggested_keyword_text (str):
            The normalized version of keyword_text for
            BROAD/EXACT/PHRASE suggestions.
        suggested_match_type (google.ads.googleads.v12.enums.types.KeywordMatchTypeEnum.KeywordMatchType):
            The suggested keyword match type.
        suggested_ad_group (str):
            The suggested AdGroup for the keyword. Resource name format:
            ``customers/{customer_id}/adGroups/{ad_group_id}``
        suggested_campaign (str):
            The suggested Campaign for the keyword. Resource name
            format: ``customers/{customer_id}/campaigns/{campaign_id}``
    """

    keyword_text = proto.Field(proto.STRING, number=1,)
    suggested_keyword_text = proto.Field(proto.STRING, number=2,)
    suggested_match_type = proto.Field(
        proto.ENUM,
        number=3,
        enum=keyword_match_type.KeywordMatchTypeEnum.KeywordMatchType,
    )
    suggested_ad_group = proto.Field(proto.STRING, number=4,)
    suggested_campaign = proto.Field(proto.STRING, number=5,)


class UnusableAdGroup(proto.Message):
    r"""An AdGroup/Campaign pair that could not be used as a suggestion for
    keywords.

    AdGroups may not be usable if the AdGroup

    -  belongs to a Campaign that is not ENABLED or PAUSED
    -  is itself not ENABLED

    Attributes:
        ad_group (str):
            The AdGroup resource name. Resource name format:
            ``customers/{customer_id}/adGroups/{ad_group_id}``
        campaign (str):
            The Campaign resource name. Resource name format:
            ``customers/{customer_id}/campaigns/{campaign_id}``
    """

    ad_group = proto.Field(proto.STRING, number=1,)
    campaign = proto.Field(proto.STRING, number=2,)


__all__ = tuple(sorted(__protobuf__.manifest))
