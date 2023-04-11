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


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"DomainCategory",},
)


class DomainCategory(proto.Message):
    r"""A category generated automatically by crawling a domain. If a
    campaign uses the DynamicSearchAdsSetting, then domain
    categories will be generated for the domain. The categories can
    be targeted using WebpageConditionInfo. See:
    https://support.google.com/google-ads/answer/2471185

    Attributes:
        resource_name (str):
            Output only. The resource name of the domain category.
            Domain category resource names have the form:

            ``customers/{customer_id}/domainCategories/{campaign_id}~{category_base64}~{language_code}``
        campaign (str):
            Output only. The campaign this category is
            recommended for.

            This field is a member of `oneof`_ ``_campaign``.
        category (str):
            Output only. Recommended category for the
            website domain, for example, if you have a
            website about electronics, the categories could
            be "cameras", "televisions", etc.

            This field is a member of `oneof`_ ``_category``.
        language_code (str):
            Output only. The language code specifying the
            language of the website, for example, "en" for
            English. The language can be specified in the
            DynamicSearchAdsSetting required for dynamic
            search ads. This is the language of the pages
            from your website that you want Google Ads to
            find, create ads for, and match searches with.

            This field is a member of `oneof`_ ``_language_code``.
        domain (str):
            Output only. The domain for the website. The
            domain can be specified in the
            DynamicSearchAdsSetting required for dynamic
            search ads.

            This field is a member of `oneof`_ ``_domain``.
        coverage_fraction (float):
            Output only. Fraction of pages on your site
            that this category matches.

            This field is a member of `oneof`_ ``_coverage_fraction``.
        category_rank (int):
            Output only. The position of this category in
            the set of categories. Lower numbers indicate a
            better match for the domain. null indicates not
            recommended.

            This field is a member of `oneof`_ ``_category_rank``.
        has_children (bool):
            Output only. Indicates whether this category
            has sub-categories.

            This field is a member of `oneof`_ ``_has_children``.
        recommended_cpc_bid_micros (int):
            Output only. The recommended cost per click
            for the category.

            This field is a member of `oneof`_ ``_recommended_cpc_bid_micros``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    campaign = proto.Field(proto.STRING, number=10, optional=True,)
    category = proto.Field(proto.STRING, number=11, optional=True,)
    language_code = proto.Field(proto.STRING, number=12, optional=True,)
    domain = proto.Field(proto.STRING, number=13, optional=True,)
    coverage_fraction = proto.Field(proto.DOUBLE, number=14, optional=True,)
    category_rank = proto.Field(proto.INT64, number=15, optional=True,)
    has_children = proto.Field(proto.BOOL, number=16, optional=True,)
    recommended_cpc_bid_micros = proto.Field(
        proto.INT64, number=17, optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
