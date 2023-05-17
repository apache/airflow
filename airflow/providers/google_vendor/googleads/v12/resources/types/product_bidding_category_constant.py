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

from airflow.providers.google_vendor.googleads.v12.enums.types import product_bidding_category_level
from airflow.providers.google_vendor.googleads.v12.enums.types import product_bidding_category_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"ProductBiddingCategoryConstant",},
)


class ProductBiddingCategoryConstant(proto.Message):
    r"""A Product Bidding Category.

    Attributes:
        resource_name (str):
            Output only. The resource name of the product bidding
            category. Product bidding category resource names have the
            form:

            ``productBiddingCategoryConstants/{country_code}~{level}~{id}``
        id (int):
            Output only. ID of the product bidding category.

            This ID is equivalent to the google_product_category ID as
            described in this article:
            https://support.google.com/merchants/answer/6324436.

            This field is a member of `oneof`_ ``_id``.
        country_code (str):
            Output only. Two-letter upper-case country
            code of the product bidding category.

            This field is a member of `oneof`_ ``_country_code``.
        product_bidding_category_constant_parent (str):
            Output only. Resource name of the parent
            product bidding category.

            This field is a member of `oneof`_ ``_product_bidding_category_constant_parent``.
        level (google.ads.googleads.v12.enums.types.ProductBiddingCategoryLevelEnum.ProductBiddingCategoryLevel):
            Output only. Level of the product bidding
            category.
        status (google.ads.googleads.v12.enums.types.ProductBiddingCategoryStatusEnum.ProductBiddingCategoryStatus):
            Output only. Status of the product bidding
            category.
        language_code (str):
            Output only. Language code of the product
            bidding category.

            This field is a member of `oneof`_ ``_language_code``.
        localized_name (str):
            Output only. Display value of the product bidding category
            localized according to language_code.

            This field is a member of `oneof`_ ``_localized_name``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=10, optional=True,)
    country_code = proto.Field(proto.STRING, number=11, optional=True,)
    product_bidding_category_constant_parent = proto.Field(
        proto.STRING, number=12, optional=True,
    )
    level = proto.Field(
        proto.ENUM,
        number=5,
        enum=product_bidding_category_level.ProductBiddingCategoryLevelEnum.ProductBiddingCategoryLevel,
    )
    status = proto.Field(
        proto.ENUM,
        number=6,
        enum=product_bidding_category_status.ProductBiddingCategoryStatusEnum.ProductBiddingCategoryStatus,
    )
    language_code = proto.Field(proto.STRING, number=13, optional=True,)
    localized_name = proto.Field(proto.STRING, number=14, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
