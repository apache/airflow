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
    package="airflow.providers.google_vendor.googleads.v12.errors",
    marshal="google.ads.googleads.v12",
    manifest={"AssetGroupListingGroupFilterErrorEnum",},
)


class AssetGroupListingGroupFilterErrorEnum(proto.Message):
    r"""Container for enum describing possible asset group listing
    group filter errors.

    """

    class AssetGroupListingGroupFilterError(proto.Enum):
        r"""Enum describing possible asset group listing group filter
        errors.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        TREE_TOO_DEEP = 2
        UNIT_CANNOT_HAVE_CHILDREN = 3
        SUBDIVISION_MUST_HAVE_EVERYTHING_ELSE_CHILD = 4
        DIFFERENT_DIMENSION_TYPE_BETWEEN_SIBLINGS = 5
        SAME_DIMENSION_VALUE_BETWEEN_SIBLINGS = 6
        SAME_DIMENSION_TYPE_BETWEEN_ANCESTORS = 7
        MULTIPLE_ROOTS = 8
        INVALID_DIMENSION_VALUE = 9
        MUST_REFINE_HIERARCHICAL_PARENT_TYPE = 10
        INVALID_PRODUCT_BIDDING_CATEGORY = 11
        CHANGING_CASE_VALUE_WITH_CHILDREN = 12
        SUBDIVISION_HAS_CHILDREN = 13
        CANNOT_REFINE_HIERARCHICAL_EVERYTHING_ELSE = 14


__all__ = tuple(sorted(__protobuf__.manifest))
