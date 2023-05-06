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

from airflow.providers.google_vendor.googleads.v12.common.types import (
    criterion_category_availability,
)
from airflow.providers.google_vendor.googleads.v12.enums.types import user_interest_taxonomy_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"UserInterest",},
)


class UserInterest(proto.Message):
    r"""A user interest: a particular interest-based vertical to be
    targeted.

    Attributes:
        resource_name (str):
            Output only. The resource name of the user interest. User
            interest resource names have the form:

            ``customers/{customer_id}/userInterests/{user_interest_id}``
        taxonomy_type (google.ads.googleads.v12.enums.types.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType):
            Output only. Taxonomy type of the user
            interest.
        user_interest_id (int):
            Output only. The ID of the user interest.

            This field is a member of `oneof`_ ``_user_interest_id``.
        name (str):
            Output only. The name of the user interest.

            This field is a member of `oneof`_ ``_name``.
        user_interest_parent (str):
            Output only. The parent of the user interest.

            This field is a member of `oneof`_ ``_user_interest_parent``.
        launched_to_all (bool):
            Output only. True if the user interest is
            launched to all channels and locales.

            This field is a member of `oneof`_ ``_launched_to_all``.
        availabilities (Sequence[google.ads.googleads.v12.common.types.CriterionCategoryAvailability]):
            Output only. Availability information of the
            user interest.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    taxonomy_type = proto.Field(
        proto.ENUM,
        number=2,
        enum=user_interest_taxonomy_type.UserInterestTaxonomyTypeEnum.UserInterestTaxonomyType,
    )
    user_interest_id = proto.Field(proto.INT64, number=8, optional=True,)
    name = proto.Field(proto.STRING, number=9, optional=True,)
    user_interest_parent = proto.Field(proto.STRING, number=10, optional=True,)
    launched_to_all = proto.Field(proto.BOOL, number=11, optional=True,)
    availabilities = proto.RepeatedField(
        proto.MESSAGE,
        number=7,
        message=criterion_category_availability.CriterionCategoryAvailability,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
