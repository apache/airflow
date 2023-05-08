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


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"DetailedDemographic",},
)


class DetailedDemographic(proto.Message):
    r"""A detailed demographic: a particular interest-based vertical
    to be targeted to reach users based on long-term life facts.

    Attributes:
        resource_name (str):
            Output only. The resource name of the detailed demographic.
            Detailed demographic resource names have the form:

            ``customers/{customer_id}/detailedDemographics/{detailed_demographic_id}``
        id (int):
            Output only. The ID of the detailed
            demographic.
        name (str):
            Output only. The name of the detailed
            demographic. For example,"Highest Level of
            Educational Attainment".
        parent (str):
            Output only. The parent of the detailed_demographic.
        launched_to_all (bool):
            Output only. True if the detailed demographic
            is launched to all channels and locales.
        availabilities (Sequence[google.ads.googleads.v12.common.types.CriterionCategoryAvailability]):
            Output only. Availability information of the
            detailed demographic.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    name = proto.Field(proto.STRING, number=3,)
    parent = proto.Field(proto.STRING, number=4,)
    launched_to_all = proto.Field(proto.BOOL, number=5,)
    availabilities = proto.RepeatedField(
        proto.MESSAGE,
        number=6,
        message=criterion_category_availability.CriterionCategoryAvailability,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
