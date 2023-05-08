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
    manifest={"UserLocationView",},
)


class UserLocationView(proto.Message):
    r"""A user location view.
    User Location View includes all metrics aggregated at the
    country level, one row per country. It reports metrics at the
    actual physical location of the user by targeted or not targeted
    location. If other segment fields are used, you may get more
    than one row per country.

    Attributes:
        resource_name (str):
            Output only. The resource name of the user location view.
            UserLocation view resource names have the form:

            ``customers/{customer_id}/userLocationViews/{country_criterion_id}~{targeting_location}``
        country_criterion_id (int):
            Output only. Criterion Id for the country.

            This field is a member of `oneof`_ ``_country_criterion_id``.
        targeting_location (bool):
            Output only. Indicates whether location was
            targeted or not.

            This field is a member of `oneof`_ ``_targeting_location``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    country_criterion_id = proto.Field(proto.INT64, number=4, optional=True,)
    targeting_location = proto.Field(proto.BOOL, number=5, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
