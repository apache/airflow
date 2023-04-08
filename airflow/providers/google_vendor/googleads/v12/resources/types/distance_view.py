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

from airflow.providers.google_vendor.googleads.v12.enums.types import (
    distance_bucket as gage_distance_bucket,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"DistanceView",},
)


class DistanceView(proto.Message):
    r"""A distance view with metrics aggregated by the user's
    distance from an advertiser's location extensions. Each
    DistanceBucket includes all impressions that fall within its
    distance and a single impression will contribute to the metrics
    for all DistanceBuckets that include the user's distance.

    Attributes:
        resource_name (str):
            Output only. The resource name of the distance view.
            Distance view resource names have the form:

            ``customers/{customer_id}/distanceViews/1~{distance_bucket}``
        distance_bucket (google.ads.googleads.v12.enums.types.DistanceBucketEnum.DistanceBucket):
            Output only. Grouping of user distance from
            location extensions.
        metric_system (bool):
            Output only. True if the DistanceBucket is
            using the metric system, false otherwise.

            This field is a member of `oneof`_ ``_metric_system``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    distance_bucket = proto.Field(
        proto.ENUM,
        number=2,
        enum=gage_distance_bucket.DistanceBucketEnum.DistanceBucket,
    )
    metric_system = proto.Field(proto.BOOL, number=4, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
