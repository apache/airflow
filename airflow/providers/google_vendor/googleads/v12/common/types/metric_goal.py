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

from airflow.providers.google_vendor.googleads.v12.enums.types import experiment_metric
from airflow.providers.google_vendor.googleads.v12.enums.types import experiment_metric_direction


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.common",
    marshal="google.ads.googleads.v12",
    manifest={"MetricGoal",},
)


class MetricGoal(proto.Message):
    r"""A metric goal for an experiment.

    Attributes:
        metric (google.ads.googleads.v12.enums.types.ExperimentMetricEnum.ExperimentMetric):
            The metric of the goal. For example, clicks,
            impressions, cost, conversions, etc.
        direction (google.ads.googleads.v12.enums.types.ExperimentMetricDirectionEnum.ExperimentMetricDirection):
            The metric direction of the goal. For
            example, increase, decrease, no change.
    """

    metric = proto.Field(
        proto.ENUM,
        number=1,
        enum=experiment_metric.ExperimentMetricEnum.ExperimentMetric,
    )
    direction = proto.Field(
        proto.ENUM,
        number=2,
        enum=experiment_metric_direction.ExperimentMetricDirectionEnum.ExperimentMetricDirection,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
