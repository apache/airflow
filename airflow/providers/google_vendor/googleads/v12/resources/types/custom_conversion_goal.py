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

from airflow.providers.google_vendor.googleads.v12.enums.types import custom_conversion_goal_status


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomConversionGoal",},
)


class CustomConversionGoal(proto.Message):
    r"""Custom conversion goal that can make arbitrary conversion
    actions biddable.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the custom conversion goal.
            Custom conversion goal resource names have the form:

            ``customers/{customer_id}/customConversionGoals/{goal_id}``
        id (int):
            Immutable. The ID for this custom conversion
            goal.
        name (str):
            The name for this custom conversion goal.
        conversion_actions (Sequence[str]):
            Conversion actions that the custom conversion
            goal makes biddable.
        status (google.ads.googleads.v12.enums.types.CustomConversionGoalStatusEnum.CustomConversionGoalStatus):
            The status of the custom conversion goal.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    name = proto.Field(proto.STRING, number=3,)
    conversion_actions = proto.RepeatedField(proto.STRING, number=4,)
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=custom_conversion_goal_status.CustomConversionGoalStatusEnum.CustomConversionGoalStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
