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
    goal_config_level as gage_goal_config_level,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"ConversionGoalCampaignConfig",},
)


class ConversionGoalCampaignConfig(proto.Message):
    r"""Conversion goal settings for a Campaign.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the conversion goal campaign
            config. Conversion goal campaign config resource names have
            the form:

            ``customers/{customer_id}/conversionGoalCampaignConfigs/{campaign_id}``
        campaign (str):
            Immutable. The campaign with which this
            conversion goal campaign config is associated.
        goal_config_level (google.ads.googleads.v12.enums.types.GoalConfigLevelEnum.GoalConfigLevel):
            The level of goal config the campaign is
            using.
        custom_conversion_goal (str):
            The custom conversion goal the campaign is
            using for optimization.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    campaign = proto.Field(proto.STRING, number=2,)
    goal_config_level = proto.Field(
        proto.ENUM,
        number=3,
        enum=gage_goal_config_level.GoalConfigLevelEnum.GoalConfigLevel,
    )
    custom_conversion_goal = proto.Field(proto.STRING, number=4,)


__all__ = tuple(sorted(__protobuf__.manifest))
