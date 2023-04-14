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
    manifest={"ExperimentErrorEnum",},
)


class ExperimentErrorEnum(proto.Message):
    r"""Container for enum describing possible experiment error.
    """

    class ExperimentError(proto.Enum):
        r"""Enum describing possible experiment errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_SET_START_DATE_IN_PAST = 2
        END_DATE_BEFORE_START_DATE = 3
        START_DATE_TOO_FAR_IN_FUTURE = 4
        DUPLICATE_EXPERIMENT_NAME = 5
        CANNOT_MODIFY_REMOVED_EXPERIMENT = 6
        START_DATE_ALREADY_PASSED = 7
        CANNOT_SET_END_DATE_IN_PAST = 8
        CANNOT_SET_STATUS_TO_REMOVED = 9
        CANNOT_MODIFY_PAST_END_DATE = 10
        INVALID_STATUS = 11
        INVALID_CAMPAIGN_CHANNEL_TYPE = 12
        OVERLAPPING_MEMBERS_AND_DATE_RANGE = 13
        INVALID_TRIAL_ARM_TRAFFIC_SPLIT = 14
        TRAFFIC_SPLIT_OVERLAPPING = 15
        SUM_TRIAL_ARM_TRAFFIC_UNEQUALS_TO_TRIAL_TRAFFIC_SPLIT_DENOMINATOR = 16
        CANNOT_MODIFY_TRAFFIC_SPLIT_AFTER_START = 17
        EXPERIMENT_NOT_FOUND = 18
        EXPERIMENT_NOT_YET_STARTED = 19
        CANNOT_HAVE_MULTIPLE_CONTROL_ARMS = 20
        IN_DESIGN_CAMPAIGNS_NOT_SET = 21
        CANNOT_SET_STATUS_TO_GRADUATED = 22
        CANNOT_CREATE_EXPERIMENT_CAMPAIGN_WITH_SHARED_BUDGET = 23
        CANNOT_CREATE_EXPERIMENT_CAMPAIGN_WITH_CUSTOM_BUDGET = 24
        STATUS_TRANSITION_INVALID = 25


__all__ = tuple(sorted(__protobuf__.manifest))
