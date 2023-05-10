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
    manifest={"CampaignExperimentErrorEnum",},
)


class CampaignExperimentErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign experiment
    errors.

    """

    class CampaignExperimentError(proto.Enum):
        r"""Enum describing possible campaign experiment errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_NAME = 2
        INVALID_TRANSITION = 3
        CANNOT_CREATE_EXPERIMENT_WITH_SHARED_BUDGET = 4
        CANNOT_CREATE_EXPERIMENT_FOR_REMOVED_BASE_CAMPAIGN = 5
        CANNOT_CREATE_EXPERIMENT_FOR_NON_PROPOSED_DRAFT = 6
        CUSTOMER_CANNOT_CREATE_EXPERIMENT = 7
        CAMPAIGN_CANNOT_CREATE_EXPERIMENT = 8
        EXPERIMENT_DURATIONS_MUST_NOT_OVERLAP = 9
        EXPERIMENT_DURATION_MUST_BE_WITHIN_CAMPAIGN_DURATION = 10
        CANNOT_MUTATE_EXPERIMENT_DUE_TO_STATUS = 11


__all__ = tuple(sorted(__protobuf__.manifest))
