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
    manifest={"CampaignDraftErrorEnum",},
)


class CampaignDraftErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign draft errors.
    """

    class CampaignDraftError(proto.Enum):
        r"""Enum describing possible campaign draft errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_DRAFT_NAME = 2
        INVALID_STATUS_TRANSITION_FROM_REMOVED = 3
        INVALID_STATUS_TRANSITION_FROM_PROMOTED = 4
        INVALID_STATUS_TRANSITION_FROM_PROMOTE_FAILED = 5
        CUSTOMER_CANNOT_CREATE_DRAFT = 6
        CAMPAIGN_CANNOT_CREATE_DRAFT = 7
        INVALID_DRAFT_CHANGE = 8
        INVALID_STATUS_TRANSITION = 9
        MAX_NUMBER_OF_DRAFTS_PER_CAMPAIGN_REACHED = 10
        LIST_ERRORS_FOR_PROMOTED_DRAFT_ONLY = 11


__all__ = tuple(sorted(__protobuf__.manifest))
