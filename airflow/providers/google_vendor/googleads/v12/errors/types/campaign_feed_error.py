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
    manifest={"CampaignFeedErrorEnum",},
)


class CampaignFeedErrorEnum(proto.Message):
    r"""Container for enum describing possible campaign feed errors.
    """

    class CampaignFeedError(proto.Enum):
        r"""Enum describing possible campaign feed errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        FEED_ALREADY_EXISTS_FOR_PLACEHOLDER_TYPE = 2
        CANNOT_CREATE_FOR_REMOVED_FEED = 4
        CANNOT_CREATE_ALREADY_EXISTING_CAMPAIGN_FEED = 5
        CANNOT_MODIFY_REMOVED_CAMPAIGN_FEED = 6
        INVALID_PLACEHOLDER_TYPE = 7
        MISSING_FEEDMAPPING_FOR_PLACEHOLDER_TYPE = 8
        NO_EXISTING_LOCATION_CUSTOMER_FEED = 9
        LEGACY_FEED_TYPE_READ_ONLY = 10


__all__ = tuple(sorted(__protobuf__.manifest))
