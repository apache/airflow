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
    manifest={"FeedItemTargetErrorEnum",},
)


class FeedItemTargetErrorEnum(proto.Message):
    r"""Container for enum describing possible feed item target
    errors.

    """

    class FeedItemTargetError(proto.Enum):
        r"""Enum describing possible feed item target errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        MUST_SET_TARGET_ONEOF_ON_CREATE = 2
        FEED_ITEM_TARGET_ALREADY_EXISTS = 3
        FEED_ITEM_SCHEDULES_CANNOT_OVERLAP = 4
        TARGET_LIMIT_EXCEEDED_FOR_GIVEN_TYPE = 5
        TOO_MANY_SCHEDULES_PER_DAY = 6
        CANNOT_HAVE_ENABLED_CAMPAIGN_AND_ENABLED_AD_GROUP_TARGETS = 7
        DUPLICATE_AD_SCHEDULE = 8
        DUPLICATE_KEYWORD = 9


__all__ = tuple(sorted(__protobuf__.manifest))
