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
    manifest={"ResourceCountLimitExceededErrorEnum",},
)


class ResourceCountLimitExceededErrorEnum(proto.Message):
    r"""Container for enum describing possible resource count limit
    exceeded errors.

    """

    class ResourceCountLimitExceededError(proto.Enum):
        r"""Enum describing possible resource count limit exceeded
        errors.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        ACCOUNT_LIMIT = 2
        CAMPAIGN_LIMIT = 3
        ADGROUP_LIMIT = 4
        AD_GROUP_AD_LIMIT = 5
        AD_GROUP_CRITERION_LIMIT = 6
        SHARED_SET_LIMIT = 7
        MATCHING_FUNCTION_LIMIT = 8
        RESPONSE_ROW_LIMIT_EXCEEDED = 9
        RESOURCE_LIMIT = 10


__all__ = tuple(sorted(__protobuf__.manifest))
