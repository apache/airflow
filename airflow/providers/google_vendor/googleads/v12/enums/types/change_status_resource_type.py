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
    package="airflow.providers.google_vendor.googleads.v12.enums",
    marshal="google.ads.googleads.v12",
    manifest={"ChangeStatusResourceTypeEnum",},
)


class ChangeStatusResourceTypeEnum(proto.Message):
    r"""Container for enum describing supported resource types for
    the ChangeStatus resource.

    """

    class ChangeStatusResourceType(proto.Enum):
        r"""Enum listing the resource types support by the ChangeStatus
        resource.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_GROUP = 3
        AD_GROUP_AD = 4
        AD_GROUP_CRITERION = 5
        CAMPAIGN = 6
        CAMPAIGN_CRITERION = 7
        FEED = 9
        FEED_ITEM = 10
        AD_GROUP_FEED = 11
        CAMPAIGN_FEED = 12
        AD_GROUP_BID_MODIFIER = 13
        SHARED_SET = 14
        CAMPAIGN_SHARED_SET = 15
        ASSET = 16
        CUSTOMER_ASSET = 17
        CAMPAIGN_ASSET = 18
        AD_GROUP_ASSET = 19


__all__ = tuple(sorted(__protobuf__.manifest))
