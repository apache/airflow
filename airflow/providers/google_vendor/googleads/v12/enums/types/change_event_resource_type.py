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
    manifest={"ChangeEventResourceTypeEnum",},
)


class ChangeEventResourceTypeEnum(proto.Message):
    r"""Container for enum describing supported resource types for
    the ChangeEvent resource.

    """

    class ChangeEventResourceType(proto.Enum):
        r"""Enum listing the resource types support by the ChangeEvent
        resource.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        AD = 2
        AD_GROUP = 3
        AD_GROUP_CRITERION = 4
        CAMPAIGN = 5
        CAMPAIGN_BUDGET = 6
        AD_GROUP_BID_MODIFIER = 7
        CAMPAIGN_CRITERION = 8
        FEED = 9
        FEED_ITEM = 10
        CAMPAIGN_FEED = 11
        AD_GROUP_FEED = 12
        AD_GROUP_AD = 13
        ASSET = 14
        CUSTOMER_ASSET = 15
        CAMPAIGN_ASSET = 16
        AD_GROUP_ASSET = 17
        ASSET_SET = 18
        ASSET_SET_ASSET = 19
        CAMPAIGN_ASSET_SET = 20


__all__ = tuple(sorted(__protobuf__.manifest))
