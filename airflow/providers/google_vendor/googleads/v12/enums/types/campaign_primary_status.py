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
    manifest={"CampaignPrimaryStatusEnum",},
)


class CampaignPrimaryStatusEnum(proto.Message):
    r"""Container for enum describing possible campaign primary
    status.

    """

    class CampaignPrimaryStatus(proto.Enum):
        r"""Enum describing the possible campaign primary status.
        Provides insight into why a campaign is not serving or not
        serving optimally. Modification to the campaign and its related
        entities might take a while to be reflected in this status.
        """
        UNSPECIFIED = 0
        UNKNOWN = 1
        ELIGIBLE = 2
        PAUSED = 3
        REMOVED = 4
        ENDED = 5
        PENDING = 6
        MISCONFIGURED = 7
        LIMITED = 8
        LEARNING = 9
        NOT_ELIGIBLE = 10


__all__ = tuple(sorted(__protobuf__.manifest))
