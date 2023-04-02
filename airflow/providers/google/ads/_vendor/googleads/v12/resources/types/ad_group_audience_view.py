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
    package="google.ads.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"AdGroupAudienceView",},
)


class AdGroupAudienceView(proto.Message):
    r"""An ad group audience view.
    Includes performance data from interests and remarketing lists
    for Display Network and YouTube Network ads, and remarketing
    lists for search ads (RLSA), aggregated at the audience level.

    Attributes:
        resource_name (str):
            Output only. The resource name of the ad group audience
            view. Ad group audience view resource names have the form:

            ``customers/{customer_id}/adGroupAudienceViews/{ad_group_id}~{criterion_id}``
    """

    resource_name = proto.Field(proto.STRING, number=1,)


__all__ = tuple(sorted(__protobuf__.manifest))
