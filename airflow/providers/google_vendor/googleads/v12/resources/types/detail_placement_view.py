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

from airflow.providers.google_vendor.googleads.v12.enums.types import (
    placement_type as gage_placement_type,
)


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"DetailPlacementView",},
)


class DetailPlacementView(proto.Message):
    r"""A view with metrics aggregated by ad group and URL or YouTube
    video.

    Attributes:
        resource_name (str):
            Output only. The resource name of the detail placement view.
            Detail placement view resource names have the form:

            ``customers/{customer_id}/detailPlacementViews/{ad_group_id}~{base64_placement}``
        placement (str):
            Output only. The automatic placement string
            at detail level, e. g. website URL, mobile
            application ID, or a YouTube video ID.

            This field is a member of `oneof`_ ``_placement``.
        display_name (str):
            Output only. The display name is URL name for
            websites, YouTube video name for YouTube videos,
            and translated mobile app name for mobile apps.

            This field is a member of `oneof`_ ``_display_name``.
        group_placement_target_url (str):
            Output only. URL of the group placement, for
            example, domain, link to the mobile application
            in app store, or a YouTube channel URL.

            This field is a member of `oneof`_ ``_group_placement_target_url``.
        target_url (str):
            Output only. URL of the placement, for
            example, website, link to the mobile application
            in app store, or a YouTube video URL.

            This field is a member of `oneof`_ ``_target_url``.
        placement_type (google.ads.googleads.v12.enums.types.PlacementTypeEnum.PlacementType):
            Output only. Type of the placement, for
            example, Website, YouTube Video, and Mobile
            Application.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    placement = proto.Field(proto.STRING, number=7, optional=True,)
    display_name = proto.Field(proto.STRING, number=8, optional=True,)
    group_placement_target_url = proto.Field(
        proto.STRING, number=9, optional=True,
    )
    target_url = proto.Field(proto.STRING, number=10, optional=True,)
    placement_type = proto.Field(
        proto.ENUM,
        number=6,
        enum=gage_placement_type.PlacementTypeEnum.PlacementType,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
