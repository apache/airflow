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

from airflow.providers.google_vendor.googleads.v12.common.types import criteria
from airflow.providers.google_vendor.googleads.v12.enums.types import criterion_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomerNegativeCriterion",},
)


class CustomerNegativeCriterion(proto.Message):
    r"""A negative criterion for exclusions at the customer level.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        resource_name (str):
            Immutable. The resource name of the customer negative
            criterion. Customer negative criterion resource names have
            the form:

            ``customers/{customer_id}/customerNegativeCriteria/{criterion_id}``
        id (int):
            Output only. The ID of the criterion.

            This field is a member of `oneof`_ ``_id``.
        type_ (google.ads.googleads.v12.enums.types.CriterionTypeEnum.CriterionType):
            Output only. The type of the criterion.
        content_label (google.ads.googleads.v12.common.types.ContentLabelInfo):
            Immutable. ContentLabel.

            This field is a member of `oneof`_ ``criterion``.
        mobile_application (google.ads.googleads.v12.common.types.MobileApplicationInfo):
            Immutable. MobileApplication.

            This field is a member of `oneof`_ ``criterion``.
        mobile_app_category (google.ads.googleads.v12.common.types.MobileAppCategoryInfo):
            Immutable. MobileAppCategory.

            This field is a member of `oneof`_ ``criterion``.
        placement (google.ads.googleads.v12.common.types.PlacementInfo):
            Immutable. Placement.

            This field is a member of `oneof`_ ``criterion``.
        youtube_video (google.ads.googleads.v12.common.types.YouTubeVideoInfo):
            Immutable. YouTube Video.

            This field is a member of `oneof`_ ``criterion``.
        youtube_channel (google.ads.googleads.v12.common.types.YouTubeChannelInfo):
            Immutable. YouTube Channel.

            This field is a member of `oneof`_ ``criterion``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=10, optional=True,)
    type_ = proto.Field(
        proto.ENUM,
        number=3,
        enum=criterion_type.CriterionTypeEnum.CriterionType,
    )
    content_label = proto.Field(
        proto.MESSAGE,
        number=4,
        oneof="criterion",
        message=criteria.ContentLabelInfo,
    )
    mobile_application = proto.Field(
        proto.MESSAGE,
        number=5,
        oneof="criterion",
        message=criteria.MobileApplicationInfo,
    )
    mobile_app_category = proto.Field(
        proto.MESSAGE,
        number=6,
        oneof="criterion",
        message=criteria.MobileAppCategoryInfo,
    )
    placement = proto.Field(
        proto.MESSAGE,
        number=7,
        oneof="criterion",
        message=criteria.PlacementInfo,
    )
    youtube_video = proto.Field(
        proto.MESSAGE,
        number=8,
        oneof="criterion",
        message=criteria.YouTubeVideoInfo,
    )
    youtube_channel = proto.Field(
        proto.MESSAGE,
        number=9,
        oneof="criterion",
        message=criteria.YouTubeChannelInfo,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
