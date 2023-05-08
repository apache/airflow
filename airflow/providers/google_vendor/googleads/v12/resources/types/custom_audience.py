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

from airflow.providers.google_vendor.googleads.v12.enums.types import custom_audience_member_type
from airflow.providers.google_vendor.googleads.v12.enums.types import custom_audience_status
from airflow.providers.google_vendor.googleads.v12.enums.types import custom_audience_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomAudience", "CustomAudienceMember",},
)


class CustomAudience(proto.Message):
    r"""A custom audience. This is a list of users by interest.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the custom audience. Custom
            audience resource names have the form:

            ``customers/{customer_id}/customAudiences/{custom_audience_id}``
        id (int):
            Output only. ID of the custom audience.
        status (google.ads.googleads.v12.enums.types.CustomAudienceStatusEnum.CustomAudienceStatus):
            Output only. Status of this custom audience.
            Indicates whether the custom audience is enabled
            or removed.
        name (str):
            Name of the custom audience. It should be
            unique for all custom audiences created by a
            customer. This field is required for creating
            operations.
        type_ (google.ads.googleads.v12.enums.types.CustomAudienceTypeEnum.CustomAudienceType):
            Type of the custom audience. ("INTEREST" OR
            "PURCHASE_INTENT" is not allowed for newly created custom
            audience but kept for existing audiences)
        description (str):
            Description of this custom audience.
        members (Sequence[google.ads.googleads.v12.resources.types.CustomAudienceMember]):
            List of custom audience members that this
            custom audience is composed of. Members can be
            added during CustomAudience creation. If members
            are presented in UPDATE operation, existing
            members will be overridden.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=2,)
    status = proto.Field(
        proto.ENUM,
        number=3,
        enum=custom_audience_status.CustomAudienceStatusEnum.CustomAudienceStatus,
    )
    name = proto.Field(proto.STRING, number=4,)
    type_ = proto.Field(
        proto.ENUM,
        number=5,
        enum=custom_audience_type.CustomAudienceTypeEnum.CustomAudienceType,
    )
    description = proto.Field(proto.STRING, number=6,)
    members = proto.RepeatedField(
        proto.MESSAGE, number=7, message="CustomAudienceMember",
    )


class CustomAudienceMember(proto.Message):
    r"""A member of custom audience. A member can be a KEYWORD, URL,
    PLACE_CATEGORY or APP. It can only be created or removed but not
    changed.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        member_type (google.ads.googleads.v12.enums.types.CustomAudienceMemberTypeEnum.CustomAudienceMemberType):
            The type of custom audience member, KEYWORD, URL,
            PLACE_CATEGORY or APP.
        keyword (str):
            A keyword or keyword phrase — at most 10
            words and 80 characters. Languages with
            double-width characters such as Chinese,
            Japanese, or Korean, are allowed 40 characters,
            which describes the user's interests or actions.

            This field is a member of `oneof`_ ``value``.
        url (str):
            An HTTP URL, protocol-included — at most 2048
            characters, which includes contents users have
            interests in.

            This field is a member of `oneof`_ ``value``.
        place_category (int):
            A place type described by a place category
            users visit.

            This field is a member of `oneof`_ ``value``.
        app (str):
            A package name of Android apps which users
            installed such as com.google.example.

            This field is a member of `oneof`_ ``value``.
    """

    member_type = proto.Field(
        proto.ENUM,
        number=1,
        enum=custom_audience_member_type.CustomAudienceMemberTypeEnum.CustomAudienceMemberType,
    )
    keyword = proto.Field(proto.STRING, number=2, oneof="value",)
    url = proto.Field(proto.STRING, number=3, oneof="value",)
    place_category = proto.Field(proto.INT64, number=4, oneof="value",)
    app = proto.Field(proto.STRING, number=5, oneof="value",)


__all__ = tuple(sorted(__protobuf__.manifest))
