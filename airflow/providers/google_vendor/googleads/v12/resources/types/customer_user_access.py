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

from airflow.providers.google_vendor.googleads.v12.enums.types import access_role as gage_access_role


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomerUserAccess",},
)


class CustomerUserAccess(proto.Message):
    r"""Represents the permission of a single user onto a single
    customer.

    Attributes:
        resource_name (str):
            Immutable. Name of the resource. Resource names have the
            form:
            ``customers/{customer_id}/customerUserAccesses/{user_id}``
        user_id (int):
            Output only. User id of the user with the
            customer access. Read only field
        email_address (str):
            Output only. Email address of the user.
            Read only field

            This field is a member of `oneof`_ ``_email_address``.
        access_role (google.ads.googleads.v12.enums.types.AccessRoleEnum.AccessRole):
            Access role of the user.
        access_creation_date_time (str):
            Output only. The customer user access
            creation time. Read only field
            The format is "YYYY-MM-DD HH:MM:SS".
            Examples: "2018-03-05 09:15:00" or "2018-02-01
            14:34:30".

            This field is a member of `oneof`_ ``_access_creation_date_time``.
        inviter_user_email_address (str):
            Output only. The email address of the inviter
            user. Read only field

            This field is a member of `oneof`_ ``_inviter_user_email_address``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    user_id = proto.Field(proto.INT64, number=2,)
    email_address = proto.Field(proto.STRING, number=3, optional=True,)
    access_role = proto.Field(
        proto.ENUM, number=4, enum=gage_access_role.AccessRoleEnum.AccessRole,
    )
    access_creation_date_time = proto.Field(
        proto.STRING, number=6, optional=True,
    )
    inviter_user_email_address = proto.Field(
        proto.STRING, number=7, optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
