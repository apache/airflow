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

from airflow.providers.google_vendor.googleads.v12.enums.types import access_invitation_status
from airflow.providers.google_vendor.googleads.v12.enums.types import access_role as gage_access_role


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"CustomerUserAccessInvitation",},
)


class CustomerUserAccessInvitation(proto.Message):
    r"""Represent an invitation to a new user on this customer
    account.

    Attributes:
        resource_name (str):
            Immutable. Name of the resource. Resource names have the
            form:
            ``customers/{customer_id}/customerUserAccessInvitations/{invitation_id}``
        invitation_id (int):
            Output only. The ID of the invitation.
            This field is read-only.
        access_role (google.ads.googleads.v12.enums.types.AccessRoleEnum.AccessRole):
            Immutable. Access role of the user.
        email_address (str):
            Immutable. Email address the invitation was
            sent to. This can differ from the email address
            of the account that accepts the invite.
        creation_date_time (str):
            Output only. Time invitation was created.
            This field is read-only.
            The format is "YYYY-MM-DD HH:MM:SS".
            Examples: "2018-03-05 09:15:00" or "2018-02-01
            14:34:30".
        invitation_status (google.ads.googleads.v12.enums.types.AccessInvitationStatusEnum.AccessInvitationStatus):
            Output only. Invitation status of the user.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    invitation_id = proto.Field(proto.INT64, number=2,)
    access_role = proto.Field(
        proto.ENUM, number=3, enum=gage_access_role.AccessRoleEnum.AccessRole,
    )
    email_address = proto.Field(proto.STRING, number=4,)
    creation_date_time = proto.Field(proto.STRING, number=5,)
    invitation_status = proto.Field(
        proto.ENUM,
        number=6,
        enum=access_invitation_status.AccessInvitationStatusEnum.AccessInvitationStatus,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
