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

from airflow.providers.google_vendor.googleads.v12.common.types import offline_user_data


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.services",
    marshal="google.ads.googleads.v12",
    manifest={
        "UploadUserDataRequest",
        "UserDataOperation",
        "UploadUserDataResponse",
    },
)


class UploadUserDataRequest(proto.Message):
    r"""Request message for
    [UserDataService.UploadUserData][google.ads.googleads.v12.services.UserDataService.UploadUserData]


    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        customer_id (str):
            Required. The ID of the customer for which to
            update the user data.
        operations (Sequence[google.ads.googleads.v12.services.types.UserDataOperation]):
            Required. The list of operations to be done.
        customer_match_user_list_metadata (google.ads.googleads.v12.common.types.CustomerMatchUserListMetadata):
            Metadata for data updates to a Customer Match
            user list.

            This field is a member of `oneof`_ ``metadata``.
    """

    customer_id = proto.Field(proto.STRING, number=1,)
    operations = proto.RepeatedField(
        proto.MESSAGE, number=3, message="UserDataOperation",
    )
    customer_match_user_list_metadata = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="metadata",
        message=offline_user_data.CustomerMatchUserListMetadata,
    )


class UserDataOperation(proto.Message):
    r"""Operation to be made for the UploadUserDataRequest.

    This message has `oneof`_ fields (mutually exclusive fields).
    For each oneof, at most one member field can be set at the same time.
    Setting any member of the oneof automatically clears all other
    members.

    .. _oneof: https://proto-plus-python.readthedocs.io/en/stable/fields.html#oneofs-mutually-exclusive-fields

    Attributes:
        create (google.ads.googleads.v12.common.types.UserData):
            The list of user data to be appended to the
            user list.

            This field is a member of `oneof`_ ``operation``.
        remove (google.ads.googleads.v12.common.types.UserData):
            The list of user data to be removed from the
            user list.

            This field is a member of `oneof`_ ``operation``.
    """

    create = proto.Field(
        proto.MESSAGE,
        number=1,
        oneof="operation",
        message=offline_user_data.UserData,
    )
    remove = proto.Field(
        proto.MESSAGE,
        number=2,
        oneof="operation",
        message=offline_user_data.UserData,
    )


class UploadUserDataResponse(proto.Message):
    r"""Response message for
    [UserDataService.UploadUserData][google.ads.googleads.v12.services.UserDataService.UploadUserData]
    Uploads made through this service will not be visible under the
    'Segment members' section for the Customer Match List in the Google
    Ads UI.

    Attributes:
        upload_date_time (str):
            The date time at which the request was received by API,
            formatted as "yyyy-mm-dd hh:mm:ss+|-hh:mm", for example,
            "2019-01-01 12:32:45-08:00".

            This field is a member of `oneof`_ ``_upload_date_time``.
        received_operations_count (int):
            Number of upload data operations received by
            API.

            This field is a member of `oneof`_ ``_received_operations_count``.
    """

    upload_date_time = proto.Field(proto.STRING, number=3, optional=True,)
    received_operations_count = proto.Field(
        proto.INT32, number=4, optional=True,
    )


__all__ = tuple(sorted(__protobuf__.manifest))
