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

from airflow.providers.google_vendor.googleads.v12.enums.types import shared_set_status
from airflow.providers.google_vendor.googleads.v12.enums.types import shared_set_type


__protobuf__ = proto.module(
    package="airflow.providers.google_vendor.googleads.v12.resources",
    marshal="google.ads.googleads.v12",
    manifest={"SharedSet",},
)


class SharedSet(proto.Message):
    r"""SharedSets are used for sharing criterion exclusions across
    multiple campaigns.

    Attributes:
        resource_name (str):
            Immutable. The resource name of the shared set. Shared set
            resource names have the form:

            ``customers/{customer_id}/sharedSets/{shared_set_id}``
        id (int):
            Output only. The ID of this shared set. Read
            only.

            This field is a member of `oneof`_ ``_id``.
        type_ (google.ads.googleads.v12.enums.types.SharedSetTypeEnum.SharedSetType):
            Immutable. The type of this shared set: each
            shared set holds only a single kind of resource.
            Required. Immutable.
        name (str):
            The name of this shared set. Required.
            Shared Sets must have names that are unique
            among active shared sets of the same type.
            The length of this string should be between 1
            and 255 UTF-8 bytes, inclusive.

            This field is a member of `oneof`_ ``_name``.
        status (google.ads.googleads.v12.enums.types.SharedSetStatusEnum.SharedSetStatus):
            Output only. The status of this shared set.
            Read only.
        member_count (int):
            Output only. The number of shared criteria
            within this shared set. Read only.

            This field is a member of `oneof`_ ``_member_count``.
        reference_count (int):
            Output only. The number of campaigns
            associated with this shared set. Read only.

            This field is a member of `oneof`_ ``_reference_count``.
    """

    resource_name = proto.Field(proto.STRING, number=1,)
    id = proto.Field(proto.INT64, number=8, optional=True,)
    type_ = proto.Field(
        proto.ENUM,
        number=3,
        enum=shared_set_type.SharedSetTypeEnum.SharedSetType,
    )
    name = proto.Field(proto.STRING, number=9, optional=True,)
    status = proto.Field(
        proto.ENUM,
        number=5,
        enum=shared_set_status.SharedSetStatusEnum.SharedSetStatus,
    )
    member_count = proto.Field(proto.INT64, number=10, optional=True,)
    reference_count = proto.Field(proto.INT64, number=11, optional=True,)


__all__ = tuple(sorted(__protobuf__.manifest))
