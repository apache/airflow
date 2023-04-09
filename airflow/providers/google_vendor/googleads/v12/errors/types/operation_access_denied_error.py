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
    package="airflow.providers.google_vendor.googleads.v12.errors",
    marshal="google.ads.googleads.v12",
    manifest={"OperationAccessDeniedErrorEnum",},
)


class OperationAccessDeniedErrorEnum(proto.Message):
    r"""Container for enum describing possible operation access
    denied errors.

    """

    class OperationAccessDeniedError(proto.Enum):
        r"""Enum describing possible operation access denied errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        ACTION_NOT_PERMITTED = 2
        CREATE_OPERATION_NOT_PERMITTED = 3
        REMOVE_OPERATION_NOT_PERMITTED = 4
        UPDATE_OPERATION_NOT_PERMITTED = 5
        MUTATE_ACTION_NOT_PERMITTED_FOR_CLIENT = 6
        OPERATION_NOT_PERMITTED_FOR_CAMPAIGN_TYPE = 7
        CREATE_AS_REMOVED_NOT_PERMITTED = 8
        OPERATION_NOT_PERMITTED_FOR_REMOVED_RESOURCE = 9
        OPERATION_NOT_PERMITTED_FOR_AD_GROUP_TYPE = 10
        MUTATE_NOT_PERMITTED_FOR_CUSTOMER = 11


__all__ = tuple(sorted(__protobuf__.manifest))
