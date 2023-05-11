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
    manifest={"UserListErrorEnum",},
)


class UserListErrorEnum(proto.Message):
    r"""Container for enum describing possible user list errors.
    """

    class UserListError(proto.Enum):
        r"""Enum describing possible user list errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        EXTERNAL_REMARKETING_USER_LIST_MUTATE_NOT_SUPPORTED = 2
        CONCRETE_TYPE_REQUIRED = 3
        CONVERSION_TYPE_ID_REQUIRED = 4
        DUPLICATE_CONVERSION_TYPES = 5
        INVALID_CONVERSION_TYPE = 6
        INVALID_DESCRIPTION = 7
        INVALID_NAME = 8
        INVALID_TYPE = 9
        CAN_NOT_ADD_LOGICAL_LIST_AS_LOGICAL_LIST_OPERAND = 10
        INVALID_USER_LIST_LOGICAL_RULE_OPERAND = 11
        NAME_ALREADY_USED = 12
        NEW_CONVERSION_TYPE_NAME_REQUIRED = 13
        CONVERSION_TYPE_NAME_ALREADY_USED = 14
        OWNERSHIP_REQUIRED_FOR_SET = 15
        USER_LIST_MUTATE_NOT_SUPPORTED = 16
        INVALID_RULE = 17
        INVALID_DATE_RANGE = 27
        CAN_NOT_MUTATE_SENSITIVE_USERLIST = 28
        MAX_NUM_RULEBASED_USERLISTS = 29
        CANNOT_MODIFY_BILLABLE_RECORD_COUNT = 30
        APP_ID_NOT_SET = 31
        USERLIST_NAME_IS_RESERVED_FOR_SYSTEM_LIST = 32
        ADVERTISER_NOT_ON_ALLOWLIST_FOR_USING_UPLOADED_DATA = 37
        RULE_TYPE_IS_NOT_SUPPORTED = 34
        CAN_NOT_ADD_A_SIMILAR_USERLIST_AS_LOGICAL_LIST_OPERAND = 35
        CAN_NOT_MIX_CRM_BASED_IN_LOGICAL_LIST_WITH_OTHER_LISTS = 36


__all__ = tuple(sorted(__protobuf__.manifest))
