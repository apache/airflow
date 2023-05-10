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
    manifest={"ManagerLinkErrorEnum",},
)


class ManagerLinkErrorEnum(proto.Message):
    r"""Container for enum describing possible ManagerLink errors.
    """

    class ManagerLinkError(proto.Enum):
        r"""Enum describing possible ManagerLink errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        ACCOUNTS_NOT_COMPATIBLE_FOR_LINKING = 2
        TOO_MANY_MANAGERS = 3
        TOO_MANY_INVITES = 4
        ALREADY_INVITED_BY_THIS_MANAGER = 5
        ALREADY_MANAGED_BY_THIS_MANAGER = 6
        ALREADY_MANAGED_IN_HIERARCHY = 7
        DUPLICATE_CHILD_FOUND = 8
        CLIENT_HAS_NO_ADMIN_USER = 9
        MAX_DEPTH_EXCEEDED = 10
        CYCLE_NOT_ALLOWED = 11
        TOO_MANY_ACCOUNTS = 12
        TOO_MANY_ACCOUNTS_AT_MANAGER = 13
        NON_OWNER_USER_CANNOT_MODIFY_LINK = 14
        SUSPENDED_ACCOUNT_CANNOT_ADD_CLIENTS = 15
        CLIENT_OUTSIDE_TREE = 16
        INVALID_STATUS_CHANGE = 17
        INVALID_CHANGE = 18
        CUSTOMER_CANNOT_MANAGE_SELF = 19
        CREATING_ENABLED_LINK_NOT_ALLOWED = 20


__all__ = tuple(sorted(__protobuf__.manifest))
