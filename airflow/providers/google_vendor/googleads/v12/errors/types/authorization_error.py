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
    manifest={"AuthorizationErrorEnum",},
)


class AuthorizationErrorEnum(proto.Message):
    r"""Container for enum describing possible authorization errors.
    """

    class AuthorizationError(proto.Enum):
        r"""Enum describing possible authorization errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        USER_PERMISSION_DENIED = 2
        DEVELOPER_TOKEN_NOT_ON_ALLOWLIST = 13
        DEVELOPER_TOKEN_PROHIBITED = 4
        PROJECT_DISABLED = 5
        AUTHORIZATION_ERROR = 6
        ACTION_NOT_PERMITTED = 7
        INCOMPLETE_SIGNUP = 8
        CUSTOMER_NOT_ENABLED = 24
        MISSING_TOS = 9
        DEVELOPER_TOKEN_NOT_APPROVED = 10
        INVALID_LOGIN_CUSTOMER_ID_SERVING_CUSTOMER_ID_COMBINATION = 11
        SERVICE_ACCESS_DENIED = 12
        ACCESS_DENIED_FOR_ACCOUNT_TYPE = 25
        METRIC_ACCESS_DENIED = 26


__all__ = tuple(sorted(__protobuf__.manifest))
