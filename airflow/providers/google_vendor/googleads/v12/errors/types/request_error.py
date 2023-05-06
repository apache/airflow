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
    manifest={"RequestErrorEnum",},
)


class RequestErrorEnum(proto.Message):
    r"""Container for enum describing possible request errors.
    """

    class RequestError(proto.Enum):
        r"""Enum describing possible request errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        RESOURCE_NAME_MISSING = 3
        RESOURCE_NAME_MALFORMED = 4
        BAD_RESOURCE_ID = 17
        INVALID_CUSTOMER_ID = 16
        OPERATION_REQUIRED = 5
        RESOURCE_NOT_FOUND = 6
        INVALID_PAGE_TOKEN = 7
        EXPIRED_PAGE_TOKEN = 8
        INVALID_PAGE_SIZE = 22
        REQUIRED_FIELD_MISSING = 9
        IMMUTABLE_FIELD = 11
        TOO_MANY_MUTATE_OPERATIONS = 13
        CANNOT_BE_EXECUTED_BY_MANAGER_ACCOUNT = 14
        CANNOT_MODIFY_FOREIGN_FIELD = 15
        INVALID_ENUM_VALUE = 18
        DEVELOPER_TOKEN_PARAMETER_MISSING = 19
        LOGIN_CUSTOMER_ID_PARAMETER_MISSING = 20
        VALIDATE_ONLY_REQUEST_HAS_PAGE_TOKEN = 21
        CANNOT_RETURN_SUMMARY_ROW_FOR_REQUEST_WITHOUT_METRICS = 29
        CANNOT_RETURN_SUMMARY_ROW_FOR_VALIDATE_ONLY_REQUESTS = 30
        INCONSISTENT_RETURN_SUMMARY_ROW_VALUE = 31
        TOTAL_RESULTS_COUNT_NOT_ORIGINALLY_REQUESTED = 32
        RPC_DEADLINE_TOO_SHORT = 33


__all__ = tuple(sorted(__protobuf__.manifest))
