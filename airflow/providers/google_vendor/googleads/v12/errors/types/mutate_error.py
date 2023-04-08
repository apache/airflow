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
    manifest={"MutateErrorEnum",},
)


class MutateErrorEnum(proto.Message):
    r"""Container for enum describing possible mutate errors.
    """

    class MutateError(proto.Enum):
        r"""Enum describing possible mutate errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        RESOURCE_NOT_FOUND = 3
        ID_EXISTS_IN_MULTIPLE_MUTATES = 7
        INCONSISTENT_FIELD_VALUES = 8
        MUTATE_NOT_ALLOWED = 9
        RESOURCE_NOT_IN_GOOGLE_ADS = 10
        RESOURCE_ALREADY_EXISTS = 11
        RESOURCE_DOES_NOT_SUPPORT_VALIDATE_ONLY = 12
        OPERATION_DOES_NOT_SUPPORT_PARTIAL_FAILURE = 16
        RESOURCE_READ_ONLY = 13


__all__ = tuple(sorted(__protobuf__.manifest))
