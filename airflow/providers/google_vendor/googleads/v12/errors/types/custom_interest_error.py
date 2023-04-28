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
    manifest={"CustomInterestErrorEnum",},
)


class CustomInterestErrorEnum(proto.Message):
    r"""Container for enum describing possible custom interest
    errors.

    """

    class CustomInterestError(proto.Enum):
        r"""Enum describing possible custom interest errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        NAME_ALREADY_USED = 2
        CUSTOM_INTEREST_MEMBER_ID_AND_TYPE_PARAMETER_NOT_PRESENT_IN_REMOVE = 3
        TYPE_AND_PARAMETER_NOT_FOUND = 4
        TYPE_AND_PARAMETER_ALREADY_EXISTED = 5
        INVALID_CUSTOM_INTEREST_MEMBER_TYPE = 6
        CANNOT_REMOVE_WHILE_IN_USE = 7
        CANNOT_CHANGE_TYPE = 8


__all__ = tuple(sorted(__protobuf__.manifest))
