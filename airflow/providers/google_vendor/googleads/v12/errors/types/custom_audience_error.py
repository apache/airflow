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
    manifest={"CustomAudienceErrorEnum",},
)


class CustomAudienceErrorEnum(proto.Message):
    r"""Container for enum describing possible custom audience
    errors.

    """

    class CustomAudienceError(proto.Enum):
        r"""Enum describing possible custom audience errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        NAME_ALREADY_USED = 2
        CANNOT_REMOVE_WHILE_IN_USE = 3
        RESOURCE_ALREADY_REMOVED = 4
        MEMBER_TYPE_AND_PARAMETER_ALREADY_EXISTED = 5
        INVALID_MEMBER_TYPE = 6
        MEMBER_TYPE_AND_VALUE_DOES_NOT_MATCH = 7
        POLICY_VIOLATION = 8
        INVALID_TYPE_CHANGE = 9


__all__ = tuple(sorted(__protobuf__.manifest))
