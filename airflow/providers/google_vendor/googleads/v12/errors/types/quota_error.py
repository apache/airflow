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
    manifest={"QuotaErrorEnum",},
)


class QuotaErrorEnum(proto.Message):
    r"""Container for enum describing possible quota errors.
    """

    class QuotaError(proto.Enum):
        r"""Enum describing possible quota errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        RESOURCE_EXHAUSTED = 2
        ACCESS_PROHIBITED = 3
        RESOURCE_TEMPORARILY_EXHAUSTED = 4


__all__ = tuple(sorted(__protobuf__.manifest))
