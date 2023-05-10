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
    manifest={"ConversionActionErrorEnum",},
)


class ConversionActionErrorEnum(proto.Message):
    r"""Container for enum describing possible conversion action
    errors.

    """

    class ConversionActionError(proto.Enum):
        r"""Enum describing possible conversion action errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_NAME = 2
        DUPLICATE_APP_ID = 3
        TWO_CONVERSION_ACTIONS_BIDDING_ON_SAME_APP_DOWNLOAD = 4
        BIDDING_ON_SAME_APP_DOWNLOAD_AS_GLOBAL_ACTION = 5
        DATA_DRIVEN_MODEL_WAS_NEVER_GENERATED = 6
        DATA_DRIVEN_MODEL_EXPIRED = 7
        DATA_DRIVEN_MODEL_STALE = 8
        DATA_DRIVEN_MODEL_UNKNOWN = 9
        CREATION_NOT_SUPPORTED = 10
        UPDATE_NOT_SUPPORTED = 11


__all__ = tuple(sorted(__protobuf__.manifest))
