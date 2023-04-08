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
    manifest={"AdGroupAdErrorEnum",},
)


class AdGroupAdErrorEnum(proto.Message):
    r"""Container for enum describing possible ad group ad errors.
    """

    class AdGroupAdError(proto.Enum):
        r"""Enum describing possible ad group ad errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        AD_GROUP_AD_LABEL_DOES_NOT_EXIST = 2
        AD_GROUP_AD_LABEL_ALREADY_EXISTS = 3
        AD_NOT_UNDER_ADGROUP = 4
        CANNOT_OPERATE_ON_REMOVED_ADGROUPAD = 5
        CANNOT_CREATE_DEPRECATED_ADS = 6
        CANNOT_CREATE_TEXT_ADS = 7
        EMPTY_FIELD = 8
        RESOURCE_REFERENCED_IN_MULTIPLE_OPS = 9
        AD_TYPE_CANNOT_BE_PAUSED = 10
        AD_TYPE_CANNOT_BE_REMOVED = 11
        CANNOT_UPDATE_DEPRECATED_ADS = 12


__all__ = tuple(sorted(__protobuf__.manifest))
