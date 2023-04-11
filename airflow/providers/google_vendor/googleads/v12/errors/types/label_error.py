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
    manifest={"LabelErrorEnum",},
)


class LabelErrorEnum(proto.Message):
    r"""Container for enum describing possible label errors.
    """

    class LabelError(proto.Enum):
        r"""Enum describing possible label errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        CANNOT_APPLY_INACTIVE_LABEL = 2
        CANNOT_APPLY_LABEL_TO_DISABLED_AD_GROUP_CRITERION = 3
        CANNOT_APPLY_LABEL_TO_NEGATIVE_AD_GROUP_CRITERION = 4
        EXCEEDED_LABEL_LIMIT_PER_TYPE = 5
        INVALID_RESOURCE_FOR_MANAGER_LABEL = 6
        DUPLICATE_NAME = 7
        INVALID_LABEL_NAME = 8
        CANNOT_ATTACH_LABEL_TO_DRAFT = 9
        CANNOT_ATTACH_NON_MANAGER_LABEL_TO_CUSTOMER = 10


__all__ = tuple(sorted(__protobuf__.manifest))
