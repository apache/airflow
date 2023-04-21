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
    manifest={"AssetSetErrorEnum",},
)


class AssetSetErrorEnum(proto.Message):
    r"""Container for enum describing possible asset set errors.
    """

    class AssetSetError(proto.Enum):
        r"""Enum describing possible asset set errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_ASSET_SET_NAME = 2
        INVALID_PARENT_ASSET_SET_TYPE = 3
        ASSET_SET_SOURCE_INCOMPATIBLE_WITH_PARENT_ASSET_SET = 4
        ASSET_SET_TYPE_CANNOT_BE_LINKED_TO_CUSTOMER = 5
        INVALID_CHAIN_IDS = 6
        LOCATION_SYNC_ASSET_SET_DOES_NOT_SUPPORT_RELATIONSHIP_TYPE = 7
        NOT_UNIQUE_ENABLED_LOCATION_SYNC_TYPED_ASSET_SET = 8
        INVALID_PLACE_IDS = 9


__all__ = tuple(sorted(__protobuf__.manifest))
