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
    manifest={"AssetGroupErrorEnum",},
)


class AssetGroupErrorEnum(proto.Message):
    r"""Container for enum describing possible asset group errors.
    """

    class AssetGroupError(proto.Enum):
        r"""Enum describing possible asset group errors."""
        UNSPECIFIED = 0
        UNKNOWN = 1
        DUPLICATE_NAME = 2
        CANNOT_ADD_ASSET_GROUP_FOR_CAMPAIGN_TYPE = 3
        NOT_ENOUGH_HEADLINE_ASSET = 4
        NOT_ENOUGH_LONG_HEADLINE_ASSET = 5
        NOT_ENOUGH_DESCRIPTION_ASSET = 6
        NOT_ENOUGH_BUSINESS_NAME_ASSET = 7
        NOT_ENOUGH_MARKETING_IMAGE_ASSET = 8
        NOT_ENOUGH_SQUARE_MARKETING_IMAGE_ASSET = 9
        NOT_ENOUGH_LOGO_ASSET = 10
        FINAL_URL_SHOPPING_MERCHANT_HOME_PAGE_URL_DOMAINS_DIFFER = 11


__all__ = tuple(sorted(__protobuf__.manifest))
